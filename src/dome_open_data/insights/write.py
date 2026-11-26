"""Stage 2, write: Claude words the shortlist, and does nothing else.

The model sees only the shortlist JSON (`Candidate.for_model`): what each detector found, in a
sentence the code wrote, and each figure's meaning and display string. It never reads the store,
never sees a raw series and is told not to do arithmetic; whatever it writes goes to the checker,
which drops an insight for a single number it cannot prove.

One request per collection, for the whole shortlist, and none at all when every candidate's text
can be kept from the previous dashboard (same finding, same figures). The model id and effort come
from `insights.yaml`.
"""

import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from dome_open_data.insights.check import Draft
from dome_open_data.insights.config import Writer
from dome_open_data.insights.scan import Candidate

log = logging.getLogger(__name__)

FALLBACK_BETA = "server-side-fallback-2026-07-01"

SYSTEM = """\
You write short insight cards for DOME Open Data, a public dashboard of official statistics \
(ECB, World Bank, US federal agencies via ALFRED). Each card is read by a general business \
reader in a few seconds.

You receive a JSON shortlist. Each candidate has an id, a tag, a subject, a finding written by \
code, and a list of figures, each with a meaning and a display string. For every candidate, \
write one card.

Rules, all of them checked by code after you; a card that breaks one is thrown away:
- Use only the figures given for that candidate. Every number, date, month, quarter and year \
you write must be one of its figures, written as its display string or rounded from it. Never \
compute a new number: no differences, ratios, averages or percentages of your own.
- Write numbers as digits, except you may write a count of weeks or months in words.
- State what the data shows and nothing about why. Never claim a cause: no "because", "due \
to", "drove", "led to", "triggered", "in response to" and the like. For a Pattern, say that one \
thing followed another.
- Plain British English. No em dashes or en dashes; use commas, full stops or colons.
- Headline: one sentence, at most 100 characters, the finding a reader should remember.
- Body: two or three sentences, at most 450 characters, with the figures that support it.
- A Pattern candidate carries a caveat that the dashboard prints under the card; do not repeat \
it in the body.
- Echo the candidate's id and tag exactly, and list the ids of the figures you used.
- No advice, no forecasts, no adjectives the figures do not support ("dramatic", "historic").
"""

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "insights": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "tag": {"type": "string"},
                    "headline": {"type": "string"},
                    "body": {"type": "string"},
                    "figures_used": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["candidate_id", "tag", "headline", "body", "figures_used"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["insights"],
    "additionalProperties": False,
}


class WriterError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Written:
    drafts: list[Draft]
    model: str  # the model that actually served the request (a fallback may have)
    input_tokens: int
    output_tokens: int


# A request in, (text of the JSON answer, served model, input tokens, output tokens) out.
Call = Callable[[dict[str, Any]], Awaitable[tuple[str, str, int, int]]]


def request(candidates: list[Candidate], writer: Writer) -> dict[str, Any]:
    shortlist = [c.for_model() for c in candidates]
    body: dict[str, Any] = {
        "model": writer.model,
        "max_tokens": writer.max_tokens,
        "system": SYSTEM,
        "messages": [
            {
                "role": "user",
                "content": "Shortlist:\n" + json.dumps(shortlist, ensure_ascii=False, indent=1),
            }
        ],
        "output_config": {
            "effort": writer.effort,
            "format": {"type": "json_schema", "schema": SCHEMA},
        },
    }
    if writer.fallbacks == "default":
        body["betas"] = [FALLBACK_BETA]
        body["fallbacks"] = "default"
    return body


async def anthropic_call(body: dict[str, Any]) -> tuple[str, str, int, int]:
    """The real request, through the Anthropic SDK. Needs `ANTHROPIC_API_KEY`."""
    import anthropic

    if not os.environ.get("ANTHROPIC_API_KEY", "").strip():
        raise WriterError("ANTHROPIC_API_KEY is not set")
    client = anthropic.AsyncAnthropic()
    try:
        response = await client.beta.messages.create(**body)
    except anthropic.APIStatusError as exc:
        raise WriterError(f"the writer request failed: HTTP {exc.status_code}") from exc
    except anthropic.APIConnectionError as exc:
        raise WriterError("the writer request could not connect") from exc
    finally:
        await client.close()
    if response.stop_reason == "refusal":
        raise WriterError("the writer declined the request")
    if response.stop_reason == "max_tokens":
        raise WriterError("the writer ran out of tokens before finishing")
    text = next((b.text for b in response.content if b.type == "text"), None)
    if text is None:
        raise WriterError("the writer returned no text")
    return text, response.model, response.usage.input_tokens, response.usage.output_tokens


def parse(text: str) -> list[Draft]:
    try:
        data = json.loads(text)
        return [
            Draft(
                candidate_id=str(item["candidate_id"]),
                tag=str(item["tag"]),
                headline=str(item["headline"]).strip(),
                body=str(item["body"]).strip(),
                figures_used=[str(f) for f in item["figures_used"]],
            )
            for item in data["insights"]
        ]
    except (ValueError, KeyError, TypeError) as exc:
        raise WriterError(f"the writer's answer is not the agreed shape: {exc}") from exc


async def write(candidates: list[Candidate], writer: Writer, call: Call | None = None) -> Written:
    if not candidates:
        return Written([], writer.model, 0, 0)
    text, served, tokens_in, tokens_out = await (call or anthropic_call)(
        request(candidates, writer)
    )
    drafts = parse(text)
    log.info(
        "insights written",
        extra={
            "model": served,
            "candidates": len(candidates),
            "drafts": len(drafts),
            "input_tokens": tokens_in,
            "output_tokens": tokens_out,
        },
    )
    return Written(drafts, served, tokens_in, tokens_out)
