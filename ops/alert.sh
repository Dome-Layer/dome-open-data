#!/usr/bin/env bash
# One open issue per subject, never a pile.
#
# A pipeline that nobody watches has to say when it breaks, but a scheduled job that opens a
# fresh issue on every run teaches its owner to ignore the label within a fortnight. So each
# subject (a source that fails, stale sources, the storage guard) keeps exactly one open issue:
# the first failure opens it, later failures comment on it, and a recovery closes it.
#
#   ops/alert.sh open    "<title>" <body-file>
#   ops/alert.sh resolve "<title>" "<comment>"
#
# Needs GH_TOKEN and `issues: write`.
set -euo pipefail

LABEL=open-data
action=${1:?open or resolve}
title=${2:?issue title}

existing=$(gh issue list --state open --label "$LABEL" --limit 100 --json number,title \
  --jq "[.[] | select(.title == \"${title//\"/\\\"}\")] | .[0].number // empty" 2>/dev/null || true)

case "$action" in
  open)
    body_file=${3:?body file}
    if [ -n "$existing" ]; then
      gh issue comment "$existing" --body-file "$body_file"
      echo "commented on #$existing"
    else
      gh label create "$LABEL" --color B60205 --description 'Open data pipeline alert' \
        2>/dev/null || true
      gh issue create --title "$title" --label "$LABEL" --body-file "$body_file"
    fi
    ;;
  resolve)
    if [ -n "$existing" ]; then
      gh issue comment "$existing" --body "${3:-Recovered.}"
      gh issue close "$existing"
      echo "closed #$existing"
    else
      echo "nothing open for: $title"
    fi
    ;;
  *)
    echo "usage: ops/alert.sh open|resolve <title> [body-file|comment]" >&2
    exit 64
    ;;
esac
