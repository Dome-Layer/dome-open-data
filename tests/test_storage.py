import pytest

from dome_open_data.storage import R2Credentials

GOOD = {
    "R2_ACCOUNT_ID": "a" * 32,
    "R2_ACCESS_KEY_ID": "b" * 32,
    "R2_SECRET_ACCESS_KEY": "c" * 64,
    "R2_BUCKET": "dome-open-data",
}


def set_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for name, value in {**GOOD, **overrides}.items():
        monkeypatch.setenv(name, value)


def test_valid_credentials_build_the_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    set_env(monkeypatch)
    credentials = R2Credentials.from_env()
    assert credentials.endpoint == f"https://{'a' * 32}.r2.cloudflarestorage.com"
    assert "a" * 32 not in repr(credentials)


def test_a_token_in_the_account_id_is_refused_without_echoing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = "cfat_" + "x" * 40
    set_env(monkeypatch, R2_ACCOUNT_ID=token)
    with pytest.raises(RuntimeError, match="R2_ACCOUNT_ID should be 32 hex characters") as info:
        R2Credentials.from_env()
    assert token not in str(info.value)


def test_missing_variables_are_named(monkeypatch: pytest.MonkeyPatch) -> None:
    set_env(monkeypatch, R2_SECRET_ACCESS_KEY="")
    with pytest.raises(RuntimeError, match="missing environment variables: R2_SECRET_ACCESS_KEY"):
        R2Credentials.from_env()
