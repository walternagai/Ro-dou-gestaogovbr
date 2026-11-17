"""Testes unitários do serviço: hashing, limite, sanitização e catálogo."""

import pytest
from app.auth import RateLimiter, hash_key
from app.catalog import SOURCES, SUPPORTED_UFS, validate_sources
from app.sanitize import sanitize_item, sanitize_text


def test_hash_key_is_deterministic():
    assert hash_key("abc") == hash_key("abc")
    assert hash_key("abc") != hash_key("abd")
    assert len(hash_key("abc")) == 64


def test_rate_limiter_window():
    limiter = RateLimiter()

    allowed, remaining, retry = limiter.allow("k", "read", limit=2, window=60)
    assert allowed is True
    assert remaining == 1
    assert retry is None

    allowed, remaining, retry = limiter.allow("k", "read", limit=2, window=60)
    assert allowed is True
    assert remaining == 0

    allowed, remaining, retry = limiter.allow("k", "read", limit=2, window=60)
    assert allowed is False
    assert remaining == 0
    assert retry and retry > 0


def test_validate_sources():
    assert validate_sources(["DOU", "DOE:MG"]) == ["DOU", "DOE:MG"]
    assert SUPPORTED_UFS == ("MG", "SP")

    with pytest.raises(ValueError) as excinfo:
        validate_sources(["XPTO"])

    assert "XPTO" in str(excinfo.value)
    assert "DOE:MG" in str(excinfo.value)


def test_sanitize_text_and_item():
    raw = "<p>Contrato\x00 <b>nº 10</b></p>\n<script>alert(1)</script>"
    cleaned = sanitize_text(raw, max_length=50)
    assert cleaned == "Contrato nº 10"
    assert "alert" not in cleaned

    truncated = sanitize_text("Contrato administrativo nº 10/2026", max_length=15)
    assert len(truncated) <= 15
    assert truncated.endswith("…")

    item = sanitize_item({"title": "<i>Título</i>", "abstract": "Texto\x07 limpo"})
    assert item["title"] == "Título"
    assert item["abstract"] == "Texto limpo"


def test_catalog_matches_state_adapters():
    try:
        from dags.ro_dou_src.state_adapters import STATE_ADAPTERS
    except ImportError:  # pragma: no cover - ambiente sem o pacote do Ro-DOU
        pytest.skip("state_adapters indisponível neste ambiente")

    assert set(STATE_ADAPTERS) == {
        source for source in SOURCES if source.startswith("DOE:")
    }
