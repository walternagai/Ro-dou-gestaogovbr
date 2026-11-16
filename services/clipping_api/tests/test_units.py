"""Testes unitários do serviço: hashing, limite e catálogo."""

import pytest
from app.auth import RateLimiter, hash_key
from app.catalog import SOURCES, validate_sources


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

    with pytest.raises(ValueError) as excinfo:
        validate_sources(["XPTO"])

    assert "XPTO" in str(excinfo.value)
    assert "DOE:MG" in str(excinfo.value)


def test_catalog_matches_state_adapters():
    try:
        from dags.ro_dou_src.state_adapters import STATE_ADAPTERS
    except ImportError:  # pragma: no cover - ambiente sem o pacote do Ro-DOU
        pytest.skip("state_adapters indisponível neste ambiente")

    assert set(STATE_ADAPTERS) == {
        source for source in SOURCES if source.startswith("DOE:")
    }
