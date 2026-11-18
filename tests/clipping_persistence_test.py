"""Testes da persistência do clipping (fase F1).

Cobrem a lógica pura de normalização e o cálculo de status. A escrita real no
PostgreSQL é exercitada em `clipping_persistence_pg_test.py`, que roda quando
`CLIPPING_TEST_DSN` está definido.
"""

from datetime import date

from dags.ro_dou_src.clipping_persistence import (
    RUN_FAILED,
    RUN_PARTIAL,
    RUN_SUCCESS,
    aggregate_sources_status,
    build_item_rows,
    external_id_for,
    item_id,
    overall_status,
    parse_published_date,
)

REFERENCE_DATE = date(2026, 11, 16)


def _item(**overrides):
    item = {
        "source": "DOU",
        "section": "Seção 3",
        "title": "Extrato de contrato",
        "href": "https://www.in.gov.br/web/dou/-/extrato-1",
        "abstract": "Contrato de licitação",
        "date": "15/11/2026",
    }
    item.update(overrides)
    return item


def _search_result(items=None, statuses=None, term="licitação", department="single_department"):
    items = items if items is not None else [_item()]
    return {
        "result": {
            "single_group": {
                term: {
                    department: items,
                }
            }
        },
        "sources_status": statuses
        or {"DOU": {"status": "ok", "error": None, "items_count": len(items)}},
    }


def test_item_id_is_deterministic_and_source_sensitive():
    first = item_id("DOU", "SP", "https://example.test/1")
    assert first == item_id("DOU", "SP", "https://example.test/1")
    assert first != item_id("DOE:SP", "SP", "https://example.test/1")
    assert first != item_id("DOU", None, "https://example.test/1")


def test_parse_published_date_accepts_brazilian_and_iso():
    assert parse_published_date("15/11/2026", REFERENCE_DATE) == date(2026, 11, 15)
    assert parse_published_date("2026-11-14", REFERENCE_DATE) == date(2026, 11, 14)


def test_parse_published_date_falls_back_to_reference_date():
    assert parse_published_date(None, REFERENCE_DATE) == REFERENCE_DATE
    assert parse_published_date("data inválida", REFERENCE_DATE) == REFERENCE_DATE


def test_external_id_prefers_href_then_url():
    assert external_id_for({"href": "https://a.test/1"}) == "https://a.test/1"
    assert external_id_for({"url": "https://a.test/2"}) == "https://a.test/2"
    fallback = external_id_for(
        {"source": "DOU", "section": "Seção 1", "title": "T", "date": "15/11/2026"}
    )
    assert fallback == "DOU|Seção 1|T|15/11/2026"


def test_build_item_rows_extracts_fields():
    rows = build_item_rows([_search_result()], REFERENCE_DATE)

    assert len(rows) == 1
    row = rows[0]
    assert row["source"] == "DOU"
    assert row["id"] == item_id("DOU", None, _item()["href"])
    assert row["external_id"] == _item()["href"]
    assert row["published_date"] == date(2026, 11, 15)
    assert row["section"] == "Seção 3"
    assert row["org"] is None
    assert row["title"] == "Extrato de contrato"
    assert row["abstract"] == "Contrato de licitação"
    assert row["url"] == _item()["href"]
    assert row["matched_terms"] == ["licitação"]
    assert row["content_hash"]


def test_build_item_rows_uses_department_as_org():
    rows = build_item_rows(
        [_search_result(department="Ministério da Gestão")], REFERENCE_DATE
    )

    assert rows[0]["org"] == "Ministério da Gestão"


def test_build_item_rows_dedupes_same_item():
    duplicated = [_item(), _item()]
    rows = build_item_rows([_search_result(items=duplicated)], REFERENCE_DATE)

    assert len(rows) == 1


def test_build_item_rows_keeps_same_content_from_different_sources():
    dou_item = _item(source="DOU")
    sp_item = _item(source="DOE:SP")

    rows = build_item_rows(
        [_search_result(items=[dou_item, sp_item])], REFERENCE_DATE
    )

    assert len(rows) == 2
    by_source = {row["source"]: row for row in rows}
    assert set(by_source) == {"DOU", "DOE:SP"}
    assert by_source["DOU"]["uf"] is None
    assert by_source["DOE:SP"]["uf"] == "SP"


def test_build_item_rows_fills_uf_for_state_sources():
    rows = build_item_rows(
        [_search_result(items=[_item(source="DOE:MG")])], REFERENCE_DATE
    )
    assert rows[0]["uf"] == "MG"


def test_build_item_rows_skips_missing_search():
    assert build_item_rows([None, _search_result()], REFERENCE_DATE)


def test_aggregate_sources_status_merges_ok_and_failed():
    results = [
        _search_result(
            statuses={
                "DOU": {"status": "ok", "error": None, "items_count": 2},
                "QD": {"status": "failed", "error": "HTTP 503", "items_count": 0},
            }
        ),
        _search_result(
            statuses={
                "DOU": {"status": "empty", "error": None, "items_count": 0},
            }
        ),
    ]

    statuses, requested = aggregate_sources_status(results, ["DOU", "QD"])

    assert requested == ["DOU", "QD"]
    assert statuses["DOU"]["status"] == "ok"
    assert statuses["DOU"]["items_count"] == 2
    assert statuses["QD"]["status"] == "failed"
    assert statuses["QD"]["error"] == "HTTP 503"


def test_aggregate_marks_configured_source_without_status_as_failed():
    statuses, requested = aggregate_sources_status([None], ["DOU", "DOE:SP"])

    assert requested == ["DOU", "DOE:SP"]
    assert {status["status"] for status in statuses.values()} == {"failed"}
    assert all("não concluiu" in status["error"] for status in statuses.values())


def test_overall_status():
    ok = {"DOU": {"status": "ok"}}
    empty = {"DOU": {"status": "empty"}}
    failed = {"DOU": {"status": "failed", "error": "x"}}

    assert overall_status(ok) == RUN_SUCCESS
    assert overall_status(empty) == RUN_SUCCESS
    assert (
        overall_status(
            {
                "DOU": {"status": "ok"},
                "QD": {"status": "failed", "error": "x"},
            }
        )
        == RUN_PARTIAL
    )
    assert overall_status(failed) == RUN_FAILED
    assert overall_status({}) == RUN_FAILED
