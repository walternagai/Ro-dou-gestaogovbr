"""Testes do hook e do searcher do DOE-MG (fase F2)."""

from datetime import date
from unittest import mock

import pytest
import requests
from dags.ro_dou_src.hooks.doemg_hook import DOEMGHook
from dags.ro_dou_src.searchers import MGSearcher

REFERENCE_DATE = date(2026, 11, 14)


def _response(payload, status=200):
    response = mock.Mock()
    response.status_code = status
    response.json.return_value = payload
    response.raise_for_status = mock.Mock()
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"HTTP {status}", response=response
        )
    return response


def _raw(page="5", id_jornal="331334", text="trecho com licitacao"):
    return {
        "idJornal": id_jornal,
        "dataPublicacao": "2026-11-14T00:00:00",
        "tipoCaderno": "Diário do Executivo",
        "textoResultado": text,
        "pagina": page,
    }


def _search_payload(items, page=1, total=1):
    return {
        "dados": items,
        "paginaAtual": page,
        "totalDePaginas": total,
        "totalDeRegistros": len(items),
        "erros": [],
    }


@pytest.fixture()
def hook(mocker):
    mocker.patch("dags.ro_dou_src.hooks.doemg_hook.time.sleep")
    mocker.patch.object(DOEMGHook, "_throttle", return_value=None)
    return DOEMGHook()


def test_search_authenticates_and_paginates(hook, mocker):
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if url.endswith("Autenticacao/Autenticar"):
            return _response({"dados": "token-1", "erros": []})
        page = kwargs["params"]["paginaAtual"]
        return _response(_search_payload([_raw(page=str(page))], page, total=2))

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    results = hook.search(["licitacao"], REFERENCE_DATE)

    assert len(results["licitacao"]) == 2

    auth_calls = [c for c in calls if c[1].endswith("Autenticacao/Autenticar")]
    search_calls = [c for c in calls if "PesquisarJornaisPaginados" in c[1]]
    assert len(auth_calls) == 1
    assert len(search_calls) == 2
    assert search_calls[0][2]["headers"]["Authorization"] == "Bearer token-1"
    assert "Authorization" not in auth_calls[0][2]["headers"]
    assert [c[2]["params"]["paginaAtual"] for c in search_calls] == [1, 2]
    assert search_calls[0][2]["params"]["DiarioExecutivo"] == "true"
    assert search_calls[0][2]["params"]["DiarioMunicipios"] == "true"
    assert search_calls[0][2]["params"]["DiarioTerceiros"] == "false"
    assert search_calls[0][2]["verify"].endswith(".pem")


def test_search_returns_empty_when_there_are_no_results(hook, mocker):
    def fake_request(method, url, **kwargs):
        if url.endswith("Autenticacao/Autenticar"):
            return _response({"dados": "token", "erros": []})
        return _response(_search_payload([], total=0))

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    assert hook.search(["x"], REFERENCE_DATE) == {"x": []}


def test_search_retries_on_429(hook, mocker):
    state = {"search_calls": 0}

    def fake_request(method, url, **kwargs):
        if url.endswith("Autenticacao/Autenticar"):
            return _response({"dados": "token", "erros": []})
        state["search_calls"] += 1
        if state["search_calls"] == 1:
            return _response({"dados": [], "erros": []}, status=429)
        return _response(_search_payload([_raw()]))

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    results = hook.search(["x"], REFERENCE_DATE)

    assert state["search_calls"] == 2
    assert len(results["x"]) == 1


def test_search_raises_after_exhausting_retries(hook, mocker):
    def fake_request(method, url, **kwargs):
        if url.endswith("Autenticacao/Autenticar"):
            return _response({"dados": "token", "erros": []})
        return _response({"dados": [], "erros": []}, status=500)

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    with pytest.raises(requests.HTTPError):
        hook.search(["x"], REFERENCE_DATE)


def test_search_reauthenticates_on_401(hook, mocker):
    tokens = iter(["token-1", "token-2"])
    state = {"auth_calls": 0, "search_calls": 0}

    def fake_request(method, url, **kwargs):
        if url.endswith("Autenticacao/Autenticar"):
            state["auth_calls"] += 1
            return _response({"dados": next(tokens), "erros": []})
        state["search_calls"] += 1
        if state["search_calls"] == 1:
            return _response({"dados": [], "erros": []}, status=401)
        return _response(_search_payload([_raw()]))

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    results = hook.search(["x"], REFERENCE_DATE)

    assert state["auth_calls"] == 2
    assert len(results["x"]) == 1


def test_search_is_cached(hook, mocker):
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        if url.endswith("Autenticacao/Autenticar"):
            return _response({"dados": "token", "erros": []})
        return _response(_search_payload([_raw()]))

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    hook.search(["x"], REFERENCE_DATE, cadernos=["Executivo"])
    hook.search(["x"], REFERENCE_DATE, cadernos=["Executivo"])

    assert len(calls) == 2  # autenticação + uma busca


def test_ca_bundle_combines_certifi_and_intermediate():
    bundle = DOEMGHook._ca_bundle_path()
    with open(bundle, encoding="utf-8") as bundle_file:
        content = bundle_file.read()

    assert bundle.endswith(".pem")
    assert content.count("BEGIN CERTIFICATE") >= 2


def test_caderno_flags_default_and_all():
    assert DOEMGHook._caderno_flags(None) == {
        "DiarioExecutivo": "true",
        "DiarioMunicipios": "true",
        "DiarioTerceiros": "false",
        "EdicaoExtra": "false",
    }
    assert set(DOEMGHook._caderno_flags(["Todos"]).values()) == {"true"}


def test_normalize_builds_ro_dou_item(hook):
    item = hook.normalize(_raw(), "licitacao")

    assert item["title"] == "Diário do Executivo - página 5"
    assert item["section"] == "DOE-MG - Diário do Executivo"
    assert item["abstract"] == "trecho com licitacao"
    assert item["date"] == "14/11/2026"
    assert "edicao-do-dia?dados=" in item["href"]
    assert item["hierarchyList"] == []


def test_mg_searcher_groups_and_filters_terms_ignore(mocker):
    mocker.patch("dags.ro_dou_src.searchers.time.sleep")
    searcher = MGSearcher()
    searcher.adapter = mocker.Mock()
    searcher.adapter.search.return_value = {
        "licitacao": [
            {"title": "Executivo - página 1", "abstract": "licitacao", "hierarchyList": []},
            {"title": "Executivo - página 2", "abstract": "conteúdo a ignorar", "hierarchyList": []},
        ]
    }

    result = searcher.exec_search(
        term_list=["licitacao"],
        journals=["Executivo"],
        search_date="DIA",
        department=None,
        department_ignore=None,
        terms_ignore=["ignorar"],
        pubtype=None,
        reference_date=REFERENCE_DATE,
    )

    items = result["single_group"]["licitacao"]["single_department"]
    assert len(items) == 1
    assert items[0]["title"] == "Executivo - página 1"


def test_mg_searcher_ignores_department_with_warning(mocker, caplog):
    mocker.patch("dags.ro_dou_src.searchers.time.sleep")
    searcher = MGSearcher()
    searcher.adapter = mocker.Mock()
    searcher.adapter.search.return_value = {
        "x": [{"title": "t", "abstract": "a", "hierarchyList": []}]
    }

    with caplog.at_level("WARNING"):
        result = searcher.exec_search(
            term_list=["x"],
            journals=None,
            search_date="DIA",
            department=["Secretaria de Saúde"],
            department_ignore=None,
            terms_ignore=None,
            pubtype=None,
            reference_date=REFERENCE_DATE,
        )

    items = result["single_group"]["x"]["single_department"]
    assert len(items) == 1
    assert "department" in caplog.text


def test_fetch_edition_requests_publication_date(hook, mocker):
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if url.endswith("Autenticacao/Autenticar"):
            return _response({"dados": "token", "erros": []})
        return _response(
            {
                "dados": {
                    "dataPublicacao": "2026-11-14T00:00:00",
                    "cadernos": [{"id": 1, "descricao": "Diário do Executivo"}],
                },
                "erros": [],
            }
        )

    mocker.patch(
        "dags.ro_dou_src.hooks.doemg_hook.requests.request",
        side_effect=fake_request,
    )

    edition = hook.fetch_edition(REFERENCE_DATE)

    assert edition["cadernos"][0]["descricao"] == "Diário do Executivo"
    method, url, kwargs = calls[-1]
    assert method == "GET"
    assert url.endswith("Jornal/ObterEdicaoPorDataPublicacao")
    assert kwargs["params"] == {"dataPublicacao": "2026-11-14"}
