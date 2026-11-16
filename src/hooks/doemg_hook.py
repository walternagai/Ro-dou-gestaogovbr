"""Hook para o Diário Oficial do Estado de Minas Gerais.

O portal Jornal Minas Gerais expõe uma API usada pela própria interface web:

- ``POST Autenticacao/Autenticar`` com corpo vazio devolve um token anônimo;
- ``GET Pesquisa/PesquisarJornaisPaginados`` pesquisa por termo e período,
  exigindo ``Authorization: Bearer <token>``;
- a paginação usa ``paginaAtual`` (10 resultados por página).

A cadeia TLS do portal está incompleta: o servidor envia apenas o certificado
folha, sem o intermediário Sectigo. O hook monta um bundle com o ``certifi``
do ambiente e o intermediário versionado em ``hooks/certs`` e o usa em
``verify`` — a verificação TLS permanece habilitada.
"""

import hashlib
import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import ClassVar
from urllib.parse import quote, urlencode

import certifi
import requests

try:
    from airflow.sdk.bases.hook import BaseHook
except ImportError:  # pragma: no cover - permite rodar testes sem Airflow
    BaseHook = object

from utils.search_domains import SearchDate, calculate_from_datetime

_CERT_FILE = (
    Path(__file__).resolve().parent
    / "certs"
    / "sectigo_public_server_auth_ca_dv_r36.pem"
)


logger = logging.getLogger(__name__)


class DOEMGHook(BaseHook):
    """Cliente da API pública do Jornal Minas Gerais (DOE-MG)."""

    BASE_URL = "https://www.jornalminasgerais.mg.gov.br/api/v1/"
    AUTH_URL = BASE_URL + "Autenticacao/Autenticar"
    SEARCH_URL = BASE_URL + "Pesquisa/PesquisarJornaisPaginados"
    EDITION_URL = BASE_URL + "Jornal/ObterEdicaoPorDataPublicacao"
    PORTAL_URL = "https://www.jornalminasgerais.mg.gov.br/edicao-do-dia"

    REQUEST_TIMEOUT = (10, 30)  # (conexão, leitura)
    MAX_ATTEMPTS = 3
    BACKOFF_BASE_SECONDS = 2
    RETRY_STATUS: ClassVar[set[int]] = {
        429, 500, 502, 503, 504, 520, 521, 522, 523, 524
    }
    RATE_LIMIT_SECONDS = 1.0
    PAGE_SIZE = 10
    MAX_PAGES = 10  # até 100 resultados por termo

    CADERNOS: ClassVar[dict[str, str]] = {
        "Executivo": "DiarioExecutivo",
        "Municipios": "DiarioMunicipios",
        "Terceiros": "DiarioTerceiros",
        "EdicaoExtra": "EdicaoExtra",
    }
    DEFAULT_CADERNOS: ClassVar[tuple[str, ...]] = ("Executivo", "Municipios")

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._token: str | None = None
        self._cache: dict = {}
        self._last_request_at = 0.0

    # ------------------------------------------------------------------
    # TLS
    # ------------------------------------------------------------------
    @classmethod
    def _ca_bundle_path(cls) -> str:
        """Monta (uma vez) um bundle com certifi + intermediário Sectigo."""
        if not _CERT_FILE.exists():
            raise FileNotFoundError(
                "Certificado intermediário do DOE-MG não encontrado: "
                f"{_CERT_FILE}"
            )

        intermediate = _CERT_FILE.read_bytes()
        digest = hashlib.sha256(intermediate).hexdigest()[:12]
        bundle = Path(tempfile.gettempdir()) / f"rodou-ca-{digest}.pem"

        if not bundle.exists():
            tmp = bundle.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_bytes(Path(certifi.where()).read_bytes() + b"\n" + intermediate)
            os.replace(tmp, bundle)

        return str(bundle)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.RATE_LIMIT_SECONDS:
            time.sleep(self.RATE_LIMIT_SECONDS - elapsed)
        self._last_request_at = time.monotonic()

    def _authenticate(self) -> str:
        response = self._request(
            "POST", self.AUTH_URL, json_body={}, authenticated=False
        )
        payload = response.json()
        token = payload.get("dados")
        if not token:
            raise ValueError("DOE-MG: resposta de autenticação sem token.")
        self._token = token
        return token

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        authenticated: bool = True,
    ) -> requests.Response:
        last_error: Exception | None = None

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            headers = {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (compatible; Ro-DOU; +https://github.com/gestaogovbr/Ro-dou)",
            }
            if authenticated:
                headers["Authorization"] = f"Bearer {self._token or self._authenticate()}"

            self._throttle()
            try:
                response = requests.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=self.REQUEST_TIMEOUT,
                    verify=self._ca_bundle_path(),
                )
            except requests.RequestException as exc:
                last_error = exc
            else:
                if authenticated and response.status_code == 401:
                    # Token expirado/inválido: renova e repete.
                    self._token = None
                    last_error = requests.HTTPError(
                        "DOE-MG: HTTP 401", response=response
                    )
                elif response.status_code not in self.RETRY_STATUS:
                    response.raise_for_status()
                    return response
                else:
                    last_error = requests.HTTPError(
                        f"DOE-MG: HTTP {response.status_code}", response=response
                    )

            if attempt == self.MAX_ATTEMPTS:
                break

            delay = self.BACKOFF_BASE_SECONDS * 2 ** (attempt - 1)
            logger.warning(
                "DOE-MG: tentativa %s de %s falhou (%r); aguardando %ss.",
                attempt,
                self.MAX_ATTEMPTS,
                last_error,
                delay,
            )
            time.sleep(delay)

        raise last_error

    # ------------------------------------------------------------------
    # Busca
    # ------------------------------------------------------------------
    @classmethod
    def _caderno_flags(cls, cadernos: list[str] | None) -> dict:
        selected = cadernos or list(cls.DEFAULT_CADERNOS)
        selected_lower = {str(name).casefold() for name in selected}

        if {"todos", "todas"} & selected_lower:
            enabled = set(cls.CADERNOS.values())
        else:
            enabled = {
                flag
                for name, flag in cls.CADERNOS.items()
                if name.casefold() in selected_lower
            }

        return {
            flag: "true" if flag in enabled else "false"
            for flag in cls.CADERNOS.values()
        }

    def search(
        self,
        terms: list[str],
        reference_date,
        search_date: SearchDate = SearchDate.DIA,
        cadernos: list[str] | None = None,
    ) -> dict:
        """Pesquisa cada termo e devolve ``{termo: [itens]}``."""
        unique_terms = []
        for term in terms or []:
            if term and term not in unique_terms:
                unique_terms.append(term)

        start_date = calculate_from_datetime(reference_date, search_date)
        flags = self._caderno_flags(cadernos)
        cache_key = (
            start_date.isoformat(),
            reference_date.isoformat(),
            tuple(sorted(flags)),
            tuple(unique_terms),
        )
        if cache_key in self._cache:
            return self._cache[cache_key]

        results = {
            term: self._search_term(term, start_date, reference_date, flags)
            for term in unique_terms
        }
        self._cache[cache_key] = results
        return results

    def fetch_edition(self, day) -> dict:
        """Metadados da edição publicada em ``day`` (sem decodificar o PDF)."""
        response = self._request(
            "GET", self.EDITION_URL, params={"dataPublicacao": day.isoformat()}
        )
        return response.json().get("dados") or {}

    def _search_term(self, term, start_date, reference_date, flags) -> list:
        params = {
            "DataPublicacaoInicial": start_date.strftime("%Y-%m-%d"),
            "DataPublicacaoFinal": reference_date.strftime("%Y-%m-%d"),
            "TextoPesquisa": term,
            **flags,
        }

        items = []
        page = 1
        while page <= self.MAX_PAGES:
            response = self._request(
                "GET", self.SEARCH_URL, params={**params, "paginaAtual": page}
            )
            payload = response.json()

            for raw in payload.get("dados") or []:
                items.append(self.normalize(raw, term))

            total_pages = int(payload.get("totalDePaginas") or 1)
            current_page = int(payload.get("paginaAtual") or page)
            if current_page >= total_pages:
                break
            page += 1

        return items

    def normalize(self, raw: dict, term: str) -> dict:
        """Converte um resultado da API no formato consumido pelo Ro-DOU."""
        data_publicacao = raw.get("dataPublicacao") or ""
        try:
            date_str = datetime.fromisoformat(
                data_publicacao.split("T")[0]
            ).strftime("%d/%m/%Y")
        except ValueError:
            date_str = data_publicacao

        caderno = raw.get("tipoCaderno") or "DOE-MG"
        page = raw.get("pagina")
        title = f"{caderno} - página {page}" if page else caderno

        return {
            "title": title,
            "href": self._build_url(raw, term),
            "abstract": raw.get("textoResultado") or "",
            "date": date_str,
            "id": f"{raw.get('idJornal')}:{page}",
            "section": f"DOE-MG - {caderno}",
            "display_date_sortable": data_publicacao,
            "hierarchyList": [],
            "hierarchyStr": "",
            "arttype": "",
        }

    @classmethod
    def _build_url(cls, raw: dict, term: str) -> str:
        dados = {
            "dataPublicacaoSelecionada": raw.get("dataPublicacao"),
            "idCadernoEdicaoSelecionado": raw.get("idJornal"),
            "paginaSelecionada": raw.get("pagina"),
            "textoPesquisa": quote(term or ""),
        }
        return cls.PORTAL_URL + "?" + urlencode(
            {"dados": json.dumps(dados, ensure_ascii=False, separators=(",", ":"))}
        )
