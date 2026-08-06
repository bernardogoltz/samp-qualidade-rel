"""Cliente da API CKAN do portal de dados abertos da ANEEL.

Responsabilidades: montar a chamada, sobreviver à instabilidade do portal (retentativa com
backoff) e traduzir a resposta em objetos de domínio ou em erros com mensagem útil.

O portal é externo e ocasionalmente indisponível; por isso a política de retentativa é parte do
contrato do cliente, e não algo que quem chama precise reimplementar.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from types import TracebackType
from typing import Any, Self

import httpx

from samp_dq.ckan.models import Dataset
from samp_dq.errors import CkanHTTPError, CkanRespostaInvalidaError

PORTAL_ANEEL = "https://dadosabertos.aneel.gov.br"
"""Portal de dados abertos da ANEEL."""

DATASET_SAMP = "3e153db4-a503-4093-88be-75d31b002dcf"
"""Id do dataset `samp` no CKAN (o slug `samp` também funciona)."""

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 samp-dq/0.1 "
    "(+https://github.com/bergoltzx2/samp-dq)"
)
"""O portal responde 403 a user-agents de biblioteca; usa-se um de navegador, mas o módulo
também se identifica para que a ANEEL saiba quem está acessando."""

TENTATIVAS_PADRAO = 4
ESPERA_INICIAL_PADRAO = 1.0
TIMEOUT_PADRAO = 60.0

# Status transitórios: vale repetir. Os demais 4xx são definitivos.
_STATUS_REPETIVEIS = frozenset({408, 425, 429, 500, 502, 503, 504})


def _retry_after(resposta: httpx.Response) -> float | None:
    """Segundos pedidos pelo servidor no cabeçalho `Retry-After`, quando em formato numérico."""
    cabecalho = resposta.headers.get("retry-after")
    if not cabecalho:
        return None
    try:
        return max(0.0, float(cabecalho.strip()))
    except ValueError:
        return None  # a forma com data HTTP é rara aqui; cai no backoff normal


class CkanClient:
    """Acesso somente-leitura à API CKAN.

    Serve tanto como gerenciador de contexto quanto como objeto de vida longa:

        with CkanClient() as cliente:
            dataset = cliente.package_show()
    """

    def __init__(
        self,
        base_url: str = PORTAL_ANEEL,
        *,
        sessao: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        timeout: float = TIMEOUT_PADRAO,
        tentativas: int = TENTATIVAS_PADRAO,
        espera_inicial: float = ESPERA_INICIAL_PADRAO,
        pausa: Callable[[float], None] = time.sleep,
        user_agent: str = USER_AGENT,
    ) -> None:
        if tentativas < 1:
            raise ValueError("tentativas deve ser >= 1")
        self.base_url = base_url.rstrip("/")
        self.tentativas = tentativas
        self.espera_inicial = espera_inicial
        self._pausa = pausa
        # Sessão emprestada continua sendo do chamador: não a fechamos.
        self._sessao_propria = sessao is None
        self._sessao = sessao or httpx.Client(
            base_url=self.base_url,
            transport=transport,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )

    # ------------------------------------------------------------------ ciclo de vida

    @property
    def sessao(self) -> httpx.Client:
        """Sessão HTTP subjacente (reaproveitada pelo downloader)."""
        return self._sessao

    @property
    def esta_fechado(self) -> bool:
        return self._sessao.is_closed

    def fechar(self) -> None:
        if self._sessao_propria:
            self._sessao.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        tipo: type[BaseException] | None,
        valor: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.fechar()

    # ------------------------------------------------------------------ HTTP

    def requisitar(
        self,
        metodo: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        """Faz a requisição repetindo falhas transitórias, com backoff exponencial.

        Com `stream=True` devolve a resposta sem consumir o corpo — quem chama fica
        responsável por lê-la e fechá-la.
        """
        espera = self.espera_inicial
        ultimo_erro: Exception | None = None

        for tentativa in range(1, self.tentativas + 1):
            pausa_pedida: float | None = None
            try:
                pedido = self._sessao.build_request(metodo, url, params=params, headers=headers)
                resposta = self._sessao.send(pedido, stream=stream)
            except httpx.HTTPError as erro:
                ultimo_erro = erro
            else:
                if resposta.status_code < 400:
                    return resposta

                corpo = "" if stream else resposta.text
                if stream:
                    resposta.close()
                ultimo_erro = CkanHTTPError(resposta.status_code, str(resposta.url), corpo)
                if resposta.status_code not in _STATUS_REPETIVEIS:
                    raise ultimo_erro
                pausa_pedida = _retry_after(resposta)

            if tentativa < self.tentativas:
                self._pausa(espera if pausa_pedida is None else pausa_pedida)
                espera *= 2

        assert ultimo_erro is not None
        if isinstance(ultimo_erro, CkanHTTPError):
            raise ultimo_erro
        raise CkanRespostaInvalidaError(
            f"falha de rede ao acessar {url} após {self.tentativas} tentativa(s): {ultimo_erro}"
        ) from ultimo_erro

    # ------------------------------------------------------------------ ações CKAN

    def package_show(self, id_dataset: str = DATASET_SAMP) -> Dataset:
        """Metadados do dataset e a lista de recursos publicados."""
        resposta = self.requisitar("GET", "/api/3/action/package_show", params={"id": id_dataset})
        return Dataset.de_ckan(self._resultado(resposta))

    @staticmethod
    def _resultado(resposta: httpx.Response) -> dict[str, Any]:
        """Desembrulha o envelope `{success, result}` do CKAN."""
        try:
            corpo = resposta.json()
        except (json.JSONDecodeError, ValueError) as erro:
            trecho = resposta.text[:200]
            raise CkanRespostaInvalidaError(
                f"resposta de {resposta.url} não é JSON (o portal pode estar em manutenção): "
                f"{trecho!r}"
            ) from erro

        if not isinstance(corpo, dict):
            raise CkanRespostaInvalidaError(f"envelope inesperado do CKAN em {resposta.url}")

        if not corpo.get("success"):
            erro_ckan = corpo.get("error") or {}
            detalhe = erro_ckan.get("message") if isinstance(erro_ckan, dict) else erro_ckan
            raise CkanRespostaInvalidaError(
                f"CKAN recusou a chamada a {resposta.url}: {detalhe or 'sem detalhe'}"
            )

        resultado = corpo.get("result")
        if not isinstance(resultado, dict):
            raise CkanRespostaInvalidaError(
                f"resposta de {resposta.url} veio sem o objeto 'result'"
            )
        return resultado
