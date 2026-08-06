"""Hierarquia de erros do samp-dq.

Tudo que o módulo levanta herda de `SampDQError`, para que quem o usa como biblioteca
possa capturar uma única classe.
"""

from __future__ import annotations


class SampDQError(Exception):
    """Erro base do samp-dq."""


class CkanError(SampDQError):
    """Falha ao conversar com a API CKAN do portal de dados abertos."""


class CkanHTTPError(CkanError):
    """O portal respondeu com um status HTTP de erro."""

    def __init__(self, status: int, url: str, corpo: str = "") -> None:
        self.status = status
        self.url = url
        self.corpo = corpo
        trecho = f": {corpo[:200]}" if corpo else ""
        super().__init__(f"CKAN respondeu HTTP {status} em {url}{trecho}")


class CkanRespostaInvalidaError(CkanError):
    """A resposta não é o JSON que a API CKAN promete."""


class RecursoNaoEncontradoError(SampDQError):
    """Nenhum recurso do dataset corresponde ao filtro pedido."""


class DownloadError(SampDQError):
    """Falha ao baixar um recurso."""


class DownloadIncompletoError(DownloadError):
    """O arquivo baixado não bate com o tamanho anunciado pelo servidor."""

    def __init__(self, url: str, esperado: int, obtido: int) -> None:
        self.url = url
        self.esperado = esperado
        self.obtido = obtido
        super().__init__(
            f"download incompleto de {url}: esperados {esperado} bytes, obtidos {obtido}"
        )
