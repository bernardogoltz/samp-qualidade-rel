"""Hierarquia de erros do samp-dq.

Tudo que o módulo levanta herda de `SampDQError`, para que quem o usa como biblioteca
possa capturar uma única classe.
"""

from __future__ import annotations

from typing import Any


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


class IngestaoError(SampDQError):
    """Falha ao ler ou normalizar um arquivo do SAMP."""


class ArquivoVazioError(IngestaoError):
    """O arquivo não tem sequer a linha de cabeçalho."""

    def __init__(self, caminho: object) -> None:
        self.caminho = caminho
        super().__init__(f"arquivo vazio: {caminho}")


class CabecalhoInvalidoError(IngestaoError):
    """O cabeçalho não corresponde ao esquema — provável mudança de layout na origem."""

    def __init__(self, caminho: object, resultado: Any) -> None:
        self.caminho = caminho
        self.resultado = resultado
        super().__init__(f"{caminho}: {resultado.resumo()}")


class EstruturaInconsistenteError(IngestaoError):
    """Uma linha de dados é mais larga que o cabeçalho e o parser truncou os campos excedentes.

    Só ocorre quando a linha larga é a **primeira** do arquivo: as demais o parser descarta,
    informando a posição. Neste caso ele não informa, então a perda não é localizável — daí
    abortar em vez de contabilizar.
    """

    def __init__(self, caminho: object) -> None:
        self.caminho = caminho
        super().__init__(
            f"{caminho}: uma linha de dados tem mais campos que o cabeçalho e foi truncada; "
            "o parser não informa a posição. Releia com estrito=False para inspecionar o arquivo"
        )


class EncodingInvalidoError(IngestaoError):
    """O arquivo não decodifica no encoding declarado (base da DQ-VAL-015)."""

    def __init__(self, caminho: object, encoding: str, detalhe: str) -> None:
        self.caminho = caminho
        self.encoding = encoding
        self.detalhe = detalhe
        super().__init__(f"{caminho} não decodifica como {encoding}: {detalhe}")


class EscritaError(IngestaoError):
    """Falha ao gravar o Parquet de saída."""


class CampoDesconhecidoError(SampDQError):
    """O campo pedido não existe no esquema do SAMP."""

    def __init__(self, nome: str, conhecidos: tuple[str, ...] = ()) -> None:
        self.nome = nome
        self.conhecidos = conhecidos
        sugestao = _mais_parecido(nome, conhecidos)
        dica = f"; você quis dizer '{sugestao}'?" if sugestao else ""
        super().__init__(f"campo '{nome}' não existe no esquema do SAMP{dica}")


def _mais_parecido(nome: str, candidatos: tuple[str, ...]) -> str | None:
    """A confusão frequente é de caixa (`IdeNucleoCEG` vs. `IdeNucleoCeg`); resolver isso ajuda
    mais que listar os 18 campos."""
    alvo = nome.casefold()
    return next((c for c in candidatos if c.casefold() == alvo), None)


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
