"""Leitura do CSV bruto do SAMP — etapa 2 do pipeline de docs/04.

Lê o arquivo **em blocos**, tudo como texto, sem interpretar nada: tipagem, decimal e trim são
etapa 3, e antecipá-los aqui apagaria a evidência que as regras de qualidade precisam ver
(DQ-VAL-014 avalia o formato decimal *original*, DQ-VAL-011 os espaços de preenchimento).

Blocos são obrigatórios, não uma otimização: `samp-2024.csv` tem 369 MB e ~3,5 milhões de linhas;
como DataFrame de strings não caberia confortavelmente em memória.

## Linha malformada

O pandas trata os dois casos de forma assimétrica, e só um deles é perigoso:

- **campos a mais** — o parser **descarta a linha** e apenas emite um `ParserWarning`. Descarte
  silencioso é o que docs/04 §7 proíbe, então este módulo captura esses avisos e contabiliza cada
  linha perdida (número, campos encontrados e um trecho do texto original).
- **campos a menos** — a linha é aceita, com o final preenchido de vazio. Nada se perde: os campos
  vazios seguem para o Parquet e viram achado de completude (DQ-COM-001/002).
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd
from pandas.errors import ParserWarning

from samp_dq.errors import (
    ArquivoVazioError,
    CabecalhoInvalidoError,
    EncodingInvalidoError,
    EstruturaInconsistenteError,
)
from samp_dq.ingest.schema import (
    ENCODING_ORIGEM,
    ESQUEMA_SAMP,
    SEPARADOR,
    Esquema,
    ResultadoCabecalho,
    dividir_cabecalho,
)

#: Linhas por bloco. 250 mil linhas por 18 colunas de texto ficam na casa de centenas de MB.
TAMANHO_BLOCO_PADRAO = 250_000

#: Exemplos guardados por tipo de achado — o mesmo limite dos JSONs lidos pelos agentes.
MAX_EXEMPLOS = 5

#: Tamanho do trecho guardado de uma linha descartada.
MAX_TRECHO = 200

# "Skipping line 6: expected 18 fields, saw 21"
_AVISO_DESCARTE = re.compile(r"Skipping line (\d+): expected (\d+) fields, saw (\d+)")

# Emitido quando a primeira linha de dados é mais larga que o cabeçalho: o parser trunca os
# campos excedentes e não diz onde. Ver EstruturaInconsistenteError.
_AVISO_LARGURA = re.compile(r"Length of header or names does not match length of data")


@dataclass(frozen=True, slots=True)
class LinhaDescartada:
    """Uma linha que o parser jogou fora por ter campos demais."""

    numero: int  # posição no arquivo, 1-based, contando o cabeçalho
    campos_encontrados: int
    campos_esperados: int
    trecho: str = ""


@dataclass(slots=True)
class RelatorioLeitura:
    """O que aconteceu durante a leitura. Alimenta o bloco `normalizacoes` do perfil."""

    caminho: Path
    encoding: str
    cabecalho: ResultadoCabecalho | None = None
    linhas_lidas: int = 0
    blocos: int = 0
    linhas_descartadas: int = 0
    exemplos_descartadas: tuple[LinhaDescartada, ...] = ()
    #: Campos excedentes truncados sem posição informada (ver `EstruturaInconsistenteError`).
    truncamento_de_largura: bool = False

    def resumo(self) -> str:
        texto = f"{self.caminho.name}: {self.linhas_lidas} linhas em {self.blocos} blocos"
        if self.linhas_descartadas:
            plural = "s" if self.linhas_descartadas > 1 else ""
            texto += f"; {self.linhas_descartadas} linha{plural} descartada{plural} (campos demais)"
        if self.truncamento_de_largura:
            texto += "; linha mais larga que o cabeçalho truncada em posição desconhecida"
        return texto


@dataclass(slots=True)
class LeitorCsv:
    """Lê um CSV do SAMP em blocos de `DataFrame`, tudo como texto.

        leitor = LeitorCsv("bruto/samp-2024.csv")
        for bloco in leitor.blocos():
            ...
        print(leitor.relatorio.resumo())

    Com `estrito` (padrão), um cabeçalho divergente aborta antes de ler qualquer dado — é o
    sintoma de mudança de layout na origem, e processar assim mesmo produziria um Parquet
    silenciosamente errado. Com `estrito=False` a leitura prossegue e a divergência fica só
    registrada no relatório, para quem quer diagnosticar o arquivo novo.
    """

    caminho: Path
    esquema: Esquema = ESQUEMA_SAMP
    encoding: str = ENCODING_ORIGEM
    tamanho_bloco: int = TAMANHO_BLOCO_PADRAO
    estrito: bool = True
    _relatorio: RelatorioLeitura = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.caminho = Path(self.caminho)
        self._relatorio = RelatorioLeitura(caminho=self.caminho, encoding=self.encoding)

    @property
    def relatorio(self) -> RelatorioLeitura:
        return self._relatorio

    def cabecalho(self) -> ResultadoCabecalho:
        """Lê e valida só a primeira linha. Idempotente."""
        if self._relatorio.cabecalho is not None:
            return self._relatorio.cabecalho

        with (
            self._traduzir_erro_de_encoding(),
            self.caminho.open(encoding=self.encoding, errors="strict") as arquivo,
        ):
            primeira = arquivo.readline()

        if not primeira.strip():
            raise ArquivoVazioError(self.caminho)

        resultado = self.esquema.validar_cabecalho(dividir_cabecalho(primeira))
        self._relatorio.cabecalho = resultado
        if self.estrito and not resultado.conforme:
            raise CabecalhoInvalidoError(self.caminho, resultado)
        return resultado

    def blocos(self) -> Iterator[pd.DataFrame]:
        """Percorre o arquivo em blocos, acumulando o relatório."""
        cabecalho = self.cabecalho()
        with self._traduzir_erro_de_encoding():
            iterador = pd.read_csv(
                self.caminho,
                sep=SEPARADOR,
                encoding=self.encoding,
                encoding_errors="strict",
                # As colunas vêm do cabeçalho que já lemos e validamos. Declará-las fixa a
                # largura esperada desde a primeira linha; deixar o pandas inferi-la pela
                # primeira linha de dados faria uma linha larga *deslocar* as colunas de todo
                # o arquivo, em silêncio.
                names=list(cabecalho.colunas),
                header=0,
                index_col=False,
                # Tudo como texto e sem interpretar vazio como nulo: a etapa 3 é que decide o
                # que é ausência e o que é string vazia.
                dtype=str,
                na_filter=False,
                chunksize=self.tamanho_bloco,
                on_bad_lines="warn",
            )
            yield from self._percorrer(iterador)

        if self._relatorio.exemplos_descartadas:
            self._carregar_trechos()

    def ler_tudo(self) -> pd.DataFrame:
        """Todos os blocos num só `DataFrame`. Só para arquivos pequenos — testes e anos antigos."""
        partes = list(self.blocos())
        if not partes:
            colunas = self._relatorio.cabecalho.colunas if self._relatorio.cabecalho else ()
            return pd.DataFrame(columns=list(colunas), dtype=str)
        return pd.concat(partes, ignore_index=True)

    def _percorrer(self, iterador: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Consome o iterador do pandas capturando os avisos de cada bloco.

        A captura envolve apenas o `next()`, e não o `yield`: manter o filtro de warnings ativo
        enquanto quem chama processa o bloco mudaria o comportamento do código dele.
        """
        while True:
            with warnings.catch_warnings(record=True) as capturados:
                warnings.simplefilter("always")
                try:
                    bloco = next(iterador)
                except StopIteration:
                    break
            self._registrar_descartes(capturados)
            # Arquivo só com cabeçalho: o pandas devolve um bloco vazio, que não é um bloco.
            if bloco.empty:
                continue
            self._relatorio.linhas_lidas += len(bloco)
            self._relatorio.blocos += 1
            yield bloco

    def _registrar_descartes(self, capturados: list[warnings.WarningMessage]) -> None:
        exemplos = list(self._relatorio.exemplos_descartadas)
        for aviso in capturados:
            if not issubclass(aviso.category, ParserWarning):
                warnings.warn_explicit(aviso.message, aviso.category, aviso.filename, aviso.lineno)
                continue
            mensagem = str(aviso.message)
            if _AVISO_LARGURA.search(mensagem):
                self._relatorio.truncamento_de_largura = True
                if self.estrito:
                    raise EstruturaInconsistenteError(self.caminho)
                continue
            # Um aviso pode agrupar mais de uma linha descartada.
            for achado in _AVISO_DESCARTE.finditer(mensagem):
                numero, esperados, encontrados = (int(g) for g in achado.groups())
                self._relatorio.linhas_descartadas += 1
                if len(exemplos) < MAX_EXEMPLOS:
                    exemplos.append(
                        LinhaDescartada(
                            numero=numero,
                            campos_encontrados=encontrados,
                            campos_esperados=esperados,
                        )
                    )
        self._relatorio.exemplos_descartadas = tuple(exemplos)

    def _carregar_trechos(self) -> None:
        """Busca o texto original das linhas descartadas — uma passada, só se houve descarte."""
        procurados = {e.numero: e for e in self._relatorio.exemplos_descartadas}
        trechos: dict[int, str] = {}
        with self.caminho.open(encoding=self.encoding, errors="replace") as arquivo:
            for numero, linha in enumerate(arquivo, start=1):
                if numero in procurados:
                    trechos[numero] = _truncar(linha.rstrip("\r\n"))
                    if len(trechos) == len(procurados):
                        break
        self._relatorio.exemplos_descartadas = tuple(
            LinhaDescartada(
                numero=e.numero,
                campos_encontrados=e.campos_encontrados,
                campos_esperados=e.campos_esperados,
                trecho=trechos.get(e.numero, ""),
            )
            for e in self._relatorio.exemplos_descartadas
        )

    def _traduzir_erro_de_encoding(self) -> _TraducaoEncoding:
        return _TraducaoEncoding(self.caminho, self.encoding)


def _truncar(texto: str) -> str:
    return texto if len(texto) <= MAX_TRECHO else texto[:MAX_TRECHO] + "..."


@dataclass(slots=True)
class _TraducaoEncoding:
    """Converte o `UnicodeDecodeError` cru num erro do módulo, com o byte e a posição."""

    caminho: Path
    encoding: str

    def __enter__(self) -> None:
        return None

    def __exit__(self, tipo: object, valor: BaseException | None, tb: object) -> Literal[False]:
        if isinstance(valor, UnicodeDecodeError):
            detalhe = (
                f"byte 0x{valor.object[valor.start]:02x} na posição {valor.start} ({valor.reason})"
            )
            raise EncodingInvalidoError(self.caminho, self.encoding, detalhe) from valor
        return False
