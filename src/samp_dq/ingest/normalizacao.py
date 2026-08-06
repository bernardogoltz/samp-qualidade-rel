"""Normalização lossless do bloco lido — etapa 3 do pipeline de docs/04.

Converte o texto cru que a Parte 2 entrega em tipos (decimal, date, inteiro) e apara espaço de
preenchimento. Nada além disso: **normalizar não é corrigir**. Caixa divergente (`"CATIVO"`),
valor fora do dicionário (`"Regular"`), campo vazio e duplicata passam intactos — mascará-los aqui
destruiria a evidência que o catálogo de regras existe para encontrar.

Duas escolhas merecem explicação:

**`decimal128(20,6)`, não `float64`.** O dicionário pede 20 dígitos de precisão; `float64` garante
uns 17. Num módulo cujo produto é medir qualidade, introduzir erro de arredondamento na própria
normalização seria contraditório. O tipo Arrow atravessa direto para o Parquet, sem conversão.

**Valor ilegível vira nulo, e é contado.** Não se "conserta" nem se descarta a linha: a coluna
tipada fica nula, o relatório conta a ocorrência e guarda até cinco exemplos com o texto original,
e as regras DQ-VAL-002/014 decidem o que isso significa. Onde a regra precisa julgar o formato de
**toda** linha, e não só das que falharam, o texto original fica no Parquet numa coluna `_raw`
(ver `Campo.preservar_raw`).

Campo vazio **não** é valor ilegível: ausência é achado de completude (DQ-COM), formato é achado de
conformidade (DQ-VAL). Confundi-los inflaria uma dimensão às custas da outra.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pyarrow as pa

from samp_dq.ingest.leitura import MAX_EXEMPLOS, RelatorioLeitura
from samp_dq.ingest.schema import (
    ENCODING_ORIGEM,
    ESQUEMA_SAMP,
    SUFIXO_RAW,
    Campo,
    Esquema,
    TipoCampo,
)

#: Formato das datas publicadas (ISO), conforme docs/01.
FORMATO_DATA = "%Y-%m-%d"

_PADRAO_INTEIRO = r"-?\d+"


def tipo_arrow(campo: Campo) -> pa.DataType:
    """O tipo Arrow de um campo — o mesmo na normalização e no Parquet, por construção."""
    if campo.tipo is TipoCampo.TEXTO:
        return pa.string()
    if campo.tipo is TipoCampo.INTEIRO:
        return pa.int64()
    if campo.tipo is TipoCampo.DATA:
        return pa.date32()
    return pa.decimal128(campo.precisao or 20, campo.escala or 0)


def _padrao_decimal(campo: Campo) -> str:
    """Regex derivada da precisão declarada: decimal(20,6) admite 14 dígitos antes do ponto.

    A alternativa sem parte inteira (`,31` para 0,31) é aceita de propósito: ela ocorre em ~1%
    dos registros reais, e o valor é inequívoco. Recusá-la anularia dado recuperável — o oposto
    de lossless. O achado de formato não se perde: é contado em `decimais_sem_parte_inteira` e o
    texto original fica em `VlrMercado_raw` para a DQ-VAL-014 julgar.
    """
    precisao = campo.precisao or 20
    escala = campo.escala or 0
    inteiro = rf"\d{{1,{precisao - escala}}}"
    fracao = rf"\.\d{{1,{escala}}}"
    return rf"-?(?:{inteiro}(?:{fracao})?|{fracao})"


@dataclass(frozen=True, slots=True)
class ValorInvalido:
    """Um valor que não converteu para o tipo do campo."""

    campo: str
    #: Posição entre os registros lidos (1-based). Não é a linha do arquivo: se o parser
    #: descartou alguma linha, os dois números divergem.
    registro: int
    valor: str


@dataclass(slots=True)
class RelatorioNormalizacao:
    """O que a normalização fez. Vira o bloco `normalizacoes` do `perfil-{ano}.json`."""

    encoding_origem: str = ENCODING_ORIGEM
    linhas: int = 0
    decimais_reparados: int = 0
    #: Decimais escritos sem a parte inteira (",31"); normalização aplicada, e achado da DQ-VAL-014.
    decimais_sem_parte_inteira: int = 0
    campos_trimados: dict[str, int] = field(default_factory=dict)
    valores_invalidos: dict[str, int] = field(default_factory=dict)
    exemplos_invalidos: tuple[ValorInvalido, ...] = ()

    def como_perfil(self, leitura: RelatorioLeitura | None = None) -> dict[str, Any]:
        """O bloco `normalizacoes` no contrato de docs/04 §6.

        `linhasMalformadas` pertence à leitura, não à normalização; passe o relatório dela para
        que o bloco saia completo.
        """
        return {
            "encodingConvertido": f"{self.encoding_origem} -> utf-8",
            "decimaisReparados": self.decimais_reparados,
            "decimaisSemParteInteira": self.decimais_sem_parte_inteira,
            "camposTrimados": dict(self.campos_trimados),
            "linhasMalformadas": leitura.linhas_descartadas if leitura else 0,
            "valoresIlegiveis": dict(self.valores_invalidos),
        }

    def resumo(self) -> str:
        texto = f"{self.linhas} linhas normalizadas; {self.decimais_reparados} decimais reparados"
        trimados = sum(self.campos_trimados.values())
        if trimados:
            texto += f"; {trimados} campos aparados"
        ilegiveis = sum(self.valores_invalidos.values())
        if ilegiveis:
            plural = "es" if ilegiveis > 1 else ""
            texto += f"; {ilegiveis} valor{plural} ilegí{'veis' if ilegiveis > 1 else 'vel'}"
        return texto


@dataclass(slots=True)
class Normalizador:
    """Aplica a normalização bloco a bloco, acumulando um relatório::

    leitor = LeitorCsv("bruto/samp-2024.csv")
    normalizador = Normalizador()
    for bloco in normalizador.normalizar_blocos(leitor.blocos()):
        ...
    print(normalizador.relatorio.resumo())
    """

    esquema: Esquema = ESQUEMA_SAMP
    _relatorio: RelatorioNormalizacao = field(init=False, repr=False)
    _processadas: int = field(init=False, default=0, repr=False)

    def __post_init__(self) -> None:
        self._relatorio = RelatorioNormalizacao()

    @property
    def relatorio(self) -> RelatorioNormalizacao:
        return self._relatorio

    def normalizar_blocos(self, blocos: Iterable[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for bloco in blocos:
            yield self.normalizar(bloco)

    def normalizar(self, bloco: pd.DataFrame) -> pd.DataFrame:
        """Devolve o bloco tipado, com as colunas do esquema na ordem e as `_raw` no fim."""
        offset = self._processadas
        colunas: dict[str, pd.Series[Any]] = {}
        cruas: dict[str, pd.Series[Any]] = {}

        for campo in self.esquema:
            if campo.nome not in bloco.columns:
                continue
            original = bloco[campo.nome].astype("str")
            if campo.preservar_raw:
                cruas[campo.nome + SUFIXO_RAW] = original
            aparado = self._aparar(campo.nome, original)
            colunas[campo.nome] = self._tipar(campo, aparado, offset)

        self._processadas += len(bloco)
        self._relatorio.linhas += len(bloco)
        return pd.DataFrame({**colunas, **cruas}, index=bloco.index).reset_index(drop=True)

    def _aparar(self, nome: str, valores: pd.Series[Any]) -> pd.Series[Any]:
        """Remove espaço de preenchimento, contando quantos valores mudaram."""
        aparado = valores.str.strip()
        mudaram = int((aparado != valores).sum())
        if mudaram:
            self._relatorio.campos_trimados[nome] = (
                self._relatorio.campos_trimados.get(nome, 0) + mudaram
            )
        return aparado

    def _tipar(self, campo: Campo, valores: pd.Series[Any], offset: int) -> pd.Series[Any]:
        if campo.tipo is TipoCampo.TEXTO:
            return valores
        if campo.tipo is TipoCampo.DATA:
            return self._para_data(campo, valores, offset)
        if campo.tipo is TipoCampo.INTEIRO:
            return self._para_arrow(
                campo, valores, valores, _PADRAO_INTEIRO, tipo_arrow(campo), offset
            )
        pontos = valores.str.replace(",", ".", regex=False)
        return self._para_arrow(
            campo,
            valores,
            pontos,
            _padrao_decimal(campo),
            tipo_arrow(campo),
            offset,
            contar_reparos=True,
        )

    def _para_arrow(
        self,
        campo: Campo,
        originais: pd.Series[Any],
        candidatos: pd.Series[Any],
        padrao: str,
        tipo: pa.DataType,
        offset: int,
        *,
        contar_reparos: bool = False,
    ) -> pd.Series[Any]:
        """Converte via pyarrow, deixando nulo o que não casa com o padrão.

        O casamento é feito antes do cast porque o `cast` do pyarrow levanta no primeiro valor
        ruim — e aqui um valor ruim é um achado a contabilizar, não um motivo para abortar.
        """
        vazio = originais.str.len() == 0
        valido = candidatos.str.fullmatch(padrao).fillna(False).astype(bool)
        self._registrar_invalidos(campo.nome, originais, ~valido & ~vazio, offset)

        if contar_reparos:
            reparados = int((valido & originais.str.contains(",", regex=False)).sum())
            self._relatorio.decimais_reparados += reparados
            sem_inteiro = int((valido & candidatos.str.match(r"-?\.")).sum())
            self._relatorio.decimais_sem_parte_inteira += sem_inteiro

        limpos = candidatos.where(valido, None)
        convertido = pa.array(limpos, type=pa.string()).cast(tipo)
        return pd.Series(convertido.to_pandas(), dtype=pd.ArrowDtype(tipo))

    def _para_data(self, campo: Campo, valores: pd.Series[Any], offset: int) -> pd.Series[Any]:
        vazio = valores.str.len() == 0
        momentos = pd.to_datetime(valores, format=FORMATO_DATA, errors="coerce")
        self._registrar_invalidos(campo.nome, valores, momentos.isna() & ~vazio, offset)
        convertido = pa.array(momentos.dt.date, type=pa.date32())
        return pd.Series(convertido.to_pandas(), dtype=pd.ArrowDtype(pa.date32()))

    def _registrar_invalidos(
        self, nome: str, valores: pd.Series[Any], invalidos: pd.Series[Any], offset: int
    ) -> None:
        quantos = int(invalidos.sum())
        if not quantos:
            return
        self._relatorio.valores_invalidos[nome] = (
            self._relatorio.valores_invalidos.get(nome, 0) + quantos
        )
        ja_guardados = sum(1 for e in self._relatorio.exemplos_invalidos if e.campo == nome)
        if ja_guardados >= MAX_EXEMPLOS:
            return
        novos = [
            ValorInvalido(campo=nome, registro=offset + posicao + 1, valor=valores.iloc[posicao])
            for posicao in invalidos.to_numpy().nonzero()[0][: MAX_EXEMPLOS - ja_guardados]
        ]
        self._relatorio.exemplos_invalidos = (*self._relatorio.exemplos_invalidos, *novos)


__all__ = [
    "FORMATO_DATA",
    "Normalizador",
    "RelatorioNormalizacao",
    "ValorInvalido",
    "tipo_arrow",
]
