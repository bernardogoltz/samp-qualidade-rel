"""Ingestão: do CSV bruto do SAMP ao Parquet tipado.

Etapas 2 a 4 do pipeline de docs/04: leitura (cp1252, `;`, tudo texto), normalização *lossless*
e escrita do Parquet — todas prontas.
"""

from __future__ import annotations

from samp_dq.ingest.leitura import (
    MAX_EXEMPLOS,
    TAMANHO_BLOCO_PADRAO,
    LeitorCsv,
    LinhaDescartada,
    RelatorioLeitura,
)
from samp_dq.ingest.normalizacao import (
    FORMATO_DATA,
    Normalizador,
    RelatorioNormalizacao,
    ValorInvalido,
    tipo_arrow,
)
from samp_dq.ingest.parquet import (
    COMPRESSAO_PADRAO,
    ResultadoEscrita,
    StatusEscrita,
    chave_do_insumo,
    escrever_parquet,
    esquema_arrow,
)
from samp_dq.ingest.schema import (
    ENCODING_ORIGEM,
    ESQUEMA_SAMP,
    SEPARADOR,
    SUFIXO_RAW,
    Campo,
    Esquema,
    ResultadoCabecalho,
    TipoCampo,
    dividir_cabecalho,
)

__all__ = [
    "COMPRESSAO_PADRAO",
    "ENCODING_ORIGEM",
    "ESQUEMA_SAMP",
    "FORMATO_DATA",
    "MAX_EXEMPLOS",
    "SEPARADOR",
    "SUFIXO_RAW",
    "TAMANHO_BLOCO_PADRAO",
    "Campo",
    "Esquema",
    "LeitorCsv",
    "LinhaDescartada",
    "Normalizador",
    "RelatorioLeitura",
    "RelatorioNormalizacao",
    "ResultadoCabecalho",
    "ResultadoEscrita",
    "StatusEscrita",
    "TipoCampo",
    "ValorInvalido",
    "chave_do_insumo",
    "dividir_cabecalho",
    "escrever_parquet",
    "esquema_arrow",
    "tipo_arrow",
]
