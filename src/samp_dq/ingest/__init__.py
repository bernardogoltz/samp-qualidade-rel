"""Ingestão: do CSV bruto do SAMP ao Parquet tipado.

Etapas 2 a 4 do pipeline de docs/04: leitura (cp1252, `;`, tudo texto), normalização *lossless*
e escrita do Parquet. Prontas: o contrato do esquema e a leitura em blocos.
"""

from __future__ import annotations

from samp_dq.ingest.leitura import (
    MAX_EXEMPLOS,
    TAMANHO_BLOCO_PADRAO,
    LeitorCsv,
    LinhaDescartada,
    RelatorioLeitura,
)
from samp_dq.ingest.schema import (
    ENCODING_ORIGEM,
    ESQUEMA_SAMP,
    SEPARADOR,
    Campo,
    Esquema,
    ResultadoCabecalho,
    TipoCampo,
    dividir_cabecalho,
)

__all__ = [
    "ENCODING_ORIGEM",
    "ESQUEMA_SAMP",
    "MAX_EXEMPLOS",
    "SEPARADOR",
    "TAMANHO_BLOCO_PADRAO",
    "Campo",
    "Esquema",
    "LeitorCsv",
    "LinhaDescartada",
    "RelatorioLeitura",
    "ResultadoCabecalho",
    "TipoCampo",
    "dividir_cabecalho",
]
