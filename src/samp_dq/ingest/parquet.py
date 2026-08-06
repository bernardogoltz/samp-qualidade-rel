"""Escrita do Parquet tipado e idempotência — etapa 4 do pipeline de docs/04.

Grava `samp-{ano}.parquet` consumindo os blocos já normalizados, um grupo de linhas por bloco.
Nada é acumulado em memória: o arquivo anual sai do CSV para o Parquet em fluxo.

**Nunca resultado parcial.** A escrita vai para um `.part` e só então é renomeada — operação
atômica no mesmo sistema de arquivos. Uma falha no meio não deixa Parquet truncado nem destrói o
da execução anterior, que é o que docs/04 §10 exige.

**Idempotência por conteúdo do insumo.** Um sidecar `{nome}.samp-dq.json` guarda a chave do CSV
que originou o Parquet; reexecutar sobre o mesmo insumo devolve `EM_CACHE` **sem tocar nos
blocos** — o ponto é justamente não reler centenas de MB. A chave sai do SHA-256 que o download
já calculou, quando o sidecar dele está por perto (ver `chave_do_insumo`).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from samp_dq.ckan.download import SUFIXO_SIDECAR
from samp_dq.errors import EscritaError
from samp_dq.ingest.normalizacao import tipo_arrow
from samp_dq.ingest.schema import ESQUEMA_SAMP, SUFIXO_RAW, Esquema

#: zstd comprime melhor que snappy com custo de CPU semelhante na leitura.
COMPRESSAO_PADRAO = "zstd"

_SUFIXO_PARCIAL = ".part"
_BLOCO_HASH = 1024 * 1024


class StatusEscrita(StrEnum):
    """Como a escrita terminou."""

    GRAVADO = "gravado"
    EM_CACHE = "em_cache"


@dataclass(frozen=True, slots=True)
class ResultadoEscrita:
    """O que saiu da escrita — insumo do log e do `perfil-{ano}.json`."""

    caminho: Path
    status: StatusEscrita
    linhas: int
    tamanho: int
    chave: str

    @property
    def reaproveitado(self) -> bool:
        return self.status is StatusEscrita.EM_CACHE

    def resumo(self) -> str:
        rotulo = "reaproveitado" if self.reaproveitado else "gravado"
        return f"{self.caminho.name}: {rotulo} — {self.linhas} linhas, {self.tamanho / 1e6:.1f} MB"


def esquema_arrow(esquema: Esquema = ESQUEMA_SAMP, *, com_raw: bool = True) -> pa.Schema:
    """O esquema Arrow do Parquet: as colunas do contrato, e as `_raw` no fim.

    Declará-lo explicitamente (em vez de deixar o pyarrow inferir de cada bloco) garante que
    todos os grupos de linhas tenham o mesmo tipo — um bloco só com nulos, por exemplo, não
    consegue inferir sozinho que a coluna é decimal.
    """
    campos = [pa.field(c.nome, tipo_arrow(c)) for c in esquema]
    if com_raw:
        campos += [pa.field(c.nome + SUFIXO_RAW, pa.string()) for c in esquema if c.preservar_raw]
    return pa.schema(campos)


def chave_do_insumo(caminho: Path | str) -> str:
    """Identifica o conteúdo do CSV de origem.

    Se o sidecar do download estiver ao lado, aproveita o SHA-256 que ele já registrou — reler
    369 MB só para decidir se dá para pular o trabalho anularia o ganho da idempotência.
    """
    caminho = Path(caminho)
    registro = _ler_json(caminho.with_name(caminho.name + SUFIXO_SIDECAR))
    do_sidecar = str((registro or {}).get("sha256") or "")
    if do_sidecar:
        return do_sidecar

    digest = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        while pedaco := arquivo.read(_BLOCO_HASH):
            digest.update(pedaco)
    return digest.hexdigest()


def escrever_parquet(
    blocos: Iterable[pd.DataFrame],
    destino: Path | str,
    *,
    chave: str,
    esquema: Esquema = ESQUEMA_SAMP,
    compressao: str = COMPRESSAO_PADRAO,
    forcar: bool = False,
) -> ResultadoEscrita:
    """Grava os blocos normalizados como um Parquet tipado.

    Args:
        blocos: os `DataFrame` que saem do `Normalizador`. Só são consumidos se houver trabalho.
        destino: caminho do `.parquet`; a pasta é criada se faltar.
        chave: identidade do insumo (ver `chave_do_insumo`) — a base da idempotência.
        forcar: regrava mesmo com cache válido.
    """
    destino = Path(destino)
    sidecar = destino.with_name(destino.name + SUFIXO_SIDECAR)

    if not forcar and (cache := _em_cache(destino, sidecar, chave)):
        return cache

    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_name(destino.name + _SUFIXO_PARCIAL)
    alvo = esquema_arrow(esquema)
    linhas = 0

    try:
        with pq.ParquetWriter(parcial, alvo, compression=compressao) as escritor:
            for bloco in blocos:
                escritor.write_table(_como_tabela(bloco, alvo))
                linhas += len(bloco)
    except BaseException:
        parcial.unlink(missing_ok=True)
        raise

    # Só agora o resultado passa a existir: até aqui, o Parquet anterior seguia intacto.
    parcial.replace(destino)
    tamanho = destino.stat().st_size
    _gravar_sidecar(sidecar, chave=chave, linhas=linhas, tamanho=tamanho, compressao=compressao)
    return ResultadoEscrita(
        caminho=destino,
        status=StatusEscrita.GRAVADO,
        linhas=linhas,
        tamanho=tamanho,
        chave=chave,
    )


def _como_tabela(bloco: pd.DataFrame, alvo: pa.Schema) -> pa.Table:
    try:
        return pa.Table.from_pandas(bloco, schema=alvo, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, KeyError) as erro:
        raise EscritaError(f"o bloco não corresponde ao esquema do Parquet: {erro}") from erro


def _em_cache(destino: Path, sidecar: Path, chave: str) -> ResultadoEscrita | None:
    """Cache válido exige sidecar íntegro, mesma chave e Parquet com o tamanho registrado.

    O tamanho é o que pega arquivo mexido à mão ou truncado por queda de disco — casos em que
    reaproveitar seria pior do que refazer.
    """
    registro = _ler_json(sidecar)
    if not registro or registro.get("chave") != chave or not destino.exists():
        return None
    tamanho = int(registro.get("tamanho") or -1)
    if destino.stat().st_size != tamanho:
        return None
    return ResultadoEscrita(
        caminho=destino,
        status=StatusEscrita.EM_CACHE,
        linhas=int(registro.get("linhas") or 0),
        tamanho=tamanho,
        chave=chave,
    )


def _ler_json(caminho: Path) -> dict[str, Any] | None:
    try:
        dados = json.loads(caminho.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return dados if isinstance(dados, dict) else None


def _gravar_sidecar(
    sidecar: Path, *, chave: str, linhas: int, tamanho: int, compressao: str
) -> None:
    sidecar.write_text(
        json.dumps(
            {
                "chave": chave,
                "linhas": linhas,
                "tamanho": tamanho,
                "compressao": compressao,
                "gravado_em": datetime.now(UTC).isoformat(),
                "ferramenta": "samp-dq",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


__all__ = [
    "COMPRESSAO_PADRAO",
    "ResultadoEscrita",
    "StatusEscrita",
    "chave_do_insumo",
    "escrever_parquet",
    "esquema_arrow",
]
