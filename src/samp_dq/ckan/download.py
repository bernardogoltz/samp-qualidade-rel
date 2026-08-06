"""Download dos arquivos do SAMP com integridade, cache e retomada.

Os arquivos anuais passam de 300 MB, então tudo aqui é em *streaming*: nada é carregado
inteiro em memória. Três garantias sustentam o resto do pipeline:

* **Atomicidade** — grava-se em `{nome}.part` e só se renomeia para o nome final quando o
  arquivo está completo e conferido. Nenhuma etapa posterior enxerga arquivo pela metade.
* **Idempotência** — um sidecar `{nome}.samp-dq.json` guarda ETag, tamanho e SHA-256; reexecutar
  com o recurso inalterado não baixa nada (é a chave de idempotência do pré-processamento).
* **Retomada** — download interrompido continua do ponto em que parou via `Range`, desde que o
  ETag do servidor continue o mesmo.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx

from samp_dq.ckan.client import CkanClient
from samp_dq.ckan.models import Recurso
from samp_dq.errors import DownloadIncompletoError

SUFIXO_SIDECAR = ".samp-dq.json"
SUFIXO_PARCIAL = ".part"
SUFIXO_ESTADO = ".estado.json"
BLOCO = 1024 * 1024
"""Tamanho do bloco de leitura (1 MiB): poucas chamadas de escrita, memória constante."""

Progresso = Callable[[int, int | None], None]
"""Callback `(bytes_gravados, total_esperado_ou_None)`."""


class StatusDownload(StrEnum):
    """Como o arquivo local chegou ao seu estado atual."""

    BAIXADO = "baixado"
    RETOMADO = "retomado"
    EM_CACHE = "em_cache"


@dataclass(frozen=True, slots=True)
class ResultadoDownload:
    """O que aconteceu com um recurso."""

    recurso: Recurso
    caminho: Path
    status: StatusDownload
    tamanho: int
    sha256: str
    bytes_baixados: int = 0
    etag: str | None = None
    avisos: tuple[str, ...] = ()

    @property
    def veio_da_rede(self) -> bool:
        return self.status is not StatusDownload.EM_CACHE


def _ler_json(caminho: Path) -> dict[str, Any] | None:
    try:
        dados = json.loads(caminho.read_text("utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dados if isinstance(dados, dict) else None


def _apagar(*caminhos: Path) -> None:
    for caminho in caminhos:
        caminho.unlink(missing_ok=True)


@dataclass(slots=True)
class _Parcial:
    """Estado de um download interrompido, ao lado do `.part`."""

    arquivo: Path
    estado: Path
    bytes_gravados: int = 0
    etag: str | None = None
    sha256_parcial: str | None = None
    _hash: Any = field(default=None, repr=False)

    @classmethod
    def para(cls, destino: Path) -> _Parcial:
        parcial = cls(
            arquivo=destino.with_name(destino.name + SUFIXO_PARCIAL),
            estado=destino.with_name(destino.name + SUFIXO_PARCIAL + SUFIXO_ESTADO),
        )
        parcial._carregar()
        return parcial

    def _carregar(self) -> None:
        """Só é retomável o parcial que tem estado gravado e bate com o `.part` do disco."""
        estado = _ler_json(self.estado)
        if not estado or not self.arquivo.exists():
            return
        tamanho_disco = self.arquivo.stat().st_size
        if estado.get("bytes") != tamanho_disco or tamanho_disco == 0:
            return
        self.bytes_gravados = tamanho_disco
        self.etag = estado.get("etag")
        self.sha256_parcial = estado.get("sha256_parcial")

    @property
    def retomavel(self) -> bool:
        return self.bytes_gravados > 0

    def descartar(self) -> None:
        _apagar(self.arquivo, self.estado)
        self.bytes_gravados = 0
        self.etag = None
        self.sha256_parcial = None

    def salvar_estado(self, url: str, etag: str | None, digest: str) -> None:
        self.estado.write_text(
            json.dumps(
                {
                    "url": url,
                    "etag": etag,
                    "bytes": self.bytes_gravados,
                    "sha256_parcial": digest,
                },
                ensure_ascii=False,
            ),
            "utf-8",
        )


def _cache_valido(destino: Path, sidecar: Path, recurso: Recurso, etag_atual: str | None) -> bool:
    """O arquivo local corresponde ao recurso publicado?

    Exige sidecar íntegro e arquivo com o tamanho registrado — arquivo mexido à mão ou
    truncado por falha de disco força novo download.
    """
    if not destino.exists():
        return False
    registro = _ler_json(sidecar)
    if not registro:
        return False
    if registro.get("url") != recurso.url:
        return False
    if destino.stat().st_size != registro.get("tamanho"):
        return False

    etag_local = registro.get("etag")
    if etag_atual and etag_local:
        return bool(etag_atual == etag_local)

    # Sem ETag dos dois lados, cai-se no metadado do CKAN.
    modificacao_ckan = recurso.ultima_modificacao
    if modificacao_ckan and registro.get("ultima_modificacao_ckan"):
        return bool(registro["ultima_modificacao_ckan"] == modificacao_ckan.isoformat())
    return recurso.tamanho is None or recurso.tamanho == registro.get("tamanho")


def _resultado_em_cache(destino: Path, sidecar: Path, recurso: Recurso) -> ResultadoDownload:
    registro = _ler_json(sidecar) or {}
    return ResultadoDownload(
        recurso=recurso,
        caminho=destino,
        status=StatusDownload.EM_CACHE,
        tamanho=destino.stat().st_size,
        sha256=str(registro.get("sha256") or ""),
        bytes_baixados=0,
        etag=registro.get("etag"),
    )


def _total_esperado(resposta: httpx.Response, ja_gravados: int) -> int | None:
    """Tamanho final do arquivo, somando o que já estava em disco numa resposta parcial."""
    bruto = resposta.headers.get("content-length")
    if bruto is None:
        return None
    try:
        anunciado = int(bruto)
    except ValueError:
        return None
    return anunciado + ja_gravados if resposta.status_code == 206 else anunciado


def baixar_recurso(
    cliente: CkanClient,
    recurso: Recurso,
    destino: str | Path,
    *,
    forcar: bool = False,
    retomar: bool = True,
    progresso: Progresso | None = None,
) -> ResultadoDownload:
    """Baixa um recurso para a pasta `destino`, reaproveitando o que já estiver lá.

    Args:
        cliente: cliente CKAN (a sessão HTTP e a política de retentativa vêm dele).
        recurso: recurso obtido de `Dataset.recurso()` / `Dataset.filtrar()`.
        destino: pasta de destino; criada se não existir.
        forcar: ignora o cache e baixa de novo.
        retomar: continua um `.part` compatível em vez de recomeçar.
        progresso: chamado a cada bloco com `(bytes_gravados, total_esperado)`.

    Raises:
        DownloadIncompletoError: o corpo recebido é menor que o `Content-Length` anunciado.
        CkanHTTPError: o portal respondeu com erro depois de esgotadas as tentativas.
    """
    pasta = Path(destino)
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / recurso.nome_arquivo
    sidecar = arquivo.with_name(arquivo.name + SUFIXO_SIDECAR)
    avisos: list[str] = []

    if not forcar and _cache_valido(arquivo, sidecar, recurso, etag_atual=None):
        return _resultado_em_cache(arquivo, sidecar, recurso)

    parcial = _Parcial.para(arquivo)
    if forcar or not retomar:
        parcial.descartar()

    cabecalhos = {}
    if parcial.retomavel:
        cabecalhos["Range"] = f"bytes={parcial.bytes_gravados}-"
        if parcial.etag:
            cabecalhos["If-Range"] = parcial.etag

    resposta = cliente.requisitar("GET", recurso.url, headers=cabecalhos or None, stream=True)
    try:
        etag = resposta.headers.get("etag")

        # O servidor só honra a retomada respondendo 206; um 200 significa "comece de novo"
        # (arquivo mudou ou o servidor ignora Range). Nesse caso o parcial vai fora.
        retomando = resposta.status_code == 206 and parcial.retomavel
        if not retomando and parcial.retomavel:
            parcial.descartar()
        elif retomando and parcial.etag and etag and parcial.etag != etag:
            # Defesa extra: servidor devolveu 206 mesmo com o arquivo trocado.
            parcial.descartar()
            retomando = False
            resposta.close()
            resposta = cliente.requisitar("GET", recurso.url, stream=True)
            etag = resposta.headers.get("etag")

        total = _total_esperado(resposta, parcial.bytes_gravados if retomando else 0)
        digest = hashlib.sha256()
        gravados = 0
        inicio = parcial.bytes_gravados if retomando else 0

        if retomando:
            # Rehashear o pedaço já em disco é mais barato que rebaixar centenas de MB.
            with parcial.arquivo.open("rb") as anterior:
                for bloco in iter(lambda: anterior.read(BLOCO), b""):
                    digest.update(bloco)
            gravados = parcial.bytes_gravados

        modo = "ab" if retomando else "wb"
        with parcial.arquivo.open(modo) as saida:
            for bloco in resposta.iter_bytes(BLOCO):
                saida.write(bloco)
                digest.update(bloco)
                gravados += len(bloco)
                parcial.bytes_gravados = gravados
                if progresso is not None:
                    progresso(gravados, total)
    finally:
        resposta.close()

    baixados = gravados - inicio

    if total is not None and gravados != total:
        parcial.salvar_estado(recurso.url, etag, digest.hexdigest())
        raise DownloadIncompletoError(recurso.url, total, gravados)

    if recurso.tamanho is not None and recurso.tamanho != gravados:
        avisos.append(
            f"o CKAN anuncia {recurso.tamanho} bytes para {recurso.nome}, mas o servidor "
            f"entregou {gravados}; o metadado do portal pode estar defasado"
        )

    sha = digest.hexdigest()
    parcial.arquivo.replace(arquivo)
    _apagar(parcial.estado)

    sidecar.write_text(
        json.dumps(
            {
                "recurso_id": recurso.id,
                "nome": recurso.nome,
                "url": recurso.url,
                "formato": str(recurso.formato),
                "etag": etag,
                "tamanho": gravados,
                "sha256": sha,
                "ultima_modificacao_http": resposta.headers.get("last-modified"),
                "ultima_modificacao_ckan": (
                    recurso.ultima_modificacao.isoformat() if recurso.ultima_modificacao else None
                ),
                "baixado_em": datetime.now(UTC).isoformat(),
                "ferramenta": "samp-dq",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "utf-8",
    )

    return ResultadoDownload(
        recurso=recurso,
        caminho=arquivo,
        status=StatusDownload.RETOMADO if retomando else StatusDownload.BAIXADO,
        tamanho=gravados,
        sha256=sha,
        bytes_baixados=baixados,
        etag=etag,
        avisos=tuple(avisos),
    )


def baixar_recursos(
    cliente: CkanClient,
    recursos: Iterable[Recurso],
    destino: str | Path,
    *,
    forcar: bool = False,
    retomar: bool = True,
    progresso: Progresso | None = None,
) -> list[ResultadoDownload]:
    """Baixa vários recursos em sequência, na ordem em que vieram."""
    return [
        baixar_recurso(
            cliente, recurso, destino, forcar=forcar, retomar=retomar, progresso=progresso
        )
        for recurso in recursos
    ]
