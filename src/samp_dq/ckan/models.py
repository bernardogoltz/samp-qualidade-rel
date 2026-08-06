"""Metadados do CKAN traduzidos para objetos de domínio do SAMP.

O portal da ANEEL publica o dataset `samp` com um par de recursos por ano (CSV desde 2003,
Parquet desde 2009) mais o dicionário de dados em PDF. Estas classes são uma leitura
*tolerante* da resposta de `package_show`: campos ausentes ou formatos novos não derrubam o
parsing, porque o dataset é externo e muda sem aviso.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from urllib.parse import unquote, urlparse

from samp_dq.errors import RecursoNaoEncontradoError

# Os arquivos anuais seguem `samp-{ano}.{ext}`; o ano é a chave de trabalho do módulo.
_PADRAO_ANO = re.compile(r"samp[-_](\d{4})\b", re.IGNORECASE)


class Formato(StrEnum):
    """Formatos publicados pelo dataset SAMP."""

    CSV = "CSV"
    PARQUET = "PARQUET"
    PDF = "PDF"
    OUTRO = "OUTRO"

    @classmethod
    def de_texto(cls, valor: Formato | str | None) -> Formato:
        if isinstance(valor, Formato):
            return valor
        try:
            return cls(str(valor or "").strip().upper())
        except ValueError:
            return cls.OUTRO


def _para_datetime(valor: Any) -> datetime | None:
    """Converte um timestamp do CKAN. Vem sem timezone; o CKAN os grava em UTC."""
    if not isinstance(valor, str) or not valor.strip():
        return None
    try:
        momento = datetime.fromisoformat(valor.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return momento.replace(tzinfo=UTC) if momento.tzinfo is None else momento


def _para_int(valor: Any) -> int | None:
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True, slots=True)
class Recurso:
    """Um arquivo publicado no dataset (um CSV anual, um Parquet ou o dicionário)."""

    id: str
    nome: str
    url: str
    formato: Formato
    formato_bruto: str = ""
    tamanho: int | None = None
    ultima_modificacao: datetime | None = None
    descricao: str = ""
    bruto: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def de_ckan(cls, dados: dict[str, Any]) -> Recurso:
        formato_bruto = str(dados.get("format") or "")
        return cls(
            id=str(dados.get("id") or ""),
            nome=str(dados.get("name") or ""),
            url=str(dados.get("url") or ""),
            formato=Formato.de_texto(formato_bruto),
            formato_bruto=formato_bruto,
            tamanho=_para_int(dados.get("size")),
            # `last_modified` é nulo em recurso nunca reeditado; aí vale a data de criação.
            ultima_modificacao=(
                _para_datetime(dados.get("last_modified")) or _para_datetime(dados.get("created"))
            ),
            descricao=str(dados.get("description") or ""),
            bruto=dict(dados),
        )

    @property
    def ano(self) -> int | None:
        """Ano do arquivo anual, ou `None` para recursos que não são anuais (o dicionário)."""
        achado = _PADRAO_ANO.search(self.nome) or _PADRAO_ANO.search(self.url)
        return int(achado.group(1)) if achado else None

    @property
    def nome_arquivo(self) -> str:
        """Nome seguro para gravar em disco.

        O `name` do CKAN é um rótulo humano ("Dicionário de dados"), então só serve como nome
        de arquivo quando já vem com extensão; caso contrário usa-se o final da URL.
        """
        candidato = self.nome.strip()
        if candidato and "/" not in candidato and "." in candidato:
            return candidato
        do_url = unquote(urlparse(self.url).path.rsplit("/", 1)[-1])
        return do_url or (candidato or self.id)


def _anos_pedidos(ano: int | list[int] | tuple[int, ...] | None) -> set[int] | None:
    if ano is None:
        return None
    return {ano} if isinstance(ano, int) else set(ano)


@dataclass(frozen=True, slots=True)
class Dataset:
    """O dataset `samp` do portal, com seus recursos."""

    id: str
    nome: str
    titulo: str = ""
    licenca: str = ""
    licenca_titulo: str = ""
    metadados_modificados: datetime | None = None
    recursos: tuple[Recurso, ...] = ()

    @classmethod
    def de_ckan(cls, dados: dict[str, Any]) -> Dataset:
        recursos = [Recurso.de_ckan(r) for r in dados.get("resources") or []]
        # Ordem estável e previsível: por ano, depois por nome. Recursos sem ano (o
        # dicionário) vão para o fim.
        recursos.sort(key=lambda r: (r.ano is None, r.ano or 0, r.nome))
        return cls(
            id=str(dados.get("id") or ""),
            nome=str(dados.get("name") or ""),
            titulo=str(dados.get("title") or ""),
            licenca=str(dados.get("license_id") or ""),
            licenca_titulo=str(dados.get("license_title") or ""),
            metadados_modificados=_para_datetime(dados.get("metadata_modified")),
            recursos=tuple(recursos),
        )

    def filtrar(
        self,
        *,
        ano: int | list[int] | tuple[int, ...] | None = None,
        formato: Formato | str | None = None,
    ) -> list[Recurso]:
        """Recursos que casam com os filtros, na ordem de publicação (por ano)."""
        anos = _anos_pedidos(ano)
        alvo = None if formato is None else Formato.de_texto(formato)
        return [
            r
            for r in self.recursos
            if (anos is None or r.ano in anos) and (alvo is None or r.formato == alvo)
        ]

    def recurso(self, *, ano: int, formato: Formato | str = Formato.CSV) -> Recurso:
        """O único recurso do ano/formato pedido."""
        achados = self.filtrar(ano=ano, formato=formato)
        if not achados:
            alvo = Formato.de_texto(formato)
            disponiveis = ", ".join(str(a) for a in self.anos_disponiveis()) or "nenhum"
            raise RecursoNaoEncontradoError(
                f"não há recurso {alvo.value} para o ano {ano} no dataset "
                f"'{self.nome}'; anos disponíveis: {disponiveis}"
            )
        return achados[0]

    def anos_disponiveis(self) -> list[int]:
        """Anos com pelo menos um arquivo publicado, em ordem crescente."""
        return sorted({r.ano for r in self.recursos if r.ano is not None})
