"""Envelope `resultado-{ano}.json` — contrato de docs/02 §6 e pontuação de docs/03."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from samp_dq.artefatos import NOME_RESULTADO, caminho_artefato, escrever_json
from samp_dq.ingest.leitura import MAX_EXEMPLOS
from samp_dq.qualidade.catalogo import DIMENSOES, PESO_SEVERIDADE, SpecRegra, spec

Status = str  # ok | falha | aviso | aviso_defasagem | nao_aplicavel


def _status(severidade: str, violacoes: int, *, defasagem: bool = False) -> Status:
    if violacoes <= 0:
        return "ok"
    if defasagem:
        return "aviso_defasagem"
    return "falha" if severidade == "erro" else ("aviso" if severidade == "aviso" else "ok")


@dataclass(frozen=True, slots=True)
class ResultadoRegra:
    """Uma entrada do array `regras` do envelope."""

    spec: SpecRegra
    linhas_avaliadas: int
    linhas_violacao: int
    exemplos: tuple[dict[str, Any], ...] = ()
    status: Status | None = None
    severidade: str | None = None
    nota: str = ""

    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def dimensao(self) -> str:
        return self.spec.dimensao

    @property
    def severidade_efetiva(self) -> str:
        return self.severidade if self.severidade is not None else self.spec.severidade

    @property
    def status_efetivo(self) -> Status:
        if self.status is not None:
            return self.status
        return _status(self.severidade_efetiva, self.linhas_violacao)

    @property
    def percentual_violacao(self) -> float:
        if self.linhas_avaliadas <= 0:
            return 0.0
        return round(self.linhas_violacao / self.linhas_avaliadas, 6)

    @property
    def aplicavel(self) -> bool:
        return self.status_efetivo != "nao_aplicavel"

    def como_json(self) -> dict[str, Any]:
        dados: dict[str, Any] = {
            "id": self.id,
            "dimensao": self.dimensao,
            "severidade": self.severidade_efetiva,
            "status": self.status_efetivo,
            "linhasAvaliadas": self.linhas_avaliadas,
            "linhasViolacao": self.linhas_violacao,
            "percentualViolacao": self.percentual_violacao,
            "exemplos": list(self.exemplos[:MAX_EXEMPLOS]),
        }
        if self.nota:
            dados["nota"] = self.nota
        return dados


@dataclass(frozen=True, slots=True)
class ResultadoValidacao:
    """O JSON principal lido pelos agentes."""

    arquivo: str
    ano: int | None
    gerado_em: datetime
    linhas_totais: int
    perfil: dict[str, Any]
    regras: tuple[ResultadoRegra, ...]
    origem: dict[str, Any]

    @property
    def execucao_id(self) -> str:
        carimbo = self.gerado_em.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{carimbo}-{Path(self.arquivo).stem}"

    @property
    def scores(self) -> dict[str, float]:
        return {dim: _score_dimensao(self.regras, dim) for dim in DIMENSOES}

    @property
    def score_geral(self) -> float:
        valores = list(self.scores.values())
        return round(sum(valores) / len(valores), 1) if valores else 100.0

    def regra(self, id_regra: str) -> ResultadoRegra:
        return next(r for r in self.regras if r.id == id_regra)

    def como_json(self) -> dict[str, Any]:
        return {
            "execucaoId": self.execucao_id,
            "arquivo": self.arquivo,
            "ano": self.ano,
            "geradoEm": self.gerado_em.isoformat(),
            "linhasTotais": self.linhas_totais,
            "origem": dict(self.origem),
            "perfil": dict(self.perfil),
            "regras": [r.como_json() for r in self.regras],
            "scores": self.scores,
            "scoreGeral": self.score_geral,
        }

    def resumo(self) -> str:
        falhas = sum(1 for r in self.regras if r.status_efetivo == "falha")
        avisos = sum(1 for r in self.regras if r.status_efetivo in {"aviso", "aviso_defasagem"})
        return (
            f"{self.arquivo}: score {self.score_geral}; "
            f"{falhas} regra(s) em falha, {avisos} com aviso"
        )


def _score_dimensao(regras: tuple[ResultadoRegra, ...], dimensao: str) -> float:
    """Média dos scores por regra, para uma regra de arquivo não ser afogada por 1 milhão de linhas.

    Cada regra: `100 * (1 - (violacoes/avaliadas) * peso)`. Info não reduz o score.
    """
    partes = [r for r in regras if r.dimensao == dimensao and r.aplicavel]
    if not partes:
        return 100.0
    notas = []
    for regra in partes:
        peso = PESO_SEVERIDADE[regra.severidade_efetiva]
        fracao = regra.percentual_violacao * peso
        notas.append(100.0 * (1.0 - min(fracao, 1.0)))
    return round(sum(notas) / len(notas), 1)


def fechar_regra(
    id_regra: str,
    avaliadas: int,
    violacoes: int,
    exemplos: list[dict[str, Any]],
    *,
    status: Status | None = None,
    severidade: str | None = None,
    nota: str = "",
) -> ResultadoRegra:
    return ResultadoRegra(
        spec=spec(id_regra),
        linhas_avaliadas=avaliadas,
        linhas_violacao=violacoes,
        exemplos=tuple(exemplos[:MAX_EXEMPLOS]),
        status=status,
        severidade=severidade,
        nota=nota,
    )


def gravar_resultado(resultado: ResultadoValidacao, pasta: Path | str) -> Path:
    """Grava `resultado-{ano}.json` na pasta de saída."""
    return escrever_json(
        caminho_artefato(pasta, NOME_RESULTADO, resultado.ano), resultado.como_json()
    )


def agora() -> datetime:
    return datetime.now(UTC)


__all__ = [
    "ResultadoRegra",
    "ResultadoValidacao",
    "agora",
    "fechar_regra",
    "gravar_resultado",
]
