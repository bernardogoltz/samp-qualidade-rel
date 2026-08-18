"""Validação contra o catálogo de regras de qualidade (etapa 6 de docs/04)."""

from samp_dq.qualidade.motor import validar
from samp_dq.qualidade.resultado import ResultadoValidacao, gravar_resultado

__all__ = ["ResultadoValidacao", "gravar_resultado", "validar"]
