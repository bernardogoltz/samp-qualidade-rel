"""Acesso ao dataset SAMP no portal de dados abertos da ANEEL (API CKAN)."""

from samp_dq.ckan.client import DATASET_SAMP, PORTAL_ANEEL, CkanClient
from samp_dq.ckan.models import Dataset, Formato, Recurso
from samp_dq.errors import (
    CkanError,
    CkanHTTPError,
    CkanRespostaInvalidaError,
    RecursoNaoEncontradoError,
)

__all__ = [
    "DATASET_SAMP",
    "PORTAL_ANEEL",
    "CkanClient",
    "CkanError",
    "CkanHTTPError",
    "CkanRespostaInvalidaError",
    "Dataset",
    "Formato",
    "Recurso",
    "RecursoNaoEncontradoError",
]
