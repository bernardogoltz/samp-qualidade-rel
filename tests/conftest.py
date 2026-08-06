"""Fixtures compartilhadas: resposta do CKAN e transporte HTTP simulado."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def package_show_json() -> dict[str, Any]:
    """Resposta real (reduzida) de `package_show` para o dataset `samp`."""
    dados: dict[str, Any] = json.loads((FIXTURES / "package_show_samp.json").read_text("utf-8"))
    return dados


@pytest.fixture
def transporte_ok(package_show_json: dict[str, Any]) -> httpx.MockTransport:
    """Transporte que responde `package_show` com a fixture."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=package_show_json)

    return httpx.MockTransport(handler)


@pytest.fixture
def transporte() -> Callable[[Callable[[httpx.Request], httpx.Response]], httpx.MockTransport]:
    """Fábrica de transportes: recebe o handler e devolve o MockTransport."""
    return httpx.MockTransport
