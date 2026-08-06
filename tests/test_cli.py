"""A CLI `samp-dq`: listagem e download, sem tocar a rede."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from samp_dq import cli

CONTEUDO = b"a" * 512


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def portal(
    package_show_json: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> Iterator[list[httpx.Request]]:
    """Substitui a fábrica de clientes da CLI por um portal simulado."""
    pedidos: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        if request.url.path.startswith("/api/"):
            return httpx.Response(200, json=package_show_json)
        return httpx.Response(
            200,
            content=CONTEUDO,
            headers={"Content-Length": str(len(CONTEUDO)), "ETag": '"e1"'},
        )

    from samp_dq.ckan import CkanClient

    def fabrica(**kwargs: Any) -> CkanClient:
        return CkanClient(transport=httpx.MockTransport(handler), pausa=lambda _: None)

    monkeypatch.setattr(cli, "criar_cliente", fabrica)
    yield pedidos


Executar = Callable[..., Any]


class TestListar:
    def test_mostra_os_csvs_com_ano_e_tamanho(
        self, runner: CliRunner, portal: list[httpx.Request]
    ) -> None:
        resultado = runner.invoke(cli.app, ["listar"])

        assert resultado.exit_code == 0
        assert "samp-2024.csv" in resultado.stdout
        assert "2024" in resultado.stdout
        # Por padrão só CSV: nada de Parquet nem do dicionário.
        assert "samp-2024.parquet" not in resultado.stdout
        assert "dd-samp.pdf" not in resultado.stdout

    def test_formato_todos_mostra_tudo(
        self, runner: CliRunner, portal: list[httpx.Request]
    ) -> None:
        resultado = runner.invoke(cli.app, ["listar", "--formato", "todos"])

        assert resultado.exit_code == 0
        assert "samp-2024.parquet" in resultado.stdout

    def test_filtra_por_ano(self, runner: CliRunner, portal: list[httpx.Request]) -> None:
        resultado = runner.invoke(cli.app, ["listar", "--ano", "2024"])

        assert resultado.exit_code == 0
        assert "samp-2024.csv" in resultado.stdout
        assert "samp-2003.csv" not in resultado.stdout

    def test_saida_json_e_analisavel(self, runner: CliRunner, portal: list[httpx.Request]) -> None:
        import json

        resultado = runner.invoke(cli.app, ["listar", "--json"])

        dados = json.loads(resultado.stdout)
        assert [r["ano"] for r in dados] == [2003, 2024, 2026]
        assert dados[0]["formato"] == "CSV"


class TestBaixar:
    def test_baixa_o_ano_pedido(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        resultado = runner.invoke(cli.app, ["baixar", "--ano", "2024", "--saida", str(tmp_path)])

        assert resultado.exit_code == 0
        assert (tmp_path / "samp-2024.csv").read_bytes() == CONTEUDO

    def test_baixa_varios_anos(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        resultado = runner.invoke(
            cli.app,
            ["baixar", "--ano", "2003", "--ano", "2024", "--saida", str(tmp_path)],
        )

        assert resultado.exit_code == 0
        assert (tmp_path / "samp-2003.csv").exists()
        assert (tmp_path / "samp-2024.csv").exists()

    def test_todos_baixa_a_serie_completa(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        resultado = runner.invoke(cli.app, ["baixar", "--todos", "--saida", str(tmp_path)])

        assert resultado.exit_code == 0
        baixados = sorted(p.name for p in tmp_path.glob("*.csv"))
        assert baixados == ["samp-2003.csv", "samp-2024.csv", "samp-2026.csv"]

    def test_baixa_parquet_quando_pedido(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        resultado = runner.invoke(
            cli.app,
            ["baixar", "--ano", "2024", "--formato", "parquet", "--saida", str(tmp_path)],
        )

        assert resultado.exit_code == 0
        assert (tmp_path / "samp-2024.parquet").exists()

    def test_sem_ano_e_sem_todos_falha_explicando(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        resultado = runner.invoke(cli.app, ["baixar", "--saida", str(tmp_path)])

        assert resultado.exit_code != 0
        assert "--ano" in resultado.output and "--todos" in resultado.output

    def test_ano_inexistente_falha_com_mensagem_util(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        resultado = runner.invoke(cli.app, ["baixar", "--ano", "1999", "--saida", str(tmp_path)])

        assert resultado.exit_code != 0
        assert "1999" in resultado.output

    def test_segunda_execucao_informa_cache(
        self, runner: CliRunner, portal: list[httpx.Request], tmp_path: Path
    ) -> None:
        runner.invoke(cli.app, ["baixar", "--ano", "2024", "--saida", str(tmp_path)])
        resultado = runner.invoke(cli.app, ["baixar", "--ano", "2024", "--saida", str(tmp_path)])

        assert resultado.exit_code == 0
        assert "cache" in resultado.stdout.lower()

    def test_erro_de_rede_vira_mensagem_e_nao_traceback(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from samp_dq.ckan import CkanClient

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, text="fora do ar")

        monkeypatch.setattr(
            cli,
            "criar_cliente",
            lambda **kw: CkanClient(
                transport=httpx.MockTransport(handler), pausa=lambda _: None, tentativas=1
            ),
        )

        resultado = runner.invoke(cli.app, ["baixar", "--ano", "2024", "--saida", str(tmp_path)])

        assert resultado.exit_code == 1
        assert "503" in resultado.output
        assert "Traceback" not in resultado.output


class TestVersao:
    def test_mostra_a_versao(self, runner: CliRunner) -> None:
        resultado = runner.invoke(cli.app, ["--version"])

        assert resultado.exit_code == 0
        assert "0.1.0" in resultado.stdout
