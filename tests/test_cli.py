"""A CLI `samp-dq`: listagem e download, sem tocar a rede."""

from __future__ import annotations

import json as _json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from samp_dq import cli

CONTEUDO = b"a" * 512
FIXTURE_CSV = Path(__file__).parent / "fixtures" / "samp-real-amostra.csv"


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


class TestPerfilar:
    @pytest.fixture
    def csv(self, tmp_path: Path) -> Path:
        """A amostra real com nome de arquivo anual, para a CLI deduzir o ano."""
        destino = tmp_path / "bruto" / "samp-2024.csv"
        destino.parent.mkdir()
        destino.write_bytes(FIXTURE_CSV.read_bytes())
        return destino

    def test_grava_parquet_e_os_dois_jsons(
        self, runner: CliRunner, csv: Path, tmp_path: Path
    ) -> None:
        saida = tmp_path / "preprocessado"
        resultado = runner.invoke(
            cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)]
        )

        assert resultado.exit_code == 0, resultado.output
        assert (saida / "samp-2024.parquet").exists()
        assert (saida / "perfil-2024.json").exists()
        assert (saida / "dominios-observados-2024.json").exists()
        assert (saida / "resultado-2024.json").exists()

    def test_o_perfil_traz_as_contagens_do_arquivo(
        self, runner: CliRunner, csv: Path, tmp_path: Path
    ) -> None:
        saida = tmp_path / "preprocessado"
        runner.invoke(cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)])

        perfil = _json.loads((saida / "perfil-2024.json").read_text(encoding="utf-8"))
        assert perfil["linhasTotais"] == 24
        assert perfil["ano"] == 2024
        assert perfil["normalizacoes"]["encodingConvertido"] == "cp1252 -> utf-8"

    def test_segunda_execucao_informa_cache(
        self, runner: CliRunner, csv: Path, tmp_path: Path
    ) -> None:
        saida = tmp_path / "preprocessado"
        runner.invoke(cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)])
        resultado = runner.invoke(
            cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)]
        )

        assert resultado.exit_code == 0
        assert "cache" in resultado.stdout.lower()

    def test_forcar_refaz_o_perfil(self, runner: CliRunner, csv: Path, tmp_path: Path) -> None:
        saida = tmp_path / "preprocessado"
        runner.invoke(cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)])
        antes = (saida / "perfil-2024.json").read_text(encoding="utf-8")

        resultado = runner.invoke(
            cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida), "--forcar"]
        )

        assert resultado.exit_code == 0
        assert "cache" not in resultado.stdout.lower()
        # Só o carimbo de geração muda; o conteúdo medido é o mesmo insumo.
        assert (saida / "perfil-2024.json").read_text(encoding="utf-8") != antes

    def test_perfila_um_parquet_direto(self, runner: CliRunner, csv: Path, tmp_path: Path) -> None:
        saida = tmp_path / "preprocessado"
        runner.invoke(cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)])

        outra = tmp_path / "auditoria"
        resultado = runner.invoke(
            cli.app,
            ["perfilar", "--entrada", str(saida / "samp-2024.parquet"), "--saida", str(outra)],
        )

        assert resultado.exit_code == 0
        perfil = _json.loads((outra / "perfil-2024.json").read_text(encoding="utf-8"))
        assert perfil["linhasTotais"] == 24
        # Perfil vindo do Parquet não inventa números de uma normalização que não presenciou.
        assert perfil["normalizacoes"] == {}

    def test_entrada_inexistente_falha_com_mensagem(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        resultado = runner.invoke(
            cli.app, ["perfilar", "--entrada", str(tmp_path / "nao-existe.csv")]
        )

        assert resultado.exit_code == 1
        assert "nao-existe.csv" in resultado.output
        assert "Traceback" not in resultado.output

    def test_cabecalho_estranho_vira_erro_limpo(self, runner: CliRunner, tmp_path: Path) -> None:
        estranho = tmp_path / "samp-2024.csv"
        estranho.write_text("a;b;c\n1;2;3\n", encoding="cp1252")

        resultado = runner.invoke(
            cli.app, ["perfilar", "--entrada", str(estranho), "--saida", str(tmp_path / "saida")]
        )

        assert resultado.exit_code == 1
        assert "cabeçalho" in resultado.output.lower()
        assert "Traceback" not in resultado.output


class TestValidar:
    @pytest.fixture
    def csv(self, tmp_path: Path) -> Path:
        destino = tmp_path / "bruto" / "samp-2024.csv"
        destino.parent.mkdir()
        destino.write_bytes(FIXTURE_CSV.read_bytes())
        return destino

    def test_grava_resultado_a_partir_do_parquet(
        self, runner: CliRunner, csv: Path, tmp_path: Path
    ) -> None:
        saida = tmp_path / "preprocessado"
        runner.invoke(cli.app, ["perfilar", "--entrada", str(csv), "--saida", str(saida)])
        (saida / "resultado-2024.json").unlink()

        resultado = runner.invoke(
            cli.app,
            ["validar", "--entrada", str(saida / "samp-2024.parquet"), "--saida", str(saida)],
        )

        assert resultado.exit_code == 0, resultado.output
        assert (saida / "resultado-2024.json").exists()
        assert "score" in resultado.stdout.lower()


class TestVersao:
    def test_mostra_a_versao(self, runner: CliRunner) -> None:
        resultado = runner.invoke(cli.app, ["--version"])

        assert resultado.exit_code == 0
        assert "0.1.0" in resultado.stdout
