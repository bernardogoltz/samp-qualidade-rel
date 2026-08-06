"""Download dos arquivos do SAMP: streaming, integridade, cache e retomada."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from pathlib import Path

import httpx
import pytest

from samp_dq.ckan import CkanClient, Formato, Recurso
from samp_dq.ckan.download import ResultadoDownload, StatusDownload, baixar_recurso
from samp_dq.errors import CkanHTTPError, DownloadIncompletoError

CONTEUDO = b'"DatGeracaoConjuntoDados";"VlrMercado"\n"2026-01-01";"783,990000"\n' * 50
ETAG = '"abc-123"'
ULTIMA_MOD = "Tue, 28 Jul 2026 14:45:33 GMT"


def recurso_csv(tamanho: int | None = len(CONTEUDO)) -> Recurso:
    return Recurso(
        id="r1",
        nome="samp-2024.csv",
        url="https://portal.test/download/samp-2024.csv",
        formato=Formato.CSV,
        tamanho=tamanho,
    )


def servidor(
    conteudo: bytes = CONTEUDO,
    *,
    etag: str | None = ETAG,
    aceita_range: bool = True,
    registro: list[httpx.Request] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """Handler que serve `conteudo`, honrando `Range` como um servidor HTTP real."""

    def handler(request: httpx.Request) -> httpx.Response:
        if registro is not None:
            registro.append(request)
        cabecalhos = {"Last-Modified": ULTIMA_MOD}
        if etag:
            cabecalhos["ETag"] = etag
        if aceita_range:
            cabecalhos["Accept-Ranges"] = "bytes"

        faixa = request.headers.get("range")
        # `If-Range` que não bate com o ETag atual obriga o servidor a mandar o arquivo
        # inteiro (RFC 9110 §13.1.5) — é assim que o cliente descobre que o recurso mudou.
        if_range = request.headers.get("if-range")
        if if_range and etag and if_range != etag:
            faixa = None
        if faixa and aceita_range:
            inicio = int(faixa.removeprefix("bytes=").split("-")[0])
            parcial = conteudo[inicio:]
            cabecalhos["Content-Range"] = f"bytes {inicio}-{len(conteudo) - 1}/{len(conteudo)}"
            cabecalhos["Content-Length"] = str(len(parcial))
            return httpx.Response(206, content=parcial, headers=cabecalhos)

        cabecalhos["Content-Length"] = str(len(conteudo))
        return httpx.Response(200, content=conteudo, headers=cabecalhos)

    return handler


@pytest.fixture
def cliente_fabrica() -> Iterator[Callable[..., CkanClient]]:
    criados: list[CkanClient] = []

    def criar(handler: Callable[[httpx.Request], httpx.Response], **kwargs: object) -> CkanClient:
        cliente = CkanClient(
            transport=httpx.MockTransport(handler),
            pausa=lambda _: None,
            **kwargs,  # type: ignore[arg-type]
        )
        criados.append(cliente)
        return cliente

    yield criar
    for c in criados:
        c.fechar()


class TestDownloadBasico:
    def test_grava_o_arquivo_com_o_conteudo_do_servidor(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        cliente = cliente_fabrica(servidor())

        resultado = baixar_recurso(cliente, recurso_csv(), tmp_path)

        assert resultado.caminho == tmp_path / "samp-2024.csv"
        assert resultado.caminho.read_bytes() == CONTEUDO
        assert resultado.status is StatusDownload.BAIXADO
        assert resultado.bytes_baixados == len(CONTEUDO)

    def test_cria_a_pasta_de_destino(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        destino = tmp_path / "bruto" / "2024"

        resultado = baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), destino)

        assert resultado.caminho.parent == destino

    def test_calcula_o_sha256_do_arquivo(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        resultado = baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        assert resultado.sha256 == hashlib.sha256(CONTEUDO).hexdigest()

    def test_nao_deixa_arquivo_parcial_visivel_durante_o_download(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        vistos: list[list[str]] = []

        def espiando(pos: int, total: int | None) -> None:
            vistos.append(sorted(p.name for p in tmp_path.iterdir()))

        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path, progresso=espiando)

        # Enquanto baixa, só existe o `.part`: o nome final só aparece completo.
        assert all("samp-2024.csv" not in nomes for nomes in vistos)
        assert any("samp-2024.csv.part" in nomes for nomes in vistos)

    def test_relata_progresso(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        eventos: list[tuple[int, int | None]] = []

        baixar_recurso(
            cliente_fabrica(servidor()),
            recurso_csv(),
            tmp_path,
            progresso=lambda pos, total: eventos.append((pos, total)),
        )

        assert eventos, "o callback de progresso deveria ter sido chamado"
        assert eventos[-1] == (len(CONTEUDO), len(CONTEUDO))
        assert [p for p, _ in eventos] == sorted(p for p, _ in eventos)

    def test_remove_o_parcial_quando_o_download_falha(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, headers={"Content-Length": "3"}, content=b"nao")

        with pytest.raises(CkanHTTPError):
            baixar_recurso(cliente_fabrica(handler), recurso_csv(), tmp_path)

        assert list(tmp_path.iterdir()) == []


class TestIntegridade:
    def test_tamanho_menor_que_o_anunciado_e_erro(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            # Anuncia mais do que entrega (conexão cortada no meio).
            return httpx.Response(
                200,
                content=CONTEUDO[:100],
                headers={"Content-Length": str(len(CONTEUDO))},
            )

        with pytest.raises(DownloadIncompletoError) as erro:
            baixar_recurso(cliente_fabrica(handler), recurso_csv(), tmp_path)

        assert erro.value.esperado == len(CONTEUDO)
        assert erro.value.obtido == 100
        assert not (tmp_path / "samp-2024.csv").exists()

    def test_divergencia_com_o_tamanho_do_ckan_nao_bloqueia_mas_avisa(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        # O metadado do CKAN às vezes fica defasado; quem manda é o Content-Length.
        resultado = baixar_recurso(
            cliente_fabrica(servidor()), recurso_csv(tamanho=999_999), tmp_path
        )

        assert resultado.status is StatusDownload.BAIXADO
        assert any("999999" in aviso or "999.999" in aviso for aviso in resultado.avisos)


class TestCache:
    def test_segundo_download_do_mesmo_recurso_usa_o_cache(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        pedidos: list[httpx.Request] = []
        cliente = cliente_fabrica(servidor(registro=pedidos))

        primeiro = baixar_recurso(cliente, recurso_csv(), tmp_path)
        segundo = baixar_recurso(cliente, recurso_csv(), tmp_path)

        assert primeiro.status is StatusDownload.BAIXADO
        assert segundo.status is StatusDownload.EM_CACHE
        assert segundo.sha256 == primeiro.sha256
        assert len(pedidos) == 1, "o cache não deveria fazer uma segunda requisição"

    def test_grava_sidecar_de_metadados(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        sidecar = json.loads((tmp_path / "samp-2024.csv.samp-dq.json").read_text("utf-8"))
        assert sidecar["etag"] == ETAG
        assert sidecar["tamanho"] == len(CONTEUDO)
        assert sidecar["sha256"] == hashlib.sha256(CONTEUDO).hexdigest()
        assert sidecar["url"] == recurso_csv().url
        assert sidecar["recurso_id"] == "r1"
        assert sidecar["baixado_em"]

    def test_etag_diferente_rebaixa_o_arquivo(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        novo = CONTEUDO + b'"2026-02-01";"1,000000"\n'
        resultado = baixar_recurso(
            cliente_fabrica(servidor(novo, etag='"def-456"')),
            recurso_csv(tamanho=len(novo)),
            tmp_path,
        )

        assert resultado.status is StatusDownload.BAIXADO
        assert resultado.caminho.read_bytes() == novo

    def test_arquivo_alterado_no_disco_invalida_o_cache(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)
        (tmp_path / "samp-2024.csv").write_bytes(b"truncado")

        resultado = baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        assert resultado.status is StatusDownload.BAIXADO
        assert resultado.caminho.read_bytes() == CONTEUDO

    def test_sidecar_ausente_invalida_o_cache(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)
        (tmp_path / "samp-2024.csv.samp-dq.json").unlink()

        resultado = baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        assert resultado.status is StatusDownload.BAIXADO

    def test_forcar_ignora_o_cache(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        resultado = baixar_recurso(
            cliente_fabrica(servidor()), recurso_csv(), tmp_path, forcar=True
        )

        assert resultado.status is StatusDownload.BAIXADO


class TestRetomada:
    def _parcial(self, tmp_path: Path, bytes_ja_baixados: int, etag: str | None = ETAG) -> None:
        (tmp_path / "samp-2024.csv.part").write_bytes(CONTEUDO[:bytes_ja_baixados])
        estado = {"etag": etag, "url": recurso_csv().url, "bytes": bytes_ja_baixados}
        (tmp_path / "samp-2024.csv.part.estado.json").write_text(json.dumps(estado), "utf-8")

    def test_retoma_de_onde_parou(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        self._parcial(tmp_path, 200)
        pedidos: list[httpx.Request] = []

        resultado = baixar_recurso(
            cliente_fabrica(servidor(registro=pedidos)), recurso_csv(), tmp_path
        )

        assert pedidos[0].headers["range"] == "bytes=200-"
        assert resultado.status is StatusDownload.RETOMADO
        assert resultado.caminho.read_bytes() == CONTEUDO
        assert resultado.sha256 == hashlib.sha256(CONTEUDO).hexdigest()
        assert resultado.bytes_baixados == len(CONTEUDO) - 200

    def test_recomeca_do_zero_se_o_arquivo_mudou_no_servidor(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        self._parcial(tmp_path, 200, etag='"antigo"')
        pedidos: list[httpx.Request] = []

        resultado = baixar_recurso(
            cliente_fabrica(servidor(registro=pedidos)), recurso_csv(), tmp_path
        )

        # Pergunta com If-Range; o servidor responde 200 e o parcial obsoleto é descartado.
        assert pedidos[0].headers["if-range"] == '"antigo"'
        assert resultado.status is StatusDownload.BAIXADO
        assert resultado.caminho.read_bytes() == CONTEUDO

    def test_servidor_que_devolve_206_com_etag_trocado_nao_concatena_lixo(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        # Servidor mal-comportado: ignora o If-Range e devolve 206 mesmo com o arquivo trocado.
        self._parcial(tmp_path, 200, etag='"antigo"')

        def handler(request: httpx.Request) -> httpx.Response:
            cabecalhos = {"ETag": '"novo"', "Accept-Ranges": "bytes"}
            faixa = request.headers.get("range")
            if faixa:
                inicio = int(faixa.removeprefix("bytes=").split("-")[0])
                parcial = CONTEUDO[inicio:]
                cabecalhos["Content-Length"] = str(len(parcial))
                cabecalhos["Content-Range"] = f"bytes {inicio}-{len(CONTEUDO) - 1}/{len(CONTEUDO)}"
                return httpx.Response(206, content=parcial, headers=cabecalhos)
            cabecalhos["Content-Length"] = str(len(CONTEUDO))
            return httpx.Response(200, content=CONTEUDO, headers=cabecalhos)

        resultado = baixar_recurso(cliente_fabrica(handler), recurso_csv(), tmp_path)

        assert resultado.status is StatusDownload.BAIXADO
        assert resultado.caminho.read_bytes() == CONTEUDO
        assert resultado.sha256 == hashlib.sha256(CONTEUDO).hexdigest()

    def test_servidor_que_ignora_range_ainda_gera_arquivo_correto(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        self._parcial(tmp_path, 200)

        # Responde 200 com o arquivo inteiro apesar do `Range` — o parcial precisa ser
        # descartado, e não concatenado.
        resultado = baixar_recurso(
            cliente_fabrica(servidor(aceita_range=False)), recurso_csv(), tmp_path
        )

        assert resultado.caminho.read_bytes() == CONTEUDO

    def test_parcial_sem_estado_e_descartado(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        (tmp_path / "samp-2024.csv.part").write_bytes(b"lixo de execucao antiga")
        pedidos: list[httpx.Request] = []

        resultado = baixar_recurso(
            cliente_fabrica(servidor(registro=pedidos)), recurso_csv(), tmp_path
        )

        assert "range" not in pedidos[0].headers
        assert resultado.caminho.read_bytes() == CONTEUDO

    def test_limpa_os_arquivos_temporarios_ao_terminar(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        self._parcial(tmp_path, 200)

        baixar_recurso(cliente_fabrica(servidor()), recurso_csv(), tmp_path)

        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "samp-2024.csv",
            "samp-2024.csv.samp-dq.json",
        ]


class TestBaixarVarios:
    def test_baixa_uma_lista_de_recursos(
        self, cliente_fabrica: Callable[..., CkanClient], tmp_path: Path
    ) -> None:
        from samp_dq.ckan.download import baixar_recursos

        recursos = [
            recurso_csv(),
            Recurso(
                id="r2",
                nome="samp-2023.csv",
                url="https://portal.test/download/samp-2023.csv",
                formato=Formato.CSV,
                tamanho=len(CONTEUDO),
            ),
        ]

        resultados = baixar_recursos(cliente_fabrica(servidor()), recursos, tmp_path)

        assert [r.recurso.nome for r in resultados] == ["samp-2024.csv", "samp-2023.csv"]
        assert all(isinstance(r, ResultadoDownload) for r in resultados)
        assert (tmp_path / "samp-2023.csv").read_bytes() == CONTEUDO
