"""Cliente HTTP da API CKAN: contrato, erros e política de retentativa.

Nenhum teste aqui toca a rede — as respostas vêm de `httpx.MockTransport`.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from samp_dq.ckan import (
    CkanClient,
    CkanHTTPError,
    CkanRespostaInvalidaError,
    Dataset,
)
from samp_dq.ckan.client import DATASET_SAMP, PORTAL_ANEEL


def cliente_com(handler: Any, **kwargs: Any) -> CkanClient:
    return CkanClient(transport=httpx.MockTransport(handler), pausa=lambda _: None, **kwargs)


class TestPackageShow:
    def test_devolve_o_dataset_analisado(self, transporte_ok: httpx.MockTransport) -> None:
        with CkanClient(transport=transporte_ok) as cliente:
            dataset = cliente.package_show()

        assert isinstance(dataset, Dataset)
        assert dataset.nome == "samp"
        assert dataset.anos_disponiveis() == [2003, 2024, 2026]

    def test_consulta_o_dataset_samp_por_padrao(self, package_show_json: dict[str, Any]) -> None:
        pedidos: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            pedidos.append(request)
            return httpx.Response(200, json=package_show_json)

        with cliente_com(handler) as cliente:
            cliente.package_show()

        (pedido,) = pedidos
        assert pedido.url.path == "/api/3/action/package_show"
        assert pedido.url.params["id"] == DATASET_SAMP
        assert str(pedido.url).startswith(PORTAL_ANEEL)

    def test_aceita_outro_dataset(self, package_show_json: dict[str, Any]) -> None:
        pedidos: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            pedidos.append(request)
            return httpx.Response(200, json=package_show_json)

        with cliente_com(handler) as cliente:
            cliente.package_show("samp-balanco")

        assert pedidos[0].url.params["id"] == "samp-balanco"

    def test_manda_user_agent_de_navegador(self, package_show_json: dict[str, Any]) -> None:
        # O portal devolve 403 para alguns user-agents de biblioteca.
        pedidos: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            pedidos.append(request)
            return httpx.Response(200, json=package_show_json)

        with cliente_com(handler) as cliente:
            cliente.package_show()

        ua = pedidos[0].headers["user-agent"]
        assert "Mozilla" in ua
        assert "samp-dq" in ua  # identifica-se com honestidade


class TestErros:
    def test_status_de_erro_definitivo_vira_excecao(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="not found")

        with cliente_com(handler) as cliente, pytest.raises(CkanHTTPError) as erro:
            cliente.package_show("inexistente")

        assert erro.value.status == 404

    def test_resposta_que_nao_e_json_vira_excecao_clara(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="<html>manutenção</html>")

        with cliente_com(handler) as cliente, pytest.raises(CkanRespostaInvalidaError):
            cliente.package_show()

    def test_success_false_vira_excecao(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": False, "error": {"message": "sem acesso"}})

        with cliente_com(handler) as cliente, pytest.raises(CkanRespostaInvalidaError) as erro:
            cliente.package_show()

        assert "sem acesso" in str(erro.value)

    def test_json_sem_result_vira_excecao(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True})

        with cliente_com(handler) as cliente, pytest.raises(CkanRespostaInvalidaError):
            cliente.package_show()


class TestRetentativa:
    def test_repete_em_erro_de_servidor_e_devolve_o_sucesso(
        self, package_show_json: dict[str, Any]
    ) -> None:
        tentativas = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal tentativas
            tentativas += 1
            if tentativas < 3:
                return httpx.Response(503, text="indisponível")
            return httpx.Response(200, json=package_show_json)

        with cliente_com(handler, tentativas=3) as cliente:
            dataset = cliente.package_show()

        assert tentativas == 3
        assert dataset.nome == "samp"

    def test_repete_em_erro_de_rede(self, package_show_json: dict[str, Any]) -> None:
        tentativas = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal tentativas
            tentativas += 1
            if tentativas == 1:
                raise httpx.ConnectError("conexão recusada", request=request)
            return httpx.Response(200, json=package_show_json)

        with cliente_com(handler, tentativas=3) as cliente:
            cliente.package_show()

        assert tentativas == 2

    def test_nao_repete_em_erro_do_cliente(self) -> None:
        tentativas = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal tentativas
            tentativas += 1
            return httpx.Response(404, text="not found")

        with cliente_com(handler, tentativas=3) as cliente, pytest.raises(CkanHTTPError):
            cliente.package_show()

        assert tentativas == 1

    def test_desiste_apos_o_limite_de_tentativas(self) -> None:
        tentativas = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal tentativas
            tentativas += 1
            return httpx.Response(500, text="boom")

        with cliente_com(handler, tentativas=2) as cliente, pytest.raises(CkanHTTPError):
            cliente.package_show()

        assert tentativas == 2

    def test_backoff_exponencial_entre_tentativas(self) -> None:
        pausas: list[float] = []

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500)

        cliente = CkanClient(
            transport=httpx.MockTransport(handler),
            pausa=pausas.append,
            tentativas=4,
            espera_inicial=0.5,
        )
        with cliente, pytest.raises(CkanHTTPError):
            cliente.package_show()

        assert pausas == [0.5, 1.0, 2.0]  # três esperas para quatro tentativas

    def test_respeita_retry_after_do_servidor(self) -> None:
        pausas: list[float] = []
        tentativas = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal tentativas
            tentativas += 1
            if tentativas == 1:
                return httpx.Response(429, headers={"Retry-After": "7"})
            return httpx.Response(500)

        cliente = CkanClient(
            transport=httpx.MockTransport(handler),
            pausa=pausas.append,
            tentativas=2,
            espera_inicial=0.5,
        )
        with cliente, pytest.raises(CkanHTTPError):
            cliente.package_show()

        assert pausas == [7.0]


class TestCicloDeVida:
    def test_fecha_a_sessao_ao_sair_do_contexto(self, transporte_ok: httpx.MockTransport) -> None:
        cliente = CkanClient(transport=transporte_ok)
        with cliente:
            cliente.package_show()

        assert cliente.esta_fechado

    def test_reaproveita_uma_sessao_httpx_informada(
        self, package_show_json: dict[str, Any]
    ) -> None:
        sessao = httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=package_show_json)),
            base_url="https://exemplo.test",
        )
        cliente = CkanClient(sessao=sessao)

        assert cliente.package_show().nome == "samp"
        # Sessão emprestada não é fechada pelo cliente.
        cliente.fechar()
        assert not sessao.is_closed
        sessao.close()
