"""Testes de contrato contra o portal real da ANEEL.

Ficam fora da suíte padrão (`-m rede` para rodar) porque dependem da internet e do portal no ar.
Servem para detectar mudanças na fonte: campo renomeado, formato retirado, 403 por user-agent.
Baixam poucos KB — nunca um arquivo anual inteiro.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from samp_dq.ckan import CkanClient, Formato

pytestmark = pytest.mark.rede

CABECALHO_ESPERADO = [
    "DatGeracaoConjuntoDados",
    "NumCNPJAgenteDistribuidora",
    "SigAgenteDistribuidora",
    "NomAgenteDistribuidora",
    "NomTipoMercado",
    "DscModalidadeTarifaria",
    "DscSubGrupoTarifario",
    "DscClasseConsumoMercado",
    "DscSubClasseConsumidor",
    "DscDetalheConsumidor",
    "IdeNucleoCeg",
    "NumCNPJAgenteAcessante",
    "NomAgenteAcessante",
    "DscPostoTarifario",
    "DscOpcaoEnergia",
    "DscDetalheMercado",
    "DatCompetencia",
    "VlrMercado",
]
"""Os 18 campos que o arquivo real publica (o dicionário v1.1 lista 19, com `DscClassificacao`,
que não aparece no CSV; e grafa `IdeNucleoCEG`, enquanto o arquivo usa `IdeNucleoCeg`)."""


@pytest.fixture(scope="module")
def cliente() -> CkanClient:
    with CkanClient() as c:
        yield c


def test_dataset_samp_continua_publicado(cliente: CkanClient) -> None:
    dataset = cliente.package_show()

    assert dataset.nome == "samp"
    assert dataset.licenca == "odc-odbl"


def test_a_serie_anual_de_csv_esta_completa(cliente: CkanClient) -> None:
    dataset = cliente.package_show()
    anos = [r.ano for r in dataset.filtrar(formato=Formato.CSV)]

    assert anos[0] == 2003
    assert anos == list(range(2003, anos[-1] + 1)), "há buracos na série anual de CSV"
    assert anos[-1] >= datetime.now(UTC).year - 1


def test_o_cabecalho_do_csv_nao_mudou(cliente: CkanClient) -> None:
    """Baixa só o começo do arquivo: mudança de layout quebra todo o pré-processamento."""
    dataset = cliente.package_show()
    recurso = dataset.recurso(ano=2024, formato=Formato.CSV)

    resposta = cliente.requisitar(
        "GET", recurso.url, headers={"Range": "bytes=0-2000"}, stream=True
    )
    try:
        inicio = resposta.read()
    finally:
        resposta.close()

    # O arquivo é Latin-1, separado por `;`, com campos entre aspas.
    cabecalho = inicio.decode("latin-1").splitlines()[0]
    campos = [c.strip('"') for c in cabecalho.split(";")]

    assert campos == CABECALHO_ESPERADO


def test_baixa_um_recurso_pequeno_de_ponta_a_ponta(cliente: CkanClient, tmp_path: Path) -> None:
    """O dicionário de dados (~200 KB) exercita o caminho completo sem baixar centenas de MB."""
    from samp_dq.ckan.download import StatusDownload, baixar_recurso

    dataset = cliente.package_show()
    pdf = dataset.filtrar(formato=Formato.PDF)[0]

    primeiro = baixar_recurso(cliente, pdf, tmp_path)
    segundo = baixar_recurso(cliente, pdf, tmp_path)

    assert primeiro.status is StatusDownload.BAIXADO
    assert primeiro.caminho.read_bytes()[:4] == b"%PDF"
    assert primeiro.tamanho > 0
    assert segundo.status is StatusDownload.EM_CACHE
    assert segundo.sha256 == primeiro.sha256
