"""Validação contra o catálogo (etapa 6 de docs/04)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from samp_dq.ingest.leitura import LeitorCsv
from samp_dq.ingest.normalizacao import Normalizador
from samp_dq.ingest.parquet import chave_do_insumo, escrever_parquet
from samp_dq.ingest.schema import ESQUEMA_SAMP
from samp_dq.perfil import Perfilador
from samp_dq.qualidade import gravar_resultado, validar
from samp_dq.qualidade.catalogo import CATALOGO, REGRAS
from samp_dq.qualidade.resultado import ResultadoValidacao

FIXTURE_REAL = Path(__file__).parent / "fixtures" / "samp-real-amostra.csv"

# Registro válido: as regras de domínio/formato passam, para o teste isolar a que importa.
_OK = {
    "DatGeracaoConjuntoDados": "2026-07-28",
    "NumCNPJAgenteDistribuidora": "75805895000130",
    "SigAgenteDistribuidora": "COCEL",
    "NomAgenteDistribuidora": "COCEL",
    "NomTipoMercado": "Sistema Regular",
    "DscModalidadeTarifaria": "Azul",
    "DscSubGrupoTarifario": "A4",
    "DscClasseConsumoMercado": "Industrial",
    "DscSubClasseConsumidor": "Não se aplica",
    "DscDetalheConsumidor": "Não se aplica",
    "IdeNucleoCeg": "0",
    "NumCNPJAgenteAcessante": "75805895000130",
    "NomAgenteAcessante": "Não se aplica",
    "DscPostoTarifario": "Fora ponta",
    "DscOpcaoEnergia": "Cativo",
    "DscDetalheMercado": "Energia TUSD (kWh)",
    "DatCompetencia": "2024-01-01",
    "VlrMercado": "1,000000",
}


def bloco(**colunas: list[str]) -> pd.DataFrame:
    tamanho = len(next(iter(colunas.values()))) if colunas else 1
    dados = {nome: colunas.get(nome, [_OK[nome]] * tamanho) for nome in ESQUEMA_SAMP.nomes}
    return pd.DataFrame(dados, dtype=str)


def _pipeline(blocos: list[pd.DataFrame], destino: Path, *, ano: int = 2024) -> ResultadoValidacao:
    normalizador = Normalizador()
    perfilador = Perfilador()
    escrever_parquet(
        perfilador.perfilar_blocos(normalizador.normalizar_blocos(iter(blocos))),
        destino,
        chave="teste",
        forcar=True,
    )
    perfil = perfilador.perfil(arquivo=destino, ano=ano, normalizacao=normalizador.relatorio)
    return validar(perfil, destino, hoje=date(2026, 8, 18))


@pytest.fixture
def real(tmp_path: Path) -> ResultadoValidacao:
    leitor = LeitorCsv(FIXTURE_REAL)
    normalizador = Normalizador()
    perfilador = Perfilador()
    destino = tmp_path / "samp-2024.parquet"
    escrever_parquet(
        perfilador.perfilar_blocos(normalizador.normalizar_blocos(leitor.blocos())),
        destino,
        chave=chave_do_insumo(FIXTURE_REAL),
    )
    perfil = perfilador.perfil(
        arquivo=FIXTURE_REAL,
        ano=2024,
        leitura=leitor.relatorio,
        normalizacao=normalizador.relatorio,
    )
    return validar(perfil, destino, hoje=date(2026, 8, 18))


class TestCatalogo:
    def test_tem_as_regras_do_docs03(self) -> None:
        assert len(REGRAS) == 40
        assert "DQ-VAL-003" in CATALOGO
        assert CATALOGO["DQ-COM-001"].dimensao == "completude"
        assert CATALOGO["DQ-VAL-001"].dimensao == "conformidade"


class TestCompletude:
    def test_campo_vazio_e_violacao_da_com001(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(DscPostoTarifario=["", "Ponta"], VlrMercado=["1,0", "2,0"])],
            tmp_path / "samp-2024.parquet",
        )
        regra = resultado.regra("DQ-COM-001")
        assert regra.linhas_violacao == 1
        assert regra.status_efetivo == "falha"

    def test_vlr_mercado_nulo_e_com002(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(VlrMercado=["1,0", ""])],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-COM-002").linhas_violacao == 1


class TestConformidade:
    def test_subgrupo_fora_do_dominio(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(DscSubGrupoTarifario=["B5", "A4"], VlrMercado=["1,0", "1,0"])],
            tmp_path / "samp-2024.parquet",
        )
        regra = resultado.regra("DQ-VAL-003")
        assert regra.linhas_violacao == 1
        assert regra.exemplos[0]["valor"] == "B5"

    def test_tipo_mercado_observado_gera_aviso_de_defasagem(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(NomTipoMercado=["Regular"], VlrMercado=["1,0"])],
            tmp_path / "samp-2024.parquet",
        )
        regra = resultado.regra("DQ-VAL-004")
        assert regra.status_efetivo == "aviso_defasagem"
        assert regra.severidade_efetiva == "aviso"

    def test_opcao_energia_em_caixa_alta_e_aviso(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(DscOpcaoEnergia=["CATIVO"], VlrMercado=["1,0"])],
            tmp_path / "samp-2024.parquet",
        )
        regra = resultado.regra("DQ-VAL-009")
        assert regra.linhas_violacao == 1
        assert regra.severidade_efetiva == "aviso"

    def test_cnpj_distribuidora_precisa_de_14_digitos(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(NumCNPJAgenteDistribuidora=["123"], VlrMercado=["1,0"])],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-VAL-010").linhas_violacao == 1


class TestUnicidade:
    def test_linha_idêntica_e_uni001(self, tmp_path: Path) -> None:
        linha = bloco(VlrMercado=["1,5"], DatCompetencia=["2024-01-01"])
        duplicada = pd.concat([linha, linha], ignore_index=True)
        resultado = _pipeline([duplicada], tmp_path / "samp-2024.parquet")
        assert resultado.regra("DQ-UNI-001").linhas_violacao == 1

    def test_cnpj_com_duas_siglas(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [
                bloco(
                    NumCNPJAgenteDistribuidora=["75805895000130", "75805895000130"],
                    SigAgenteDistribuidora=["COCEL", "OUTRA"],
                    VlrMercado=["1,0", "2,0"],
                )
            ],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-UNI-003").linhas_violacao >= 1


class TestConsistencia:
    def test_competencia_de_outro_ano_sem_refaturamento(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [
                bloco(
                    DatCompetencia=["2023-12-01"],
                    NomTipoMercado=["Regular"],
                    VlrMercado=["1,0"],
                )
            ],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-CON-001").linhas_violacao == 1

    def test_refaturamento_pode_sair_do_ano(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [
                bloco(
                    DatCompetencia=["2023-12-01"],
                    NomTipoMercado=["Sistema de Compensação GD I - Refaturamento"],
                    VlrMercado=["1,0"],
                )
            ],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-CON-001").linhas_violacao == 0

    def test_b1_exige_residencial(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [
                bloco(
                    DscSubGrupoTarifario=["B1"],
                    DscClasseConsumoMercado=["Industrial"],
                    VlrMercado=["1,0"],
                )
            ],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-CON-002").linhas_violacao == 1


class TestAcuracia:
    def test_percentual_fora_de_0_100(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [
                bloco(
                    DscDetalheMercado=["Desconto Demanda %"],
                    VlrMercado=["150,0"],
                )
            ],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-ACU-001").linhas_violacao == 1

    def test_negativo_em_detalhe_sem_estorno(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [
                bloco(
                    DscDetalheMercado=["Energia TUSD (kWh)"],
                    NomTipoMercado=["Regular"],
                    VlrMercado=["-10,0"],
                )
            ],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-ACU-002").linhas_violacao == 1

    def test_serie_temporal_sem_historico_nao_se_aplica(self, tmp_path: Path) -> None:
        resultado = _pipeline([bloco(VlrMercado=["1,0"])], tmp_path / "samp-2024.parquet")
        assert resultado.regra("DQ-ACU-004").status_efetivo == "nao_aplicavel"


class TestAtualidade:
    def test_ano_fechado_nao_aplica_atu(self, real: ResultadoValidacao) -> None:
        assert real.regra("DQ-ATU-001").status_efetivo == "nao_aplicavel"
        assert real.regra("DQ-ATU-002").status_efetivo == "nao_aplicavel"


class TestEnvelope:
    def test_contrato_de_docs02(self, real: ResultadoValidacao) -> None:
        dados = real.como_json()
        assert set(dados) >= {
            "execucaoId",
            "arquivo",
            "linhasTotais",
            "perfil",
            "regras",
            "scores",
            "scoreGeral",
        }
        assert len(dados["regras"]) == 40
        ids = [r["id"] for r in dados["regras"]]
        assert ids == [r.id for r in REGRAS]
        assert 0 <= dados["scoreGeral"] <= 100

    def test_exemplos_no_maximo_cinco(self, tmp_path: Path) -> None:
        resultado = _pipeline(
            [bloco(DscSubGrupoTarifario=["B5"] * 9, VlrMercado=["1,0"] * 9)],
            tmp_path / "samp-2024.parquet",
        )
        assert resultado.regra("DQ-VAL-003").linhas_violacao == 9
        assert len(resultado.regra("DQ-VAL-003").exemplos) == 5

    def test_grava_resultado_json(self, real: ResultadoValidacao, tmp_path: Path) -> None:
        caminho = gravar_resultado(real, tmp_path)
        assert caminho.name == "resultado-2024.json"
        assert "DQ-VAL-009" in caminho.read_text(encoding="utf-8")

    def test_amostra_real_marca_cativo_em_caixa_alta(self, real: ResultadoValidacao) -> None:
        assert real.regra("DQ-VAL-009").linhas_violacao == 24
        assert real.linhas_totais == 24
