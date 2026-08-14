"""Perfilamento (etapa 5 de docs/04): o que o perfil mede e o que ele se recusa a julgar."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from samp_dq.artefatos import escrever_json, ler_json
from samp_dq.ingest.leitura import LeitorCsv
from samp_dq.ingest.normalizacao import Normalizador
from samp_dq.ingest.parquet import chave_do_insumo, escrever_parquet
from samp_dq.ingest.schema import ESQUEMA_SAMP
from samp_dq.perfil import (
    Amostra,
    ContagemDeValores,
    Origem,
    Perfil,
    Perfilador,
    ano_do_arquivo,
    gravar_dominios_observados,
    gravar_perfil,
    perfilar_parquet,
)

FIXTURE_REAL = Path(__file__).parent / "fixtures" / "samp-real-amostra.csv"


def bloco(**colunas: list[str]) -> pd.DataFrame:
    """Um bloco cru (18 colunas de texto), como a leitura entrega."""
    tamanho = len(next(iter(colunas.values()))) if colunas else 1
    dados = {nome: colunas.get(nome, [""] * tamanho) for nome in ESQUEMA_SAMP.nomes}
    return pd.DataFrame(dados, dtype=str)


def normalizado(**colunas: list[str]) -> pd.DataFrame:
    """O mesmo bloco, já tipado — a entrada que o perfilador espera."""
    return Normalizador().normalizar(bloco(**colunas))


def perfilar(*blocos: pd.DataFrame, **ajustes: Any) -> Perfil:
    perfilador = Perfilador(**ajustes)
    for parte in blocos:
        perfilador.perfilar(parte)
    return perfilador.perfil()


@pytest.fixture(scope="module")
def perfil_real() -> Perfil:
    """Perfil da amostra real, montado como no pipeline: leitura -> normalização -> perfil."""
    leitor = LeitorCsv(FIXTURE_REAL)
    normalizador = Normalizador()
    perfilador = Perfilador()
    for _ in perfilador.perfilar_blocos(normalizador.normalizar_blocos(leitor.blocos())):
        pass
    return perfilador.perfil(
        arquivo=FIXTURE_REAL,
        ano=2024,
        leitura=leitor.relatorio,
        normalizacao=normalizador.relatorio,
    )


class TestContagem:
    def test_conta_linhas_de_varios_blocos(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1,5", "2,5"]), normalizado(VlrMercado=["3,5"]))
        assert perfil.linhas == 3

    def test_bloco_vazio_nao_conta(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1,5"]).iloc[0:0])
        assert perfil.linhas == 0

    def test_cardinalidade_por_campo(self) -> None:
        perfil = perfilar(normalizado(DscPostoTarifario=["Ponta", "Ponta", "Fora ponta"]))
        assert perfil.cardinalidades["DscPostoTarifario"] == 2

    def test_cardinalidade_acumula_entre_blocos(self) -> None:
        perfil = perfilar(
            normalizado(SigAgenteDistribuidora=["COCEL"]),
            normalizado(SigAgenteDistribuidora=["CEMIG"]),
        )
        assert perfil.cardinalidades["SigAgenteDistribuidora"] == 2

    def test_vlr_mercado_nao_entra_nas_cardinalidades(self) -> None:
        # Milhões de valores distintos não dizem nada e custariam a memória toda.
        perfil = perfilar(normalizado(VlrMercado=["1,5", "2,5"]))
        assert "VlrMercado" not in perfil.cardinalidades


class TestFaltantes:
    def test_texto_vazio_conta_como_vazio_nao_como_nulo(self) -> None:
        perfil = perfilar(normalizado(DscPostoTarifario=["", "Ponta"]))
        assert perfil.vazios["DscPostoTarifario"] == 1
        assert "DscPostoTarifario" not in perfil.nulos

    def test_valor_ilegivel_conta_como_nulo(self) -> None:
        # A normalização anulou o valor; para o perfil isso é ausência, com a evidência no _raw.
        perfil = perfilar(normalizado(VlrMercado=["1,5", "abc"]))
        assert perfil.nulos["VlrMercado"] == 1

    def test_campo_sem_falta_fica_de_fora(self) -> None:
        perfil = perfilar(normalizado(DscOpcaoEnergia=["Cativo"]))
        assert "DscOpcaoEnergia" not in perfil.nulos
        assert "DscOpcaoEnergia" not in perfil.vazios

    def test_coluna_ausente_e_reportada(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1,5"]).drop(columns=["DscPostoTarifario"]))
        assert perfil.campos_ausentes == ("DscPostoTarifario",)


class TestCompetencia:
    def test_periodo_e_o_intervalo_observado(self) -> None:
        perfil = perfilar(
            normalizado(DatCompetencia=["2024-03-01", "2024-01-01"]),
            normalizado(DatCompetencia=["2024-12-01"]),
        )
        assert perfil.competencia_min == date(2024, 1, 1)
        assert perfil.competencia_max == date(2024, 12, 1)

    def test_linhas_por_competencia_em_ordem(self) -> None:
        # É o que a DQ-COM-004 lê para dizer se as 12 competências estão cobertas.
        perfil = perfilar(normalizado(DatCompetencia=["2024-02-01", "2024-01-01", "2024-02-01"]))
        assert perfil.competencias() == {"2024-01-01": 1, "2024-02-01": 2}

    def test_competencia_ilegivel_nao_entra_no_periodo(self) -> None:
        perfil = perfilar(normalizado(DatCompetencia=["2024-02-30", "2024-05-01"]))
        assert perfil.competencia_min == date(2024, 5, 1)
        assert perfil.nulos["DatCompetencia"] == 1


class TestDistribuicao:
    def test_minimo_maximo_e_soma_sao_exatos(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["99999999999999,999999", "-12,5", "0,000001"]))
        assert perfil.valor.minimo == Decimal("-12.500000")
        assert perfil.valor.maximo == Decimal("99999999999999.999999")
        assert perfil.valor.soma == Decimal("99999999999987.500000")

    def test_soma_de_muitos_valores_nao_estoura_a_precisao(self) -> None:
        # decimal(20,6) não comporta a soma de um ano inteiro; o acumulador usa 38 dígitos.
        grandes = normalizado(VlrMercado=["99999999999999,999999"] * 100)
        assert perfilar(grandes).valor.soma == Decimal("9999999999999999.999900")

    def test_conta_negativos_e_zeros(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["-1,5", "0", "0", "2"]))
        assert (perfil.valor.negativos, perfil.valor.zeros) == (1, 2)

    def test_nulo_nao_entra_na_contagem_de_valores(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1,5", ""]))
        assert (perfil.valor.contagem, perfil.valor.nulos) == (1, 1)

    def test_mediana_exata_quando_tudo_cabe_na_amostra(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1", "2", "3"]))
        assert perfil.valor_mediana == 2.0
        assert perfil.valor_mediana_exata is True

    def test_mediana_vira_estimativa_quando_a_amostra_enche(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=[str(n) for n in range(200)]), tamanho_amostra=50)
        assert perfil.valor_mediana_exata is False
        assert perfil.valor_mediana == pytest.approx(99.5, abs=25)

    def test_quebra_por_detalhe_de_mercado(self) -> None:
        # Sem esta quebra, mínimo e máximo globais comparariam kWh com R$.
        perfil = perfilar(
            normalizado(
                DscDetalheMercado=["Energia TUSD (kWh)", "Receita Demanda (R$)", "Receita (R$)"],
                VlrMercado=["100", "-5", "7,5"],
            )
        )
        assert perfil.por_detalhe["Energia TUSD (kWh)"].maximo == Decimal("100.000000")
        assert perfil.por_detalhe["Receita Demanda (R$)"].negativos == 1
        assert perfil.por_detalhe["Receita (R$)"].soma == Decimal("7.500000")

    def test_detalhe_acumula_entre_blocos(self) -> None:
        detalhe = ["Energia TUSD (kWh)"]
        perfil = perfilar(
            normalizado(DscDetalheMercado=detalhe, VlrMercado=["10"]),
            normalizado(DscDetalheMercado=detalhe, VlrMercado=["4"]),
        )
        assert perfil.por_detalhe["Energia TUSD (kWh)"].contagem == 2
        assert perfil.por_detalhe["Energia TUSD (kWh)"].minimo == Decimal("4.000000")


class TestDominiosObservados:
    def test_so_campos_de_dominio(self) -> None:
        perfil = perfilar(normalizado(DscOpcaoEnergia=["Cativo"], NomAgenteAcessante=["FULANO"]))
        assert "DscOpcaoEnergia" in perfil.dominios_observados()
        assert "NomAgenteAcessante" not in perfil.dominios_observados()

    def test_conta_cada_valor_como_ele_veio(self) -> None:
        # docs/04 §3: nada de unificar caixa aqui — a divergência é o achado (DQ-VAL-009).
        perfil = perfilar(normalizado(DscOpcaoEnergia=["CATIVO", "Cativo", "Cativo"]))
        assert perfil.dominios_observados()["DscOpcaoEnergia"] == {"Cativo": 2, "CATIVO": 1}

    def test_ordena_do_mais_frequente_para_o_menos(self) -> None:
        perfil = perfilar(normalizado(DscSubGrupoTarifario=["B1", "A4", "A4"]))
        assert list(perfil.dominios_observados()["DscSubGrupoTarifario"]) == ["A4", "B1"]


class TestTeto:
    def test_campo_muito_variado_e_marcado_como_truncado(self) -> None:
        valores = [f"NOME {n}" for n in range(10)]
        perfil = perfilar(normalizado(NomAgenteAcessante=valores), limite_distintos=5)
        assert "NomAgenteAcessante" in perfil.truncados
        assert perfil.cardinalidades["NomAgenteAcessante"] == 5

    def test_dominio_truncado_fica_fora_do_arquivo_de_dominios(self) -> None:
        # Publicar contagem parcial calibraria as listas de docs/03 com meia verdade.
        perfil = perfilar(
            normalizado(DscDetalheMercado=[f"D{n}" for n in range(10)]), limite_distintos=3
        )
        assert perfil.truncados == ("DscDetalheMercado",)
        assert "DscDetalheMercado" not in perfil.dominios_observados()

    def test_contagem_para_de_crescer_depois_do_teto(self) -> None:
        contagem = ContagemDeValores(limite=2)
        contagem.observar(pa.array(["a", "b", "c"]))
        contagem.observar(pa.array(["d", "e"]))
        assert contagem.truncado
        assert contagem.contagens == {}
        assert contagem.ocorrencias == 5


class TestAmostra:
    def test_guarda_tudo_enquanto_couber(self) -> None:
        amostra = Amostra(tamanho=10)
        amostra.observar(pd.Series([1.0, 2.0, 3.0]).to_numpy())
        assert amostra.integral
        assert amostra.mediana() == 2.0

    def test_limita_o_tamanho_e_deixa_de_ser_integral(self) -> None:
        amostra = Amostra(tamanho=10)
        amostra.observar(pd.Series(range(100)).astype("float64").to_numpy())
        assert not amostra.integral
        assert amostra.vistos == 100

    def test_mesma_semente_da_a_mesma_amostra(self) -> None:
        # Perfil de um mesmo arquivo tem de ser reprodutível (docs/04 §10).
        valores = pd.Series(range(1000)).astype("float64").to_numpy()
        primeira, segunda = Amostra(tamanho=50), Amostra(tamanho=50)
        primeira.observar(valores)
        segunda.observar(valores)
        assert primeira.mediana() == segunda.mediana()

    def test_amostra_vazia_nao_tem_mediana(self) -> None:
        assert Amostra().mediana() is None


class TestNaoJulga:
    """docs/04 §1: o perfil descreve; quem acusa violação é o catálogo de regras."""

    def test_valor_fora_do_dicionario_e_apenas_contado(self) -> None:
        perfil = perfilar(normalizado(NomTipoMercado=["Regular"]))
        assert perfil.dominios_observados()["NomTipoMercado"] == {"Regular": 1}

    def test_competencia_de_outro_ano_nao_e_descartada(self) -> None:
        perfil = perfilar(normalizado(DatCompetencia=["2023-12-01"]))
        assert perfil.competencia_min == date(2023, 12, 1)

    def test_duplicata_conta_como_duas_linhas(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1,5", "1,5"]))
        assert perfil.linhas == 2
        assert perfil.valor.contagem == 2


class TestJson:
    def test_contrato_de_docs04(self, perfil_real: Perfil) -> None:
        dados = perfil_real.como_json()
        esperadas = {
            "arquivo",
            "ano",
            "geradoEm",
            "origem",
            "linhasTotais",
            "linhasDescartadas",
            "nulosPorCampo",
            "cardinalidades",
            "periodoCompetencia",
            "distribuicaoVlrMercado",
            "normalizacoes",
        }
        assert esperadas <= set(dados)
        assert dados["arquivo"] == "samp-real-amostra.csv"
        assert dados["ano"] == 2024
        assert dados["periodoCompetencia"] == {"min": "2024-01-01", "max": "2024-01-01"}

    def test_decimal_vai_como_texto_para_nao_perder_digitos(self) -> None:
        perfil = perfilar(normalizado(VlrMercado=["99999999999999,999999"]))
        assert perfil.como_json()["distribuicaoVlrMercado"]["max"] == "99999999999999.999999"

    def test_normalizacoes_entram_quando_ha_relatorio(self, perfil_real: Perfil) -> None:
        assert perfil_real.como_json()["normalizacoes"]["encodingConvertido"] == "cp1252 -> utf-8"

    def test_sem_relatorio_normalizacoes_sai_vazio(self) -> None:
        # Perfil vindo do Parquet: a normalização foi noutra execução, e dizer zero seria mentir.
        assert perfilar(normalizado(VlrMercado=["1,5"])).como_json()["normalizacoes"] == {}

    def test_e_serializavel(self, perfil_real: Perfil) -> None:
        dados = json.loads(json.dumps(perfil_real.como_json(), ensure_ascii=False))
        assert dados["linhasTotais"] == 24


class TestArquivoReal:
    def test_conta_as_linhas_da_amostra(self, perfil_real: Perfil) -> None:
        assert perfil_real.linhas == 24

    def test_encontra_os_achados_conhecidos_de_docs03(self, perfil_real: Perfil) -> None:
        dominios = perfil_real.dominios_observados()
        assert "CATIVO" in dominios["DscOpcaoEnergia"]  # achado 2
        assert "Regular" in dominios["NomTipoMercado"]  # achado 1

    def test_soma_confere_com_a_do_pandas(self, perfil_real: Perfil) -> None:
        quadro = pd.concat(
            list(Normalizador().normalizar_blocos(LeitorCsv(FIXTURE_REAL).blocos())),
            ignore_index=True,
        )
        assert perfil_real.valor.soma == quadro["VlrMercado"].sum()
        assert perfil_real.valor.contagem == 24

    def test_origem_vem_do_arquivo(self, perfil_real: Perfil) -> None:
        assert perfil_real.origem.tamanho_bytes == FIXTURE_REAL.stat().st_size


class TestOrigem:
    def test_aproveita_o_sidecar_do_download(self, tmp_path: Path) -> None:
        arquivo = tmp_path / "samp-2024.csv"
        arquivo.write_text("x", encoding="utf-8")
        escrever_json(
            tmp_path / "samp-2024.csv.samp-dq.json",
            {
                "url": "https://exemplo/samp-2024.csv",
                "ultima_modificacao_ckan": "2026-07-28T15:53:32+00:00",
                "sha256": "abc123",
            },
        )
        origem = Origem.do_arquivo(arquivo)
        assert origem.url == "https://exemplo/samp-2024.csv"
        assert origem.chave == "abc123"
        assert origem.como_json()["lastModifiedCkan"] == "2026-07-28T15:53:32+00:00"

    def test_sem_sidecar_descreve_o_disco(self, tmp_path: Path) -> None:
        arquivo = tmp_path / "samp-2024.csv"
        arquivo.write_text("abc", encoding="utf-8")
        origem = Origem.do_arquivo(arquivo, chave="xyz")
        assert origem.tamanho_bytes == 3
        assert (origem.url, origem.chave) == ("", "xyz")


class TestAno:
    @pytest.mark.parametrize(
        ("nome", "esperado"),
        [
            ("samp-2024.csv", 2024),
            ("bruto/samp-2003.parquet", 2003),
            ("SAMP_1999.csv", 1999),
            ("samp.csv", None),
        ],
    )
    def test_extrai_do_nome(self, nome: str, esperado: int | None) -> None:
        assert ano_do_arquivo(nome) == esperado


class TestParquet:
    @pytest.fixture
    def parquet(self, tmp_path: Path) -> Path:
        """O Parquet do samp-dq, gerado da amostra real como no pipeline."""
        leitor = LeitorCsv(FIXTURE_REAL)
        destino = tmp_path / "samp-2024.parquet"
        escrever_parquet(
            Normalizador().normalizar_blocos(leitor.blocos()),
            destino,
            chave=chave_do_insumo(FIXTURE_REAL),
        )
        return destino

    def test_perfil_do_parquet_bate_com_o_do_csv(self, parquet: Path, perfil_real: Perfil) -> None:
        # Mesmo dado, mesma medida: é o que permite reperfilar sem reler o CSV.
        perfil = perfilar_parquet(parquet)
        assert perfil.linhas == perfil_real.linhas
        assert perfil.valor.soma == perfil_real.valor.soma
        assert perfil.dominios_observados() == perfil_real.dominios_observados()

    def test_ano_sai_do_nome_do_arquivo(self, parquet: Path) -> None:
        assert perfilar_parquet(parquet).ano == 2024


class TestGravacao:
    def test_grava_os_dois_artefatos(self, perfil_real: Perfil, tmp_path: Path) -> None:
        caminho = gravar_perfil(perfil_real, tmp_path)
        dominios = gravar_dominios_observados(perfil_real, tmp_path)
        assert caminho.name == "perfil-2024.json"
        assert dominios.name == "dominios-observados-2024.json"
        assert ler_json(caminho)["linhasTotais"] == 24  # type: ignore[index]

    def test_acento_nao_vira_escape(self, perfil_real: Perfil, tmp_path: Path) -> None:
        texto = gravar_dominios_observados(perfil_real, tmp_path).read_text(encoding="utf-8")
        assert "Poder público" in texto

    def test_sem_ano_o_nome_e_explicito(self, tmp_path: Path) -> None:
        perfil = perfilar(normalizado(VlrMercado=["1,5"]))
        assert gravar_perfil(perfil, tmp_path).name == "perfil-sem-ano.json"

    def test_arquivo_corrompido_e_tratado_como_ausente(self, tmp_path: Path) -> None:
        quebrado = tmp_path / "perfil-2024.json"
        quebrado.write_text("{isto não é json", encoding="utf-8")
        assert ler_json(quebrado) is None

    def test_nao_deixa_parcial_para_tras(self, perfil_real: Perfil, tmp_path: Path) -> None:
        gravar_perfil(perfil_real, tmp_path)
        assert list(tmp_path.glob("*.part")) == []
