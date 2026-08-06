"""Normalização lossless (etapa 3 de docs/04)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from samp_dq.ingest.leitura import LeitorCsv
from samp_dq.ingest.normalizacao import Normalizador
from samp_dq.ingest.schema import ESQUEMA_SAMP, SUFIXO_RAW

FIXTURE_REAL = Path(__file__).parent.parent / "fixtures" / "samp-real-amostra.csv"


def bloco(**colunas: list[str]) -> pd.DataFrame:
    """Um bloco como a Parte 2 entrega: 18 colunas, tudo texto, vazio como ''."""
    tamanho = len(next(iter(colunas.values()))) if colunas else 1
    dados = {nome: colunas.get(nome, [""] * tamanho) for nome in ESQUEMA_SAMP.nomes}
    return pd.DataFrame(dados, dtype=str)


@pytest.fixture
def normalizador() -> Normalizador:
    return Normalizador()


@pytest.fixture(scope="module")
def real_normalizado() -> tuple[pd.DataFrame, Normalizador]:
    leitor = LeitorCsv(FIXTURE_REAL)
    norm = Normalizador()
    quadro = pd.concat(list(norm.normalizar_blocos(leitor.blocos())), ignore_index=True)
    return quadro, norm


class TestDecimal:
    def test_virgula_vira_ponto(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(VlrMercado=["75313,000000"]))
        assert saida["VlrMercado"].iloc[0] == Decimal("75313.000000")

    def test_preserva_vinte_digitos(self, normalizador: Normalizador) -> None:
        # O motivo de decimal128(20,6) e não float64: aqui o float já teria arredondado.
        saida = normalizador.normalizar(bloco(VlrMercado=["99999999999999,999999"]))
        assert saida["VlrMercado"].iloc[0] == Decimal("99999999999999.999999")

    def test_negativo(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(VlrMercado=["-12,5"]))
        assert saida["VlrMercado"].iloc[0] == Decimal("-12.500000")

    def test_sem_parte_inteira_e_recuperado(self, normalizador: Normalizador) -> None:
        """`,31` é 0,31 — inequívoco. Anulá-lo perderia ~1% dos valores de um arquivo real."""
        saida = normalizador.normalizar(bloco(VlrMercado=[",310000", "-,410000"]))
        assert saida["VlrMercado"].tolist() == [Decimal("0.310000"), Decimal("-0.410000")]
        assert normalizador.relatorio.valores_invalidos == {}

    def test_sem_parte_inteira_e_contado(self, normalizador: Normalizador) -> None:
        # Recuperar o valor não apaga o achado: a DQ-VAL-014 precisa do número.
        normalizador.normalizar(bloco(VlrMercado=[",31", "1,5", "-,4"]))
        assert normalizador.relatorio.decimais_sem_parte_inteira == 2

    def test_so_virgula_nao_e_numero(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=[",", "-"]))
        assert normalizador.relatorio.valores_invalidos["VlrMercado"] == 2

    def test_conta_os_reparados(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["1,5", "2,5", "3"]))
        assert normalizador.relatorio.decimais_reparados == 2

    def test_valor_ilegivel_vira_nulo_e_e_contado(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(VlrMercado=["1,5", "abc", "1,2,3"]))
        assert pd.isna(saida["VlrMercado"].iloc[1])
        assert normalizador.relatorio.valores_invalidos["VlrMercado"] == 2

    def test_precisao_excedida_e_invalida(self, normalizador: Normalizador) -> None:
        # Acomodar 7 casas exigiria arredondar — seria corrigir, não normalizar (DQ-VAL-014).
        normalizador.normalizar(bloco(VlrMercado=["1,1234567"]))
        assert normalizador.relatorio.valores_invalidos["VlrMercado"] == 1

    def test_vazio_nao_e_invalido(self, normalizador: Normalizador) -> None:
        # Ausência é achado de completude (DQ-COM-002), não de formato.
        saida = normalizador.normalizar(bloco(VlrMercado=[""]))
        assert pd.isna(saida["VlrMercado"].iloc[0])
        assert "VlrMercado" not in normalizador.relatorio.valores_invalidos


class TestDatas:
    def test_iso_vira_date(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(DatCompetencia=["2024-01-01"]))
        assert saida["DatCompetencia"].iloc[0] == date(2024, 1, 1)

    def test_data_impossivel_vira_nulo_e_e_contada(self, normalizador: Normalizador) -> None:
        # docs/04 §7: falha de data não se "conserta"; vira violação DQ-VAL-002.
        saida = normalizador.normalizar(bloco(DatCompetencia=["2024-02-30", "2024-13-45"]))
        assert saida["DatCompetencia"].isna().all()
        assert normalizador.relatorio.valores_invalidos["DatCompetencia"] == 2

    def test_formato_alheio_e_invalido(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(DatCompetencia=["01/01/2024"]))
        assert normalizador.relatorio.valores_invalidos["DatCompetencia"] == 1

    def test_vazio_nao_e_invalido(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(DatCompetencia=[""]))
        assert "DatCompetencia" not in normalizador.relatorio.valores_invalidos


class TestInteiro:
    def test_converte(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(IdeNucleoCeg=["0", "12345"]))
        assert saida["IdeNucleoCeg"].tolist() == [0, 12345]

    def test_nao_numerico_e_contado(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(IdeNucleoCeg=["x"]))
        assert normalizador.relatorio.valores_invalidos["IdeNucleoCeg"] == 1

    def test_comprimento_excedido_nao_e_problema_da_normalizacao(
        self, normalizador: Normalizador
    ) -> None:
        # 6 dígitos violam a DQ-VAL-013, mas o valor é legível: convertê-lo é lossless.
        saida = normalizador.normalizar(bloco(IdeNucleoCeg=["123456"]))
        assert saida["IdeNucleoCeg"].iloc[0] == 123456
        assert "IdeNucleoCeg" not in normalizador.relatorio.valores_invalidos


class TestTrim:
    def test_apara_e_conta_por_campo(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(
            bloco(NumCNPJAgenteAcessante=["37121669900   ", "123", "  456  "])
        )
        assert saida["NumCNPJAgenteAcessante"].tolist() == ["37121669900", "123", "456"]
        assert normalizador.relatorio.campos_trimados["NumCNPJAgenteAcessante"] == 2

    def test_campo_sem_espaco_nao_entra_na_contagem(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(SigAgenteDistribuidora=["COCEL"]))
        assert "SigAgenteDistribuidora" not in normalizador.relatorio.campos_trimados


class TestColunasRaw:
    def test_apenas_os_dois_campos_declarados(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(VlrMercado=["1,5"]))
        cruas = [c for c in saida.columns if c.endswith(SUFIXO_RAW)]
        assert cruas == ["NumCNPJAgenteAcessante_raw", "VlrMercado_raw"]

    def test_raw_guarda_o_texto_antes_do_trim_e_da_tipagem(
        self, normalizador: Normalizador
    ) -> None:
        saida = normalizador.normalizar(
            bloco(VlrMercado=["75313,000000"], NumCNPJAgenteAcessante=["37121669900   "])
        )
        assert saida["VlrMercado_raw"].iloc[0] == "75313,000000"
        assert saida["NumCNPJAgenteAcessante_raw"].iloc[0] == "37121669900   "

    def test_raw_sobrevive_a_valor_ilegivel(self, normalizador: Normalizador) -> None:
        # A evidência não pode sumir do Parquet junto com o valor que não converteu.
        saida = normalizador.normalizar(bloco(VlrMercado=["1,2,3"]))
        assert pd.isna(saida["VlrMercado"].iloc[0])
        assert saida["VlrMercado_raw"].iloc[0] == "1,2,3"

    def test_colunas_do_esquema_vem_primeiro_e_na_ordem(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco())
        assert tuple(saida.columns[:18]) == ESQUEMA_SAMP.nomes

    def test_declarados_no_esquema(self) -> None:
        assert ESQUEMA_SAMP.campos_com_raw == ("NumCNPJAgenteAcessante", "VlrMercado")

    def test_bloco_sem_uma_coluna_e_normalizado_do_mesmo_jeito(
        self, normalizador: Normalizador
    ) -> None:
        # Acontece com estrito=False sobre arquivo de layout mudado: normaliza o que existe.
        parcial = bloco(VlrMercado=["1,5"]).drop(columns=["DatCompetencia"])
        saida = normalizador.normalizar(parcial)
        assert "DatCompetencia" not in saida.columns
        assert saida["VlrMercado"].iloc[0] == Decimal("1.500000")


class TestNaoCorrige:
    """docs/04 §3: normalizar nunca conserta problema de qualidade."""

    def test_caixa_divergente_e_preservada(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(DscOpcaoEnergia=["CATIVO", "Cativo"]))
        assert saida["DscOpcaoEnergia"].tolist() == ["CATIVO", "Cativo"]

    def test_valor_fora_do_dicionario_e_preservado(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(NomTipoMercado=["Regular"]))
        assert saida["NomTipoMercado"].iloc[0] == "Regular"

    def test_texto_vazio_nao_vira_placeholder(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(DscPostoTarifario=[""]))
        assert saida["DscPostoTarifario"].iloc[0] == ""

    def test_cnpj_continua_texto_com_zeros_a_esquerda(self, normalizador: Normalizador) -> None:
        saida = normalizador.normalizar(bloco(NumCNPJAgenteDistribuidora=["00075805895000"]))
        assert saida["NumCNPJAgenteDistribuidora"].iloc[0] == "00075805895000"


class TestExemplos:
    def test_guarda_o_valor_e_a_posicao(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["1,5", "abc"]))
        (exemplo,) = normalizador.relatorio.exemplos_invalidos
        assert exemplo.campo == "VlrMercado"
        assert exemplo.valor == "abc"
        assert exemplo.registro == 2

    def test_posicao_acumula_entre_blocos(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["1,5", "2,5"]))
        normalizador.normalizar(bloco(VlrMercado=["ruim"]))
        (exemplo,) = normalizador.relatorio.exemplos_invalidos
        assert exemplo.registro == 3

    def test_limita_a_cinco_por_campo(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["ruim"] * 9))
        assert normalizador.relatorio.valores_invalidos["VlrMercado"] == 9
        assert len(normalizador.relatorio.exemplos_invalidos) == 5

    def test_o_limite_vale_entre_blocos(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["ruim"] * 5))
        normalizador.normalizar(bloco(VlrMercado=["ruim"] * 4))
        assert normalizador.relatorio.valores_invalidos["VlrMercado"] == 9
        assert len(normalizador.relatorio.exemplos_invalidos) == 5


class TestRelatorio:
    def test_conta_as_linhas(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["1,5", "2,5"]))
        normalizador.normalizar(bloco(VlrMercado=["3,5"]))
        assert normalizador.relatorio.linhas == 3

    def test_bloco_do_perfil_segue_o_contrato_de_docs04(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["1,5"], NumCNPJAgenteAcessante=["123   "]))
        perfil = normalizador.relatorio.como_perfil()
        assert perfil["encodingConvertido"] == "cp1252 -> utf-8"
        assert perfil["decimaisReparados"] == 1
        assert perfil["decimaisSemParteInteira"] == 0
        assert perfil["camposTrimados"] == {"NumCNPJAgenteAcessante": 1}
        assert perfil["linhasMalformadas"] == 0

    def test_linhas_malformadas_vem_da_leitura(self, normalizador: Normalizador) -> None:
        leitor = LeitorCsv(FIXTURE_REAL)
        for parte in normalizador.normalizar_blocos(leitor.blocos()):
            assert not parte.empty
        perfil = normalizador.relatorio.como_perfil(leitor.relatorio)
        assert perfil["linhasMalformadas"] == 0

    def test_resumo_menciona_invalidos(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(VlrMercado=["ruim"]))
        assert "1 valor ilegível" in normalizador.relatorio.resumo()

    def test_resumo_menciona_aparados(self, normalizador: Normalizador) -> None:
        normalizador.normalizar(bloco(NumCNPJAgenteAcessante=["123  ", "456  "]))
        assert "2 campos aparados" in normalizador.relatorio.resumo()


class TestArquivoReal:
    def test_tipos_finais(self, real_normalizado: tuple[pd.DataFrame, Normalizador]) -> None:
        quadro, _ = real_normalizado
        assert str(quadro["VlrMercado"].dtype) == "decimal128(20, 6)[pyarrow]"
        assert str(quadro["DatCompetencia"].dtype) == "date32[day][pyarrow]"
        assert str(quadro["IdeNucleoCeg"].dtype) == "int64[pyarrow]"

    def test_nenhum_valor_ilegivel(
        self, real_normalizado: tuple[pd.DataFrame, Normalizador]
    ) -> None:
        _, norm = real_normalizado
        assert norm.relatorio.valores_invalidos == {}

    def test_todo_decimal_foi_reparado(
        self, real_normalizado: tuple[pd.DataFrame, Normalizador]
    ) -> None:
        quadro, norm = real_normalizado
        assert norm.relatorio.decimais_reparados == len(quadro)

    def test_o_cpf_com_espacos_foi_aparado_mas_preservado(
        self, real_normalizado: tuple[pd.DataFrame, Normalizador]
    ) -> None:
        quadro, norm = real_normalizado
        assert quadro["NumCNPJAgenteAcessante"].iloc[0] == "37121669900"
        assert quadro["NumCNPJAgenteAcessante_raw"].iloc[0] == "37121669900   "
        assert norm.relatorio.campos_trimados["NumCNPJAgenteAcessante"] == len(quadro)

    def test_competencias_dentro_do_ano(
        self, real_normalizado: tuple[pd.DataFrame, Normalizador]
    ) -> None:
        quadro, _ = real_normalizado
        assert quadro["DatCompetencia"].min() == date(2024, 1, 1)
