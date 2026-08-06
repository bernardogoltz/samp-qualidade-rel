"""O contrato das 18 colunas e a validação de cabeçalho (DQ-VAL-001)."""

from __future__ import annotations

from pathlib import Path

import pytest

from samp_dq.errors import CampoDesconhecidoError
from samp_dq.ingest.schema import (
    CAMPOS_SO_NO_DICIONARIO,
    ENCODING_ORIGEM,
    ESQUEMA_SAMP,
    SEPARADOR,
    Campo,
    TipoCampo,
    dividir_cabecalho,
)

FIXTURE_REAL = Path(__file__).parent.parent / "fixtures" / "samp-real-amostra.csv"


@pytest.fixture(scope="module")
def cabecalho_real() -> tuple[str, ...]:
    """O cabeçalho como o portal publica — a amostra foi capturada da fonte."""
    with FIXTURE_REAL.open(encoding=ENCODING_ORIGEM) as arquivo:
        return dividir_cabecalho(arquivo.readline())


class TestEsquema:
    def test_tem_dezoito_campos(self) -> None:
        # O dicionário v1.1 lista 19; o arquivo real publica 18.
        assert len(ESQUEMA_SAMP) == 18

    def test_bate_com_o_cabecalho_do_arquivo_real(self, cabecalho_real: tuple[str, ...]) -> None:
        assert ESQUEMA_SAMP.nomes == cabecalho_real

    def test_campo_do_dicionario_ausente_no_arquivo_nao_entra(self) -> None:
        assert "DscClassificacao" in CAMPOS_SO_NO_DICIONARIO
        assert "DscClassificacao" not in ESQUEMA_SAMP.nomes

    def test_grafia_do_arquivo_prevalece_sobre_a_do_dicionario(self) -> None:
        ceg = ESQUEMA_SAMP.campo("IdeNucleoCeg")
        assert ceg.nome_dicionario == "IdeNucleoCEG"
        assert ceg.diverge_do_dicionario

    def test_campo_desconhecido_levanta(self) -> None:
        with pytest.raises(CampoDesconhecidoError, match="IdeNucleoCEG"):
            ESQUEMA_SAMP.campo("IdeNucleoCEG")

    def test_tipos_dos_campos_criticos(self) -> None:
        assert ESQUEMA_SAMP.campo("VlrMercado").tipo is TipoCampo.DECIMAL
        assert ESQUEMA_SAMP.campo("DatCompetencia").tipo is TipoCampo.DATA
        assert ESQUEMA_SAMP.campo("IdeNucleoCeg").tipo is TipoCampo.INTEIRO
        # CNPJ é texto de propósito: como inteiro perderia os zeros à esquerda.
        assert ESQUEMA_SAMP.campo("NumCNPJAgenteDistribuidora").tipo is TipoCampo.TEXTO

    def test_precisao_do_valor_segue_o_dicionario(self) -> None:
        valor = ESQUEMA_SAMP.campo("VlrMercado")
        assert (valor.precisao, valor.escala) == (20, 6)

    def test_campos_de_dominio_sao_os_catalogaveis(self) -> None:
        # Alimentam o dominios-observados-{ano}.json; CNPJ e nome de agente ficam de fora
        # por cardinalidade alta.
        assert "DscSubGrupoTarifario" in ESQUEMA_SAMP.campos_de_dominio
        assert "DscOpcaoEnergia" in ESQUEMA_SAMP.campos_de_dominio
        assert "NomAgenteDistribuidora" not in ESQUEMA_SAMP.campos_de_dominio
        assert "VlrMercado" not in ESQUEMA_SAMP.campos_de_dominio

    def test_e_iteravel_e_indexavel_por_posicao(self) -> None:
        assert [c.nome for c in ESQUEMA_SAMP] == list(ESQUEMA_SAMP.nomes)
        assert ESQUEMA_SAMP[0].nome == "DatGeracaoConjuntoDados"

    def test_todo_campo_texto_declara_tamanho_maximo(self) -> None:
        # DQ-VAL-012 depende disso; um campo sem limite passaria despercebido.
        sem_limite = [
            c.nome for c in ESQUEMA_SAMP if c.tipo is TipoCampo.TEXTO and not c.tamanho_max
        ]
        assert sem_limite == []


class TestDividirCabecalho:
    def test_remove_aspas_e_separa_por_ponto_e_virgula(self) -> None:
        assert dividir_cabecalho('"A";"B";"C"') == ("A", "B", "C")

    def test_aceita_cabecalho_sem_aspas(self) -> None:
        assert dividir_cabecalho("A;B;C") == ("A", "B", "C")

    def test_descarta_bom_e_quebra_de_linha(self) -> None:
        assert dividir_cabecalho('﻿"A";"B"\r\n') == ("A", "B")

    def test_apara_espaco_de_preenchimento(self) -> None:
        assert dividir_cabecalho('" A " ; "B"') == ("A", "B")

    def test_linha_vazia_vira_tupla_vazia(self) -> None:
        assert dividir_cabecalho("") == ()

    def test_separador_do_modulo_e_o_do_arquivo(self) -> None:
        assert SEPARADOR == ";"


class TestValidarCabecalho:
    def test_cabecalho_real_e_conforme(self, cabecalho_real: tuple[str, ...]) -> None:
        resultado = ESQUEMA_SAMP.validar_cabecalho(cabecalho_real)
        assert resultado.conforme
        assert resultado.resumo() == "cabeçalho conforme: 18 colunas na ordem esperada"

    def test_coluna_faltante_e_apontada_pelo_nome(self) -> None:
        resultado = ESQUEMA_SAMP.validar_cabecalho(
            [n for n in ESQUEMA_SAMP.nomes if n != "VlrMercado"]
        )
        assert not resultado.conforme
        assert resultado.faltantes == ("VlrMercado",)
        assert resultado.inesperadas == ()
        assert "VlrMercado" in resultado.resumo()

    def test_coluna_nova_na_origem_e_apontada(self) -> None:
        resultado = ESQUEMA_SAMP.validar_cabecalho([*ESQUEMA_SAMP.nomes, "DscClassificacao"])
        assert resultado.inesperadas == ("DscClassificacao",)
        assert resultado.faltantes == ()
        assert not resultado.conforme
        assert "inesperadas DscClassificacao" in resultado.resumo()

    def test_grafia_divergente_conta_dos_dois_lados(self) -> None:
        # Se a ANEEL alinhar o arquivo ao dicionário, o achado tem de ser explícito.
        trocado = ["IdeNucleoCEG" if n == "IdeNucleoCeg" else n for n in ESQUEMA_SAMP.nomes]
        resultado = ESQUEMA_SAMP.validar_cabecalho(trocado)
        assert resultado.faltantes == ("IdeNucleoCeg",)
        assert resultado.inesperadas == ("IdeNucleoCEG",)

    def test_mesmas_colunas_em_ordem_trocada(self) -> None:
        invertido = list(reversed(ESQUEMA_SAMP.nomes))
        resultado = ESQUEMA_SAMP.validar_cabecalho(invertido)
        assert resultado.faltantes == ()
        assert resultado.inesperadas == ()
        assert resultado.ordem_divergente
        assert not resultado.conforme
        assert "ordem" in resultado.resumo()

    def test_ordem_so_diverge_quando_o_conjunto_bate(self) -> None:
        # Faltando uma coluna, o resto continua na ordem relativa correta.
        resultado = ESQUEMA_SAMP.validar_cabecalho(
            [n for n in ESQUEMA_SAMP.nomes if n != "VlrMercado"]
        )
        assert not resultado.ordem_divergente

    def test_cabecalho_vazio_reporta_tudo_faltante(self) -> None:
        resultado = ESQUEMA_SAMP.validar_cabecalho([])
        assert resultado.faltantes == ESQUEMA_SAMP.nomes
        assert not resultado.conforme

    def test_aceita_a_linha_crua_do_arquivo(self) -> None:
        linha = SEPARADOR.join(f'"{n}"' for n in ESQUEMA_SAMP.nomes)
        assert ESQUEMA_SAMP.validar_cabecalho(dividir_cabecalho(linha)).conforme


class TestCampo:
    def test_e_imutavel(self) -> None:
        campo = ESQUEMA_SAMP[0]
        with pytest.raises(AttributeError):
            campo.nome = "outro"  # type: ignore[misc]

    def test_sem_nome_de_dicionario_nao_diverge(self) -> None:
        assert not Campo("VlrMercado", TipoCampo.DECIMAL).diverge_do_dicionario
