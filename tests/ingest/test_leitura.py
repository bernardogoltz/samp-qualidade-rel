"""Leitura do CSV bruto em blocos (etapa 2 de docs/04)."""

from __future__ import annotations

import warnings
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from samp_dq.errors import (
    ArquivoVazioError,
    CabecalhoInvalidoError,
    EncodingInvalidoError,
    EstruturaInconsistenteError,
)
from samp_dq.ingest.leitura import MAX_EXEMPLOS, LeitorCsv
from samp_dq.ingest.schema import ENCODING_ORIGEM, ESQUEMA_SAMP, SEPARADOR

FIXTURE_REAL = Path(__file__).parent.parent / "fixtures" / "samp-real-amostra.csv"
LINHAS_REAIS = 24  # a amostra tem cabeçalho + 24 registros

CabecalhoEscritor = Callable[..., Path]


def _linha(*valores: str) -> str:
    return SEPARADOR.join(f'"{v}"' for v in valores)


@pytest.fixture
def escrever_csv(tmp_path: Path) -> CabecalhoEscritor:
    """Grava um CSV em cp1252, como o portal publica."""

    def escrever(*linhas: str, nome: str = "samp.csv") -> Path:
        caminho = tmp_path / nome
        caminho.write_bytes(("\r\n".join(linhas) + "\r\n").encode(ENCODING_ORIGEM))
        return caminho

    return escrever


@pytest.fixture
def csv_valido(escrever_csv: CabecalhoEscritor) -> Path:
    """Cabeçalho conforme e três registros com 18 campos."""
    cabecalho = _linha(*ESQUEMA_SAMP.nomes)
    corpo = [_linha(*[f"v{i}{j}" for j in range(18)]) for i in range(3)]
    return escrever_csv(cabecalho, *corpo)


class TestLeituraDoArquivoReal:
    def test_le_todos_os_registros(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL)
        assert len(leitor.ler_tudo()) == LINHAS_REAIS
        assert leitor.relatorio.linhas_lidas == LINHAS_REAIS

    def test_colunas_sao_as_do_esquema(self) -> None:
        quadro = LeitorCsv(FIXTURE_REAL).ler_tudo()
        assert tuple(quadro.columns) == ESQUEMA_SAMP.nomes

    def test_cabecalho_e_conforme(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL)
        assert leitor.cabecalho().conforme
        assert leitor.relatorio.cabecalho is not None

    def test_tudo_chega_como_texto(self) -> None:
        # Tipagem é etapa 3; aqui nada pode ser inferido pelo pandas.
        quadro = LeitorCsv(FIXTURE_REAL).ler_tudo()
        assert set(quadro.dtypes.astype(str)) == {"str"}

    def test_decimal_chega_cru_com_virgula(self) -> None:
        quadro = LeitorCsv(FIXTURE_REAL).ler_tudo()
        assert quadro["VlrMercado"].iloc[0] == "75313,000000"

    def test_espacos_de_preenchimento_sao_preservados(self) -> None:
        # O trim é etapa 3 e precisa ser contabilizado; a leitura não pode antecipá-lo.
        quadro = LeitorCsv(FIXTURE_REAL).ler_tudo()
        assert quadro["NumCNPJAgenteAcessante"].iloc[0] == "37121669900   "

    def test_acentuacao_decodificada(self) -> None:
        quadro = LeitorCsv(FIXTURE_REAL).ler_tudo()
        assert "Poder público" in set(quadro["DscClasseConsumoMercado"])

    def test_nao_descarta_nem_avisa(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL)
        leitor.ler_tudo()
        assert leitor.relatorio.linhas_descartadas == 0
        assert leitor.relatorio.exemplos_descartadas == ()


class TestBlocos:
    def test_respeita_o_tamanho_do_bloco(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL, tamanho_bloco=10)
        tamanhos = [len(b) for b in leitor.blocos()]
        assert tamanhos == [10, 10, 4]
        assert leitor.relatorio.blocos == 3

    def test_soma_dos_blocos_bate_com_o_total(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL, tamanho_bloco=7)
        assert sum(len(b) for b in leitor.blocos()) == LINHAS_REAIS
        assert leitor.relatorio.linhas_lidas == LINHAS_REAIS

    def test_cada_bloco_e_um_dataframe_com_todas_as_colunas(self) -> None:
        for bloco in LeitorCsv(FIXTURE_REAL, tamanho_bloco=10).blocos():
            assert isinstance(bloco, pd.DataFrame)
            assert tuple(bloco.columns) == ESQUEMA_SAMP.nomes

    def test_arquivo_so_com_cabecalho_nao_gera_bloco(self, escrever_csv: CabecalhoEscritor) -> None:
        caminho = escrever_csv(_linha(*ESQUEMA_SAMP.nomes))
        leitor = LeitorCsv(caminho)
        assert list(leitor.blocos()) == []
        assert leitor.relatorio.linhas_lidas == 0

    def test_ler_tudo_sem_dados_devolve_quadro_vazio_com_as_colunas(
        self, escrever_csv: CabecalhoEscritor
    ) -> None:
        caminho = escrever_csv(_linha(*ESQUEMA_SAMP.nomes))
        quadro = LeitorCsv(caminho).ler_tudo()
        assert quadro.empty
        assert tuple(quadro.columns) == ESQUEMA_SAMP.nomes

    def test_relatorio_zerado_antes_de_ler(self, csv_valido: Path) -> None:
        leitor = LeitorCsv(csv_valido)
        assert leitor.relatorio.linhas_lidas == 0
        assert leitor.relatorio.blocos == 0


class TestCabecalho:
    def test_divergente_aborta_por_padrao(self, escrever_csv: CabecalhoEscritor) -> None:
        # docs/04 §10: mudança de layout na origem aborta com mensagem clara.
        caminho = escrever_csv(_linha("A", "B"), _linha("1", "2"))
        leitor = LeitorCsv(caminho)
        with pytest.raises(CabecalhoInvalidoError) as erro:
            leitor.ler_tudo()
        assert "VlrMercado" in str(erro.value)
        assert erro.value.resultado.inesperadas == ("A", "B")

    def test_pode_prosseguir_sem_estrito(self, escrever_csv: CabecalhoEscritor) -> None:
        caminho = escrever_csv(_linha("A", "B"), _linha("1", "2"))
        leitor = LeitorCsv(caminho, estrito=False)
        quadro = leitor.ler_tudo()
        assert tuple(quadro.columns) == ("A", "B")
        assert leitor.relatorio.cabecalho is not None
        assert not leitor.relatorio.cabecalho.conforme

    def test_ordem_trocada_e_divergencia(self, escrever_csv: CabecalhoEscritor) -> None:
        invertido = list(reversed(ESQUEMA_SAMP.nomes))
        caminho = escrever_csv(_linha(*invertido))
        with pytest.raises(CabecalhoInvalidoError, match="ordem"):
            LeitorCsv(caminho).cabecalho()

    def test_arquivo_vazio(self, tmp_path: Path) -> None:
        caminho = tmp_path / "vazio.csv"
        caminho.write_bytes(b"")
        with pytest.raises(ArquivoVazioError, match="vazio"):
            LeitorCsv(caminho).cabecalho()

    def test_cabecalho_e_lido_uma_vez_so(self, csv_valido: Path) -> None:
        leitor = LeitorCsv(csv_valido)
        assert leitor.cabecalho() is leitor.cabecalho()


class TestLinhasDescartadas:
    def test_linha_longa_e_contabilizada(self, escrever_csv: CabecalhoEscritor) -> None:
        # O pandas descarta a linha e só emite um ParserWarning — é o descarte silencioso
        # que docs/04 §7 proíbe.
        caminho = escrever_csv(
            _linha(*ESQUEMA_SAMP.nomes),
            _linha(*["ok"] * 18),
            _linha(*["demais"] * 21),
            _linha(*["ok"] * 18),
        )
        leitor = LeitorCsv(caminho)
        assert len(leitor.ler_tudo()) == 2
        assert leitor.relatorio.linhas_descartadas == 1

        (exemplo,) = leitor.relatorio.exemplos_descartadas
        assert exemplo.numero == 3  # 1 = cabeçalho
        assert exemplo.campos_encontrados == 21
        assert exemplo.campos_esperados == 18
        assert "demais" in exemplo.trecho

    def test_linha_curta_nao_e_descartada(self, escrever_csv: CabecalhoEscritor) -> None:
        # O pandas preenche o fim com vazio; a linha chega ao Parquet e vira achado de
        # completude (DQ-COM-001/002), não de layout.
        caminho = escrever_csv(
            _linha(*ESQUEMA_SAMP.nomes),
            _linha(*["ok"] * 16),
        )
        leitor = LeitorCsv(caminho)
        quadro = leitor.ler_tudo()
        assert len(quadro) == 1
        assert leitor.relatorio.linhas_descartadas == 0
        assert quadro["DatCompetencia"].iloc[0] == ""
        assert quadro["VlrMercado"].iloc[0] == ""

    def test_guarda_no_maximo_cinco_exemplos(self, escrever_csv: CabecalhoEscritor) -> None:
        ruins = [_linha(*["x"] * 20) for _ in range(9)]
        caminho = escrever_csv(_linha(*ESQUEMA_SAMP.nomes), _linha(*["ok"] * 18), *ruins)
        leitor = LeitorCsv(caminho)
        leitor.ler_tudo()
        assert leitor.relatorio.linhas_descartadas == 9
        assert len(leitor.relatorio.exemplos_descartadas) == MAX_EXEMPLOS

    def test_conta_descartes_espalhados_por_varios_blocos(
        self, escrever_csv: CabecalhoEscritor
    ) -> None:
        linhas = [_linha(*["ok"] * 18) for _ in range(20)]
        linhas[3] = linhas[15] = _linha(*["x"] * 19)
        caminho = escrever_csv(_linha(*ESQUEMA_SAMP.nomes), *linhas)
        leitor = LeitorCsv(caminho, tamanho_bloco=5)
        assert sum(len(b) for b in leitor.blocos()) == 18
        assert leitor.relatorio.linhas_descartadas == 2
        assert [e.numero for e in leitor.relatorio.exemplos_descartadas] == [5, 17]

    def test_trecho_e_truncado(self, escrever_csv: CabecalhoEscritor) -> None:
        caminho = escrever_csv(
            _linha(*ESQUEMA_SAMP.nomes),
            _linha(*["ok"] * 18),
            _linha(*["z" * 100] * 20),
        )
        leitor = LeitorCsv(caminho)
        leitor.ler_tudo()
        (exemplo,) = leitor.relatorio.exemplos_descartadas
        assert len(exemplo.trecho) <= 203
        assert exemplo.trecho.endswith("...")


class TestLarguraInconsistente:
    """Quando a linha larga é a primeira de dados, o parser trunca em vez de descartar."""

    def test_primeira_linha_larga_aborta(self, escrever_csv: CabecalhoEscritor) -> None:
        # O pandas não informa a posição neste caso, então a perda não é rastreável —
        # contabilizá-la como "1 linha" seria mentir sobre o que se sabe.
        caminho = escrever_csv(_linha(*ESQUEMA_SAMP.nomes), _linha(*["x"] * 20))
        with pytest.raises(EstruturaInconsistenteError, match="truncada"):
            LeitorCsv(caminho).ler_tudo()

    def test_sem_estrito_registra_e_prossegue(self, escrever_csv: CabecalhoEscritor) -> None:
        caminho = escrever_csv(
            _linha(*ESQUEMA_SAMP.nomes), _linha(*["x"] * 20), _linha(*["ok"] * 18)
        )
        leitor = LeitorCsv(caminho, estrito=False)
        quadro = leitor.ler_tudo()
        assert leitor.relatorio.truncamento_de_largura
        assert "posição desconhecida" in leitor.relatorio.resumo()
        assert len(quadro) == 2

    def test_arquivo_intacto_nao_marca_truncamento(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL)
        leitor.ler_tudo()
        assert not leitor.relatorio.truncamento_de_largura

    def test_colunas_nao_se_deslocam(self, escrever_csv: CabecalhoEscritor) -> None:
        # Sem `names` explícito o pandas trataria os campos extras como índice e todo o
        # arquivo sairia com as colunas andadas uma casa — corrupção silenciosa.
        caminho = escrever_csv(
            _linha(*ESQUEMA_SAMP.nomes),
            _linha(*[f"c{i}" for i in range(20)]),
        )
        quadro = LeitorCsv(caminho, estrito=False).ler_tudo()
        assert quadro["DatGeracaoConjuntoDados"].iloc[0] == "c0"
        assert quadro["NumCNPJAgenteDistribuidora"].iloc[0] == "c1"


class TestAvisosAlheios:
    def test_aviso_que_nao_e_do_parser_e_reemitido(
        self, csv_valido: Path, recwarn: pytest.WarningsRecorder
    ) -> None:
        """A captura existe para ler os avisos do parser; os demais têm de sobreviver a ela.

        Chamada direta ao método interno porque provocar um `DeprecationWarning` real de dentro
        do `read_csv` dependeria da versão do pandas.
        """
        leitor = LeitorCsv(csv_valido)
        alheio = warnings.WarningMessage(
            message=DeprecationWarning("algo do pandas vai sumir"),
            category=DeprecationWarning,
            filename=__file__,
            lineno=1,
        )
        leitor._registrar_descartes([alheio])

        assert leitor.relatorio.linhas_descartadas == 0
        assert any("algo do pandas vai sumir" in str(a.message) for a in recwarn)


class TestEncoding:
    def test_byte_invalido_em_cp1252_e_erro_claro(self, tmp_path: Path) -> None:
        # Base da DQ-VAL-015: cp1252 deixa 5 bytes indefinidos; latin-1 aceitaria qualquer um.
        caminho = tmp_path / "ruim.csv"
        cabecalho = _linha(*ESQUEMA_SAMP.nomes).encode(ENCODING_ORIGEM)
        corpo = _linha(*["ok"] * 18).encode(ENCODING_ORIGEM).replace(b"ok", b"\x81k", 1)
        caminho.write_bytes(cabecalho + b"\r\n" + corpo + b"\r\n")
        with pytest.raises(EncodingInvalidoError) as erro:
            LeitorCsv(caminho).ler_tudo()
        assert erro.value.encoding == ENCODING_ORIGEM
        assert "0x81" in str(erro.value)

    def test_utf8_e_aceito_quando_declarado(self, tmp_path: Path) -> None:
        caminho = tmp_path / "utf8.csv"
        linhas = [_linha(*ESQUEMA_SAMP.nomes), _linha(*["ação"] * 18)]
        caminho.write_text("\r\n".join(linhas) + "\r\n", encoding="utf-8")
        quadro = LeitorCsv(caminho, encoding="utf-8").ler_tudo()
        assert quadro["SigAgenteDistribuidora"].iloc[0] == "ação"


class TestRelatorio:
    def test_descreve_a_leitura(self) -> None:
        leitor = LeitorCsv(FIXTURE_REAL, tamanho_bloco=10)
        leitor.ler_tudo()
        resumo = leitor.relatorio.resumo()
        assert "24 linhas" in resumo
        assert "3 blocos" in resumo

    def test_menciona_os_descartes(self, escrever_csv: CabecalhoEscritor) -> None:
        caminho = escrever_csv(
            _linha(*ESQUEMA_SAMP.nomes), _linha(*["ok"] * 18), _linha(*["x"] * 20)
        )
        leitor = LeitorCsv(caminho)
        leitor.ler_tudo()
        assert "1 linha descartada" in leitor.relatorio.resumo()

    def test_registra_caminho_e_encoding(self, csv_valido: Path) -> None:
        relatorio = LeitorCsv(csv_valido).relatorio
        assert relatorio.caminho == csv_valido
        assert relatorio.encoding == ENCODING_ORIGEM
