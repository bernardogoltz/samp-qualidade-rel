"""Escrita do Parquet tipado e idempotência (etapa 4 de docs/04)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from samp_dq.errors import EscritaError
from samp_dq.ingest.leitura import LeitorCsv
from samp_dq.ingest.normalizacao import Normalizador
from samp_dq.ingest.parquet import (
    COMPRESSAO_PADRAO,
    StatusEscrita,
    chave_do_insumo,
    escrever_parquet,
    esquema_arrow,
)
from samp_dq.ingest.schema import ESQUEMA_SAMP

FIXTURE_REAL = Path(__file__).parent.parent / "fixtures" / "samp-real-amostra.csv"


def blocos_reais() -> Iterator[pd.DataFrame]:
    leitor = LeitorCsv(FIXTURE_REAL)
    yield from Normalizador().normalizar_blocos(leitor.blocos())


@pytest.fixture
def destino(tmp_path: Path) -> Path:
    return tmp_path / "samp-2024.parquet"


class TestEsquemaArrow:
    def test_tipos_seguem_o_contrato(self) -> None:
        esquema = esquema_arrow()
        assert esquema.field("VlrMercado").type == pa.decimal128(20, 6)
        assert esquema.field("DatCompetencia").type == pa.date32()
        assert esquema.field("IdeNucleoCeg").type == pa.int64()
        assert esquema.field("NumCNPJAgenteDistribuidora").type == pa.string()

    def test_inclui_as_colunas_raw_no_fim(self) -> None:
        nomes = esquema_arrow().names
        assert tuple(nomes[:18]) == ESQUEMA_SAMP.nomes
        assert nomes[18:] == ["NumCNPJAgenteAcessante_raw", "VlrMercado_raw"]

    def test_pode_dispensar_as_raw(self) -> None:
        assert esquema_arrow(com_raw=False).names == list(ESQUEMA_SAMP.nomes)


class TestEscrita:
    def test_grava_e_relata(self, destino: Path) -> None:
        resultado = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert resultado.status is StatusEscrita.GRAVADO
        assert resultado.linhas == 24
        assert resultado.tamanho == destino.stat().st_size
        assert resultado.chave == "abc"

    def test_conteudo_tipado(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        tabela = pq.read_table(destino)
        assert tabela.num_rows == 24
        assert tabela.column("VlrMercado")[0].as_py() == Decimal("75313.000000")
        assert tabela.column("DatCompetencia")[0].as_py() == date(2024, 1, 1)
        assert tabela.schema.field("VlrMercado").type == pa.decimal128(20, 6)

    def test_preserva_o_texto_original(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        tabela = pq.read_table(destino)
        assert tabela.column("NumCNPJAgenteAcessante_raw")[0].as_py() == "37121669900   "
        assert tabela.column("NumCNPJAgenteAcessante")[0].as_py() == "37121669900"

    def test_um_grupo_de_linhas_por_bloco(self, tmp_path: Path) -> None:
        leitor = LeitorCsv(FIXTURE_REAL, tamanho_bloco=10)
        destino = tmp_path / "s.parquet"
        escrever_parquet(Normalizador().normalizar_blocos(leitor.blocos()), destino, chave="k")
        assert pq.ParquetFile(destino).num_row_groups == 3

    def test_cria_a_pasta_de_destino(self, tmp_path: Path) -> None:
        destino = tmp_path / "preprocessado" / "2024" / "samp-2024.parquet"
        escrever_parquet(blocos_reais(), destino, chave="abc")
        assert destino.exists()

    def test_comprime(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        metadados = pq.ParquetFile(destino).metadata.row_group(0).column(0)
        assert metadados.compression.lower() == COMPRESSAO_PADRAO

    def test_sem_blocos_grava_arquivo_vazio_com_o_esquema(self, destino: Path) -> None:
        resultado = escrever_parquet(iter([]), destino, chave="abc")
        assert resultado.linhas == 0
        tabela = pq.read_table(destino)
        assert tabela.num_rows == 0
        assert tuple(tabela.schema.names[:18]) == ESQUEMA_SAMP.nomes


class TestIdempotencia:
    def test_segunda_execucao_reaproveita(self, destino: Path) -> None:
        primeiro = escrever_parquet(blocos_reais(), destino, chave="abc")
        segundo = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert primeiro.status is StatusEscrita.GRAVADO
        assert segundo.status is StatusEscrita.EM_CACHE
        assert segundo.linhas == 24

    def test_cache_nao_consome_os_blocos(self, destino: Path) -> None:
        # O ganho da idempotência é justamente não reler os 369 MB do CSV.
        escrever_parquet(blocos_reais(), destino, chave="abc")

        consumidos = 0

        def contando() -> Iterator[pd.DataFrame]:
            nonlocal consumidos
            for bloco in blocos_reais():
                consumidos += 1
                yield bloco

        escrever_parquet(contando(), destino, chave="abc")
        assert consumidos == 0

    def test_insumo_diferente_regrava(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        segundo = escrever_parquet(blocos_reais(), destino, chave="OUTRA")
        assert segundo.status is StatusEscrita.GRAVADO

    def test_forcar_ignora_o_cache(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        segundo = escrever_parquet(blocos_reais(), destino, chave="abc", forcar=True)
        assert segundo.status is StatusEscrita.GRAVADO

    def test_parquet_apagado_invalida_o_cache(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        destino.unlink()
        segundo = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert segundo.status is StatusEscrita.GRAVADO

    def test_parquet_mexido_a_mao_invalida_o_cache(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        destino.write_bytes(b"nao sou um parquet")
        segundo = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert segundo.status is StatusEscrita.GRAVADO

    def test_sidecar_corrompido_invalida_o_cache(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        _sidecar(destino).write_text("{ nao e json")
        segundo = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert segundo.status is StatusEscrita.GRAVADO

    def test_resumo_distingue_gravado_de_reaproveitado(self, destino: Path) -> None:
        primeiro = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert not primeiro.reaproveitado
        assert "gravado — 24 linhas" in primeiro.resumo()

        segundo = escrever_parquet(blocos_reais(), destino, chave="abc")
        assert segundo.reaproveitado
        assert "reaproveitado — 24 linhas" in segundo.resumo()

    def test_sidecar_registra_o_que_e_preciso_para_auditar(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        registro = json.loads(_sidecar(destino).read_text())
        assert registro["chave"] == "abc"
        assert registro["linhas"] == 24
        assert registro["tamanho"] == destino.stat().st_size
        assert registro["compressao"] == COMPRESSAO_PADRAO
        assert registro["ferramenta"] == "samp-dq"
        assert registro["gravado_em"]


class TestFalhaNaoDeixaRastro:
    """docs/04 §10: nunca gravar resultado parcial."""

    def test_erro_no_meio_nao_deixa_arquivo(self, destino: Path) -> None:
        def explode() -> Iterator[pd.DataFrame]:
            yield from blocos_reais()
            raise RuntimeError("falha no meio da escrita")

        with pytest.raises(RuntimeError, match="falha no meio"):
            escrever_parquet(explode(), destino, chave="abc")
        assert not destino.exists()
        assert not _sidecar(destino).exists()
        assert list(destino.parent.glob("*.part")) == []

    def test_erro_nao_destroi_o_parquet_anterior(self, destino: Path) -> None:
        escrever_parquet(blocos_reais(), destino, chave="abc")
        anterior = destino.read_bytes()

        def explode() -> Iterator[pd.DataFrame]:
            yield from blocos_reais()
            raise RuntimeError("falha")

        with pytest.raises(RuntimeError):
            escrever_parquet(explode(), destino, chave="OUTRA")
        assert destino.read_bytes() == anterior

    def test_bloco_fora_do_esquema_e_erro_claro(self, destino: Path) -> None:
        ruim = pd.DataFrame({"coluna_estranha": ["x"]})
        with pytest.raises(EscritaError, match="não corresponde ao esquema"):
            escrever_parquet(iter([ruim]), destino, chave="abc")
        assert not destino.exists()


class TestChaveDoInsumo:
    def test_usa_o_sha256_do_sidecar_do_download(self, tmp_path: Path) -> None:
        # Reaproveitar o hash já calculado evita reler 369 MB só para decidir se pode pular.
        csv = tmp_path / "samp-2024.csv"
        csv.write_bytes(b"conteudo")
        csv.with_name(csv.name + ".samp-dq.json").write_text(json.dumps({"sha256": "cafe"}))
        assert chave_do_insumo(csv) == "cafe"

    def test_calcula_quando_nao_ha_sidecar(self, tmp_path: Path) -> None:
        csv = tmp_path / "samp-2024.csv"
        csv.write_bytes(b"conteudo")
        chave = chave_do_insumo(csv)
        assert len(chave) == 64
        assert chave == chave_do_insumo(csv)

    def test_conteudo_diferente_muda_a_chave(self, tmp_path: Path) -> None:
        um = tmp_path / "a.csv"
        outro = tmp_path / "b.csv"
        um.write_bytes(b"conteudo")
        outro.write_bytes(b"outro conteudo")
        assert chave_do_insumo(um) != chave_do_insumo(outro)

    def test_sidecar_sem_sha256_cai_no_calculo(self, tmp_path: Path) -> None:
        csv = tmp_path / "samp-2024.csv"
        csv.write_bytes(b"conteudo")
        csv.with_name(csv.name + ".samp-dq.json").write_text(json.dumps({"nome": "x"}))
        assert len(chave_do_insumo(csv)) == 64


def _sidecar(destino: Path) -> Path:
    return destino.with_name(destino.name + ".samp-dq.json")
