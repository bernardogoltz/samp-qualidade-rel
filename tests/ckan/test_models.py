"""Parsing dos metadados do CKAN em objetos de domínio."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from samp_dq.ckan import Dataset, Formato, Recurso, RecursoNaoEncontradoError


class TestRecurso:
    def test_le_os_campos_do_ckan(self, package_show_json: dict[str, Any]) -> None:
        bruto = next(
            r for r in package_show_json["result"]["resources"] if r["name"] == "samp-2024.csv"
        )

        recurso = Recurso.de_ckan(bruto)

        assert recurso.id == bruto["id"]
        assert recurso.nome == "samp-2024.csv"
        assert recurso.formato == Formato.CSV
        assert recurso.url.endswith("/download/samp-2024.csv")
        assert recurso.tamanho == 369234945

    def test_normaliza_o_formato_para_maiusculas(self) -> None:
        recurso = Recurso.de_ckan({"id": "x", "name": "samp-2024.csv", "format": "csv", "url": "u"})

        assert recurso.formato == Formato.CSV

    def test_formato_desconhecido_vira_outro_sem_quebrar(self) -> None:
        recurso = Recurso.de_ckan({"id": "x", "name": "leia-me.txt", "format": "TXT", "url": "u"})

        assert recurso.formato == Formato.OUTRO
        assert recurso.formato_bruto == "TXT"

    def test_deriva_o_ano_do_nome_do_arquivo(self, package_show_json: dict[str, Any]) -> None:
        recursos = [Recurso.de_ckan(r) for r in package_show_json["result"]["resources"]]

        anos = {r.nome: r.ano for r in recursos}
        assert anos["samp-2024.csv"] == 2024
        assert anos["samp-2003.parquet"] == 2003
        # O dicionário de dados não é um arquivo anual.
        assert anos["Dicionário de dados"] is None

    def test_ultima_modificacao_do_ckan_e_naive_e_assume_utc(self) -> None:
        recurso = Recurso.de_ckan(
            {
                "id": "x",
                "name": "samp-2024.csv",
                "format": "CSV",
                "url": "u",
                "last_modified": "2026-07-28T15:53:26.685626",
            }
        )

        assert recurso.ultima_modificacao == datetime(2026, 7, 28, 15, 53, 26, 685626, tzinfo=UTC)

    def test_cai_para_created_quando_nunca_foi_modificado(self) -> None:
        recurso = Recurso.de_ckan(
            {
                "id": "x",
                "name": "samp-2024.csv",
                "format": "CSV",
                "url": "u",
                "last_modified": None,
                "created": "2022-08-18T20:15:13.788810",
            }
        )

        assert recurso.ultima_modificacao == datetime(2022, 8, 18, 20, 15, 13, 788810, tzinfo=UTC)

    def test_sem_datas_a_ultima_modificacao_e_nula(self) -> None:
        recurso = Recurso.de_ckan({"id": "x", "name": "a.csv", "format": "CSV", "url": "u"})

        assert recurso.ultima_modificacao is None

    def test_nome_de_arquivo_e_saneado_para_uso_no_disco(self) -> None:
        recurso = Recurso.de_ckan(
            {
                "id": "x",
                "name": "Dicionário de dados",
                "format": "PDF",
                "url": "https://exemplo.gov.br/download/dd-samp.pdf",
            }
        )

        # O nome do recurso não serve como nome de arquivo; usa-se o final da URL.
        assert recurso.nome_arquivo == "dd-samp.pdf"

    def test_nome_de_arquivo_usa_o_nome_do_recurso_quando_ja_e_um_arquivo(self) -> None:
        recurso = Recurso.de_ckan(
            {"id": "x", "name": "samp-2024.csv", "format": "CSV", "url": "https://e/download/z"}
        )

        assert recurso.nome_arquivo == "samp-2024.csv"


class TestDataset:
    @pytest.fixture
    def dataset(self, package_show_json: dict[str, Any]) -> Dataset:
        return Dataset.de_ckan(package_show_json["result"])

    def test_le_a_identificacao_do_dataset(self, dataset: Dataset) -> None:
        assert dataset.nome == "samp"
        assert dataset.id == "3e153db4-a503-4093-88be-75d31b002dcf"
        assert dataset.licenca == "odc-odbl"
        assert "ODbL" in dataset.licenca_titulo

    def test_lista_todos_os_recursos(self, dataset: Dataset) -> None:
        assert len(dataset.recursos) == 7

    def test_filtra_por_formato(self, dataset: Dataset) -> None:
        csvs = dataset.filtrar(formato=Formato.CSV)

        assert [r.ano for r in csvs] == [2003, 2024, 2026]

    def test_filtra_por_ano(self, dataset: Dataset) -> None:
        do_ano = dataset.filtrar(ano=2024)

        assert {r.formato for r in do_ano} == {Formato.CSV, Formato.PARQUET}

    def test_filtra_por_varios_anos(self, dataset: Dataset) -> None:
        recursos = dataset.filtrar(ano=[2003, 2026], formato=Formato.CSV)

        assert [r.nome for r in recursos] == ["samp-2003.csv", "samp-2026.csv"]

    def test_aceita_formato_como_texto(self, dataset: Dataset) -> None:
        assert dataset.filtrar(formato="csv") == dataset.filtrar(formato=Formato.CSV)

    def test_recursos_saem_ordenados_por_ano(self, package_show_json: dict[str, Any]) -> None:
        embaralhado = dict(package_show_json["result"])
        embaralhado["resources"] = list(reversed(embaralhado["resources"]))

        anos = [r.ano for r in Dataset.de_ckan(embaralhado).filtrar(formato=Formato.CSV)]

        assert anos == [2003, 2024, 2026]

    def test_anos_disponiveis_ignora_recursos_sem_ano(self, dataset: Dataset) -> None:
        assert dataset.anos_disponiveis() == [2003, 2024, 2026]

    def test_recurso_devolve_exatamente_um(self, dataset: Dataset) -> None:
        recurso = dataset.recurso(ano=2024, formato=Formato.CSV)

        assert recurso.nome == "samp-2024.csv"

    def test_recurso_inexistente_falha_com_mensagem_util(self, dataset: Dataset) -> None:
        with pytest.raises(RecursoNaoEncontradoError) as erro:
            dataset.recurso(ano=1999, formato=Formato.CSV)

        assert "1999" in str(erro.value)
        assert "2003" in str(erro.value)  # sugere os anos disponíveis
