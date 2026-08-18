# ruff: noqa: RUF001
"""Metadados do catálogo (docs/03) e listas de domínio.

O dicionário v1.1 é a lista `dicionario`. O que o arquivo real publica e o dicionário ainda não
reconhece entra em `observados`: a regra não explode em falso positivo, mas o status
`aviso_defasagem` segue existindo para a ANEEL atualizar o dicionário.
"""

# Os valores de domínio copiam a grafia da ANEEL, inclusive o travessão.
from __future__ import annotations

from dataclasses import dataclass

DIMENSOES = (
    "completude",
    "conformidade",
    "unicidade",
    "consistencia",
    "acuracia",
    "atualidade",
)

PESO_SEVERIDADE = {"erro": 1.0, "aviso": 0.3, "info": 0.0}

_DIM_DO_PREFIXO = {
    "COM": "completude",
    "VAL": "conformidade",
    "UNI": "unicidade",
    "CON": "consistencia",
    "ACU": "acuracia",
    "ATU": "atualidade",
}


@dataclass(frozen=True, slots=True)
class SpecRegra:
    """Uma linha do catálogo: o que o JSON precisa para se explicar sozinho."""

    id: str
    severidade: str
    campos: str

    @property
    def dimensao(self) -> str:
        return _DIM_DO_PREFIXO[self.id.split("-")[1]]


REGRAS: tuple[SpecRegra, ...] = (
    SpecRegra("DQ-COM-001", "erro", "todos"),
    SpecRegra("DQ-COM-002", "erro", "VlrMercado"),
    SpecRegra("DQ-COM-003", "erro", "DatCompetencia"),
    SpecRegra("DQ-COM-004", "aviso", "DatCompetencia"),
    SpecRegra("DQ-COM-005", "aviso", "NumCNPJAgenteDistribuidora"),
    SpecRegra("DQ-VAL-001", "erro", "cabeçalho"),
    SpecRegra("DQ-VAL-002", "erro", "datas"),
    SpecRegra("DQ-VAL-003", "erro", "DscSubGrupoTarifario"),
    SpecRegra("DQ-VAL-004", "erro", "NomTipoMercado"),
    SpecRegra("DQ-VAL-005", "erro", "DscClasseConsumoMercado"),
    SpecRegra("DQ-VAL-006", "erro", "DscSubClasseConsumidor"),
    SpecRegra("DQ-VAL-007", "erro", "DscDetalheConsumidor"),
    SpecRegra("DQ-VAL-008", "erro", "DscPostoTarifario"),
    SpecRegra("DQ-VAL-009", "erro", "DscOpcaoEnergia"),
    SpecRegra("DQ-VAL-010", "erro", "NumCNPJAgenteDistribuidora"),
    SpecRegra("DQ-VAL-011", "aviso", "NumCNPJAgenteAcessante"),
    SpecRegra("DQ-VAL-012", "aviso", "strings"),
    SpecRegra("DQ-VAL-013", "erro", "IdeNucleoCeg"),
    SpecRegra("DQ-VAL-014", "erro", "VlrMercado"),
    SpecRegra("DQ-VAL-015", "erro", "arquivo"),
    SpecRegra("DQ-VAL-016", "aviso", "DscModalidadeTarifaria"),
    SpecRegra("DQ-UNI-001", "erro", "todos"),
    SpecRegra("DQ-UNI-002", "aviso", "chave composta"),
    SpecRegra("DQ-UNI-003", "aviso", "agente"),
    SpecRegra("DQ-CON-001", "erro", "DatCompetencia"),
    SpecRegra("DQ-CON-002", "aviso", "subgrupo, classe"),
    SpecRegra("DQ-CON-003", "aviso", "classe, subclasse"),
    SpecRegra("DQ-CON-004", "erro", "datas"),
    SpecRegra("DQ-CON-005", "aviso", "CNPJs"),
    SpecRegra("DQ-CON-006", "info", "CEG, opção"),
    SpecRegra("DQ-CON-007", "aviso", "posto, subgrupo, modalidade"),
    SpecRegra("DQ-CON-008", "info", "DatGeracaoConjuntoDados"),
    SpecRegra("DQ-ACU-001", "erro", "valor, detalhe"),
    SpecRegra("DQ-ACU-002", "aviso", "VlrMercado"),
    SpecRegra("DQ-ACU-003", "info", "VlrMercado"),
    SpecRegra("DQ-ACU-004", "info", "série temporal"),
    SpecRegra("DQ-ACU-005", "info", "série temporal"),
    SpecRegra("DQ-ATU-001", "aviso", "DatCompetencia"),
    SpecRegra("DQ-ATU-002", "aviso", "metadado CKAN"),
    SpecRegra("DQ-ATU-003", "info", "DatGeracaoConjuntoDados"),
)

CATALOGO: dict[str, SpecRegra] = {r.id: r for r in REGRAS}

CHAVE_ANALITICA: tuple[str, ...] = (
    "NumCNPJAgenteDistribuidora",
    "NomTipoMercado",
    "DscModalidadeTarifaria",
    "DscSubGrupoTarifario",
    "DscClasseConsumoMercado",
    "DscSubClasseConsumidor",
    "DscDetalheConsumidor",
    "IdeNucleoCeg",
    "NumCNPJAgenteAcessante",
    "DscPostoTarifario",
    "DscOpcaoEnergia",
    "DscDetalheMercado",
    "DatCompetencia",
)

SENTINELA = "Não se aplica"

#: {campo: (dicionario, observados)}. `observados` gera `aviso_defasagem`, não erro.
DOMINIOS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "DscSubGrupoTarifario": (
        frozenset({"A1", "A2", "A3", "A3a", "A4", "AS", "B1", "B2", "B3", "B4", SENTINELA}),
        frozenset(),
    ),
    "NomTipoMercado": (
        frozenset({"Sistema Regular", "Sistema Isolado", "Sistema Individual"}),
        frozenset(
            {
                "Regular",
                "Refaturamento - Regular",
                "Sistema Isolado - Regular",
                "Sistema Isolado - Refaturamento",
                "Sistema Individual - Regular",
                "Sistema Individual - Refaturamento",
                "Sistema de Compensação GD I",
                "Sistema de Compensação GD II",
                "Sistema de Compensação GD III",
                "Sistema de Compensação GD I - Refaturamento",
                "Sistema de Compensação GD II - Refaturamento",
                "Sistema de Compensação GD III - Refaturamento",
                "RTE",
            }
        ),
    ),
    "DscClasseConsumoMercado": (
        frozenset(
            {
                "Comercial",
                "Consumo próprio",
                "Iluminação pública",
                "Industrial",
                SENTINELA,
                "Poder público",
                "Residencial",
                "Rural",
                "Serviço público",
            }
        ),
        frozenset({"Serviço Público"}),
    ),
    "DscSubClasseConsumidor": (
        frozenset(
            {
                SENTINELA,
                "Residencial",
                "Residencial baixa renda",
                "Agropecuária rural",
                "Aquicultura",
                "Água, esgoto e saneamento",
                "Iluminação pública – B4a",
                "Iluminação pública – B4b",
                "Tração elétrica",
                "Cooperativa de eletrificação rural",
                "Serviço público de irrigação rural",
            }
        ),
        frozenset(
            {
                "Residencial baixa renda – faixa 01",
                "Residencial baixa renda – faixa 02",
                "Residencial baixa renda – faixa 03",
                "Residencial baixa renda – faixa 04",
            }
        ),
    ),
    "DscDetalheConsumidor": (
        frozenset({SENTINELA, "Fonte incentivada", "Dupla contratação", "APE", "ERC"}),
        frozenset(
            {
                "IRRIG./AQUIC.",
                "IRRIG./AQUIC. PR",
                "<500",
                "TIPO 01",
                "TIPO 02",
                "Cooperativas autorizadas",
                "Cooperativas autorizadas PR",
            }
        ),
    ),
    "DscPostoTarifario": (
        frozenset({"Fora ponta", "Intermediário", SENTINELA, "Ponta"}),
        frozenset(),
    ),
    "DscOpcaoEnergia": (
        frozenset({"Cativo", "Livre", "Distribuição", "Geração", "Suprimento"}),
        frozenset(),
    ),
    "DscModalidadeTarifaria": (
        frozenset(
            {
                "Azul",
                "Verde",
                "Convencional",
                "Branca",
                "Pré-pagamento",
                "Geração",
                "Distribuição",
                SENTINELA,
            }
        ),
        frozenset({"Energia Fotovoltaica (Res. 083/2004)"}),
    ),
}

#: Subgrupo → classe esperada (DQ-CON-002).
CLASSE_DO_SUBGRUPO: dict[str, frozenset[str]] = {
    "B1": frozenset({"Residencial"}),
    "B2": frozenset({"Rural"}),
    "B4": frozenset({"Iluminação pública"}),
}

#: Trecho da subclasse → classe exigida (DQ-CON-003).
CLASSE_DA_SUBCLASSE: tuple[tuple[str, str], ...] = (
    ("Residencial baixa renda", "Residencial"),
    ("Água, esgoto e saneamento", "Serviço público"),
)

#: `DscDetalheMercado` que admitem estorno/compensação (DQ-ACU-002).
DETALHES_COM_ESTORNO: tuple[str, ...] = (
    "compensada",
    "debitada",
    "SCEE",
    "Injetada",
)

SUBGRUPOS_A = frozenset({"A1", "A2", "A3", "A3a", "A4", "AS"})
SUBGRUPOS_B = frozenset({"B1", "B2", "B3", "B4", "B"})
MODALIDADES_HORARIAS = frozenset({"Azul", "Verde", "Branca"})
POSTOS_HORARIOS = frozenset({"Ponta", "Fora ponta", "Intermediário"})


def spec(id_regra: str) -> SpecRegra:
    return CATALOGO[id_regra]


__all__ = [
    "CATALOGO",
    "CHAVE_ANALITICA",
    "CLASSE_DA_SUBCLASSE",
    "CLASSE_DO_SUBGRUPO",
    "DETALHES_COM_ESTORNO",
    "DIMENSOES",
    "DOMINIOS",
    "MODALIDADES_HORARIAS",
    "PESO_SEVERIDADE",
    "POSTOS_HORARIOS",
    "REGRAS",
    "SENTINELA",
    "SUBGRUPOS_A",
    "SUBGRUPOS_B",
    "SpecRegra",
    "spec",
]
