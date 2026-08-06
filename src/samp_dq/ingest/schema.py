"""Contrato das colunas do SAMP — a referência única de nome, ordem e tipo.

O dicionário de dados v1.1 descreve 19 campos, mas o arquivo publicado tem 18: `DscClassificacao`
não aparece no cabeçalho real. Onde dicionário e arquivo divergem, **manda o arquivo** — é o dado
que se vai ler. As divergências ficam registradas (`nome_dicionario`, `CAMPOS_SO_NO_DICIONARIO`)
porque são elas próprias um achado de qualidade, a ser reportado à ANEEL.

Este módulo é deliberadamente sem dependências: a leitura (Polars) mapeia estes tipos para os dela,
e não o contrário. Assim o contrato continua testável sem engine de dados.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum

from samp_dq.errors import CampoDesconhecidoError

#: Separador de campos do CSV publicado (campos entre aspas duplas).
SEPARADOR = ";"

#: Encoding de origem. O dicionário diz "Latin-1", mas o arquivo usa a extensão Windows-1252
#: (aspas curvas, travessão). Decodificar como cp1252 é mais estrito: latin-1 aceita qualquer
#: byte, o que faria a DQ-VAL-015 nunca falhar.
ENCODING_ORIGEM = "cp1252"

#: Previsto no dicionário v1.1, ausente do arquivo real (achado 5 de docs/03).
CAMPOS_SO_NO_DICIONARIO = ("DscClassificacao",)


class TipoCampo(StrEnum):
    """Tipo alvo do campo depois da normalização."""

    TEXTO = "texto"
    INTEIRO = "inteiro"
    DECIMAL = "decimal"
    DATA = "data"


@dataclass(frozen=True, slots=True)
class Campo:
    """Uma coluna do arquivo, com o que a validação precisa saber sobre ela."""

    nome: str
    tipo: TipoCampo
    descricao: str = ""
    tamanho_max: int | None = None
    precisao: int | None = None
    escala: int | None = None
    #: Campo de domínio: os valores distintos são catalogados em `dominios-observados-{ano}.json`.
    dominio: bool = False
    #: Grafia do dicionário, quando difere da do arquivo.
    nome_dicionario: str = ""

    @property
    def diverge_do_dicionario(self) -> bool:
        return bool(self.nome_dicionario) and self.nome_dicionario != self.nome


@dataclass(frozen=True, slots=True)
class ResultadoCabecalho:
    """Comparação entre o cabeçalho lido e o esperado — insumo da DQ-VAL-001."""

    colunas: tuple[str, ...]
    faltantes: tuple[str, ...]
    inesperadas: tuple[str, ...]
    ordem_divergente: bool

    @property
    def conforme(self) -> bool:
        return not self.faltantes and not self.inesperadas and not self.ordem_divergente

    def resumo(self) -> str:
        """Uma linha explicando o veredito, para log, CLI e `resultado-{ano}.json`."""
        if self.conforme:
            return f"cabeçalho conforme: {len(self.colunas)} colunas na ordem esperada"
        partes = []
        if self.faltantes:
            partes.append(f"faltando {', '.join(self.faltantes)}")
        if self.inesperadas:
            partes.append(f"inesperadas {', '.join(self.inesperadas)}")
        if self.ordem_divergente:
            partes.append("colunas fora da ordem esperada")
        return f"cabeçalho divergente: {'; '.join(partes)}"


@dataclass(frozen=True, slots=True)
class Esquema:
    """As colunas do arquivo, na ordem em que aparecem nele."""

    campos: tuple[Campo, ...]

    def __len__(self) -> int:
        return len(self.campos)

    def __iter__(self) -> Iterator[Campo]:
        return iter(self.campos)

    def __getitem__(self, posicao: int) -> Campo:
        return self.campos[posicao]

    @property
    def nomes(self) -> tuple[str, ...]:
        return tuple(c.nome for c in self.campos)

    @property
    def campos_de_dominio(self) -> tuple[str, ...]:
        return tuple(c.nome for c in self.campos if c.dominio)

    def campo(self, nome: str) -> Campo:
        for campo in self.campos:
            if campo.nome == nome:
                return campo
        raise CampoDesconhecidoError(nome, self.nomes)

    def validar_cabecalho(self, colunas: Sequence[str]) -> ResultadoCabecalho:
        """Compara o cabeçalho lido com o contrato, sem levantar exceção.

        Divergência de cabeçalho é achado de qualidade, não erro de programa: quem chama decide
        se aborta (mudança de layout na origem) ou apenas reporta.
        """
        lidas = tuple(colunas)
        esperadas = self.nomes
        faltantes = tuple(n for n in esperadas if n not in lidas)
        inesperadas = tuple(n for n in lidas if n not in esperadas)
        # A ordem só é comparável sobre as colunas que os dois lados têm; do contrário uma
        # coluna a menos já apareceria como "fora de ordem".
        comuns_lidas = [n for n in lidas if n in esperadas]
        comuns_esperadas = [n for n in esperadas if n in lidas]
        return ResultadoCabecalho(
            colunas=lidas,
            faltantes=faltantes,
            inesperadas=inesperadas,
            ordem_divergente=comuns_lidas != comuns_esperadas,
        )


def dividir_cabecalho(linha: str) -> tuple[str, ...]:
    """Extrai os nomes de coluna da primeira linha do CSV.

    Não é um parser de CSV: nomes de coluna não contêm `;` nem aspas escapadas, então basta
    remover BOM, aspas e espaço de preenchimento.
    """
    limpa = linha.lstrip("﻿").strip()
    if not limpa:
        return ()
    return tuple(parte.strip().strip('"').strip() for parte in limpa.split(SEPARADOR))


# Tamanhos máximos conforme o dicionário v1.1 (base da DQ-VAL-012).
_TXT = TipoCampo.TEXTO

ESQUEMA_SAMP = Esquema(
    (
        Campo(
            "DatGeracaoConjuntoDados",
            TipoCampo.DATA,
            "Data em que a ANEEL gerou a publicação; constante dentro de um mesmo arquivo.",
        ),
        Campo(
            "NumCNPJAgenteDistribuidora",
            _TXT,
            "CNPJ da distribuidora. Texto, não número: como inteiro perderia zeros à esquerda.",
            tamanho_max=14,
        ),
        Campo(
            "SigAgenteDistribuidora",
            _TXT,
            "Sigla da distribuidora (ex.: COCEL).",
            tamanho_max=20,
        ),
        Campo(
            "NomAgenteDistribuidora",
            _TXT,
            "Razão social da distribuidora.",
            tamanho_max=400,
        ),
        Campo(
            "NomTipoMercado",
            _TXT,
            "Tipo de mercado. O arquivo traz 'Regular' e 'Sistema de Compensação GD I/II', "
            "fora do dicionário v1.1.",
            tamanho_max=400,
            dominio=True,
        ),
        Campo(
            "DscModalidadeTarifaria",
            _TXT,
            "Modalidade tarifária. O arquivo grafa 'Azul'/'Verde', não 'Horária Azul'/'Verde'.",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "DscSubGrupoTarifario",
            _TXT,
            "Subgrupo tarifário (A1..A4, AS, B1..B4).",
            tamanho_max=10,
            dominio=True,
        ),
        Campo(
            "DscClasseConsumoMercado",
            _TXT,
            "Classe de consumo (Residencial, Industrial, Poder público, ...).",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "DscSubClasseConsumidor",
            _TXT,
            "Subclasse do consumidor; 'Não se aplica' quando não detalhada.",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "DscDetalheConsumidor",
            _TXT,
            "Detalhamento adicional do consumidor.",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "IdeNucleoCeg",
            TipoCampo.INTEIRO,
            "Núcleo do código CEG da usina; 0 quando o registro não é de geração.",
            tamanho_max=5,
            nome_dicionario="IdeNucleoCEG",
        ),
        Campo(
            "NumCNPJAgenteAcessante",
            _TXT,
            "CNPJ do acessante. Observado com 11 dígitos (CPF) e espaços à direita.",
            tamanho_max=14,
        ),
        Campo(
            "NomAgenteAcessante",
            _TXT,
            "Razão social do acessante; 'Não se aplica' no mercado cativo.",
            tamanho_max=400,
        ),
        Campo(
            "DscPostoTarifario",
            _TXT,
            "Posto tarifário (Ponta, Fora ponta, Intermediário, Não se aplica).",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "DscOpcaoEnergia",
            _TXT,
            "Opção de energia. Observado em caixa alta ('CATIVO'); comparar sem distinguir caixa.",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "DscDetalheMercado",
            _TXT,
            "Grandeza medida (ex.: 'Energia TUSD (kWh)'); define a unidade de VlrMercado.",
            tamanho_max=100,
            dominio=True,
        ),
        Campo(
            "DatCompetencia",
            TipoCampo.DATA,
            "Mês de competência, sempre no primeiro dia do mês.",
        ),
        Campo(
            "VlrMercado",
            TipoCampo.DECIMAL,
            "Valor da grandeza em DscDetalheMercado. Vem com vírgula decimal no CSV.",
            precisao=20,
            escala=6,
        ),
    )
)

__all__ = [
    "CAMPOS_SO_NO_DICIONARIO",
    "ENCODING_ORIGEM",
    "ESQUEMA_SAMP",
    "SEPARADOR",
    "Campo",
    "Esquema",
    "ResultadoCabecalho",
    "TipoCampo",
    "dividir_cabecalho",
]
