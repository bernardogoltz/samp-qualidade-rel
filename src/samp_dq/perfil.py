"""Perfilamento do arquivo normalizado — etapa 5 do pipeline de docs/04.

Percorre os blocos já tipados e acumula o retrato do arquivo: quantas linhas, o que falta em cada
campo, quantos valores distintos, quais competências apareceram e como `VlrMercado` se distribui.
Sai daqui o `perfil-{ano}.json` e o `dominios-observados-{ano}.json` — os dois artefatos que o
agente de perfilamento lê, e a base para calibrar as listas `observados` de docs/03.

O perfil **descreve, não julga**. Nenhum número daqui é violação: "8 valores distintos em
`DscPostoTarifario`" é fato; dizer que um deles está fora do domínio é trabalho do catálogo de
regras. Manter essa fronteira é o que permite reaproveitar o mesmo perfil quando o catálogo mudar.

Três decisões merecem explicação:

**Passa-fio, não segunda passada.** `perfilar_blocos` devolve cada bloco intacto depois de medi-lo,
para encaixar entre a normalização e a escrita do Parquet::

    escrever_parquet(
        perfilador.perfilar_blocos(normalizador.normalizar_blocos(leitor.blocos())),
        destino, chave=chave,
    )

O arquivo anual é lido uma vez só, que é o ganho central de docs/04 §1.

**Contagem exata, com teto.** Os valores distintos de cada campo são contados exatamente — é isso
que alimenta `dominios-observados`. Um campo com mais de `LIMITE_DISTINTOS` valores distintos
(`NomAgenteAcessante` num arquivo grande) tem a contagem abandonada e é marcado como truncado: a
cardinalidade vira um piso, não um número. Sem o teto, um arquivo corrompido faria o perfilamento
consumir memória sem limite justamente quando há mais motivo para desconfiar dele.

**Mediana por amostra.** Mediana exata exigiria guardar todos os valores. Guarda-se uma amostra
uniforme de `TAMANHO_AMOSTRA` valores (sorteio *bottom-k*, com semente fixa: mesmo arquivo, mesmo
perfil) e dela sai a mediana. Quando o arquivo cabe na amostra, ela é exata — e o JSON diz qual
dos dois casos ocorreu, em `medianaExata`. Mínimo, máximo e soma são sempre exatos, em `Decimal`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from samp_dq.artefatos import (
    NOME_DOMINIOS,
    NOME_PERFIL,
    caminho_artefato,
    escrever_json,
    ler_json,
)
from samp_dq.ckan.download import SUFIXO_SIDECAR
from samp_dq.ingest.leitura import RelatorioLeitura
from samp_dq.ingest.normalizacao import RelatorioNormalizacao
from samp_dq.ingest.schema import ESQUEMA_SAMP, Esquema

#: Teto de valores distintos contados por campo (ver o cabeçalho do módulo).
LIMITE_DISTINTOS = 200_000

#: Valores de `VlrMercado` guardados para estimar a mediana.
TAMANHO_AMOSTRA = 200_000

#: Semente do sorteio da amostra. Fixa de propósito: perfil de um mesmo arquivo tem de ser
#: reprodutível, senão a idempotência de docs/04 §10 vale só para o Parquet.
SEMENTE = 42

#: Linhas por lote ao perfilar um Parquet já gravado.
TAMANHO_LOTE_PARQUET = 250_000

#: Somar milhões de decimal(20,6) estoura a precisão declarada; 38 dígitos acomodam o total de
#: qualquer ano sem trocar exatidão por float.
_DECIMAL_SOMA = pa.decimal128(38, 6)

_ANO_NO_NOME = re.compile(r"(19|20)\d{2}")


def _como_array(serie: pd.Series[Any]) -> pa.Array:
    """A coluna como um `pa.Array` de um pedaço só, seja qual for o dtype do pandas.

    Passar pelo Arrow (em vez de calcular no pandas) mantém `VlrMercado` em decimal do começo ao
    fim: `min`, `max` e soma saem exatos, sem passeio por float.
    """
    tabela = pa.Table.from_pandas(serie.to_frame(name="v"), preserve_index=False)
    coluna = tabela.column("v")
    if coluna.num_chunks == 1:
        return coluna.chunk(0)
    if coluna.num_chunks == 0:
        return pa.array([], type=coluna.type)
    return pa.concat_arrays(list(coluna.iterchunks()))


def _chave(valor: Any) -> str:
    """Chave textual do valor no JSON — datas em ISO, o resto como veio."""
    return valor.isoformat() if isinstance(valor, date) else str(valor)


def _para_decimal(valor: Any) -> Decimal | None:
    """Traz o valor para `Decimal`, venha ele em decimal ou em ponto flutuante.

    O Parquet do samp-dq guarda `VlrMercado` em decimal(20,6) e nada se perde aqui. O Parquet
    publicado pela ANEEL guarda em `double` — nesse caso a exatidão já se perdeu na origem, e
    converter pelo texto ao menos não acrescenta erro novo.
    """
    if valor is None:
        return None
    return valor if isinstance(valor, Decimal) else Decimal(str(valor))


def _zero(tipo: pa.DataType) -> pa.Scalar:
    """O escalar zero no tipo da coluna, para comparar sem conversão implícita."""
    return pa.scalar(Decimal(0), tipo) if pa.types.is_decimal(tipo) else pa.scalar(0, tipo)


def _para_soma(valores: pa.Array) -> pa.Array:
    """Soma de decimal exige mais casas que o campo declara (ver `_DECIMAL_SOMA`)."""
    return pc.cast(valores, _DECIMAL_SOMA) if pa.types.is_decimal(valores.type) else valores


def _arredondar(valor: float | None) -> float | None:
    """Estimativas saem com a mesma escala do campo — 6 casas, sem cauda binária no JSON."""
    return None if valor is None else round(valor, 6)


def _texto_decimal(valor: Decimal | None) -> str | None:
    """Decimal vai para o JSON como texto.

    Como número, `float` truncaria os 20 dígitos que a normalização se deu ao trabalho de
    preservar. Quem consome reconstrói com `Decimal(valor)`; estimativas (média, mediana) seguem
    como número, porque estimativa não tem exatidão a proteger.
    """
    return None if valor is None else format(valor, "f")


@dataclass(slots=True)
class ContagemDeValores:
    """Quantas vezes cada valor distinto apareceu num campo, até o teto de distintos."""

    limite: int = LIMITE_DISTINTOS
    contagens: dict[str, int] = field(default_factory=dict)
    #: Passou do teto: `contagens` foi abandonada e a cardinalidade vira um piso.
    truncado: bool = False
    ocorrencias: int = 0

    def observar(self, valores: pa.Array) -> None:
        self.ocorrencias += len(valores) - valores.null_count
        if self.truncado:
            return
        for par in pc.value_counts(pc.drop_null(valores)).to_pylist():
            chave = _chave(par["values"])
            self.contagens[chave] = self.contagens.get(chave, 0) + int(par["counts"])
        if len(self.contagens) > self.limite:
            self.truncado = True
            self.contagens = {}

    @property
    def distintos(self) -> int:
        """Valores distintos; quando truncado, o teto — leia-se "pelo menos isso"."""
        return self.limite if self.truncado else len(self.contagens)

    def como_json(self) -> dict[str, int]:
        """Do mais frequente ao menos frequente; empate desfeito pelo valor, para dar ordem fixa."""
        return dict(sorted(self.contagens.items(), key=lambda par: (-par[1], par[0])))


@dataclass(slots=True)
class Amostra:
    """Amostra uniforme de tamanho fixo, colhida em fluxo.

    Sorteio *bottom-k*: cada valor recebe uma chave aleatória e ficam os `tamanho` de menor chave.
    Equivale a sortear sem reposição sobre o arquivo inteiro, mas custa uma passada e memória
    constante — e, ao contrário do algoritmo clássico de reservatório, resolve-se por bloco, sem
    laço em Python por linha.
    """

    tamanho: int = TAMANHO_AMOSTRA
    semente: int = SEMENTE
    vistos: int = 0
    _valores: np.ndarray[Any, Any] = field(init=False, repr=False)
    _chaves: np.ndarray[Any, Any] = field(init=False, repr=False)
    _sorteio: np.random.Generator = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._valores = np.empty(0, dtype="float64")
        self._chaves = np.empty(0, dtype="float64")
        self._sorteio = np.random.default_rng(self.semente)

    @property
    def integral(self) -> bool:
        """Todos os valores couberam: as estatísticas da amostra são as do arquivo."""
        return self.vistos <= self.tamanho

    def observar(self, valores: np.ndarray[Any, Any]) -> None:
        if valores.size == 0:
            return
        self.vistos += int(valores.size)
        candidatos = np.concatenate([self._valores, valores])
        chaves = np.concatenate([self._chaves, self._sorteio.random(valores.size)])
        if candidatos.size > self.tamanho:
            escolhidos = np.argpartition(chaves, self.tamanho)[: self.tamanho]
            candidatos, chaves = candidatos[escolhidos], chaves[escolhidos]
        self._valores, self._chaves = candidatos, chaves

    def mediana(self) -> float | None:
        return float(np.median(self._valores)) if self._valores.size else None


@dataclass(slots=True)
class EstatisticaValor:
    """Estatísticas de um conjunto de `VlrMercado` — exatas, em `Decimal`."""

    contagem: int = 0
    nulos: int = 0
    minimo: Decimal | None = None
    maximo: Decimal | None = None
    soma: Decimal = Decimal(0)
    negativos: int = 0
    zeros: int = 0

    def juntar(self, outra: EstatisticaValor) -> None:
        self.contagem += outra.contagem
        self.nulos += outra.nulos
        self.soma += outra.soma
        self.negativos += outra.negativos
        self.zeros += outra.zeros
        if outra.minimo is not None:
            self.minimo = outra.minimo if self.minimo is None else min(self.minimo, outra.minimo)
        if outra.maximo is not None:
            self.maximo = outra.maximo if self.maximo is None else max(self.maximo, outra.maximo)

    @property
    def media(self) -> float | None:
        return float(self.soma) / self.contagem if self.contagem else None

    def como_json(self) -> dict[str, Any]:
        return {
            "contagem": self.contagem,
            "nulos": self.nulos,
            "min": _texto_decimal(self.minimo),
            "max": _texto_decimal(self.maximo),
            "soma": _texto_decimal(self.soma),
            "media": _arredondar(self.media),
            "negativos": self.negativos,
            "zeros": self.zeros,
        }


def _estatistica(valores: pa.Array) -> EstatisticaValor:
    """Mede um bloco de `VlrMercado` sem sair do Arrow."""
    extremos = pc.min_max(valores).as_py()
    soma = _para_decimal(pc.sum(_para_soma(valores)).as_py())
    zero = _zero(valores.type)
    return EstatisticaValor(
        contagem=len(valores) - valores.null_count,
        nulos=valores.null_count,
        minimo=_para_decimal(extremos["min"]),
        maximo=_para_decimal(extremos["max"]),
        soma=soma if soma is not None else Decimal(0),
        negativos=int(pc.sum(pc.less(valores, zero)).as_py() or 0),
        zeros=int(pc.sum(pc.equal(valores, zero)).as_py() or 0),
    )


@dataclass(frozen=True, slots=True)
class Origem:
    """De onde veio o insumo — o que o sidecar do download registrou sobre ele."""

    arquivo: str = ""
    url: str = ""
    ultima_modificacao_ckan: str = ""
    tamanho_bytes: int | None = None
    #: SHA-256 do insumo: a mesma chave de idempotência do Parquet (docs/04 §10).
    chave: str = ""

    @classmethod
    def do_arquivo(cls, caminho: Path | str, *, chave: str = "") -> Origem:
        """Lê o sidecar `{nome}.samp-dq.json`, se houver; senão descreve o que o disco mostra."""
        caminho = Path(caminho)
        registro = ler_json(caminho.with_name(caminho.name + SUFIXO_SIDECAR)) or {}
        tamanho = caminho.stat().st_size if caminho.exists() else registro.get("tamanho")
        return cls(
            arquivo=caminho.name,
            url=str(registro.get("url") or ""),
            ultima_modificacao_ckan=str(registro.get("ultima_modificacao_ckan") or ""),
            tamanho_bytes=int(tamanho) if tamanho is not None else None,
            chave=chave or str(registro.get("sha256") or ""),
        )

    def como_json(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "lastModifiedCkan": self.ultima_modificacao_ckan,
            "tamanhoBytes": self.tamanho_bytes,
            "chaveInsumo": self.chave,
        }


@dataclass(frozen=True, slots=True)
class Perfil:
    """O retrato do arquivo. `como_json()` é o contrato de docs/04 §6."""

    arquivo: str
    ano: int | None
    gerado_em: datetime
    origem: Origem
    linhas: int
    linhas_descartadas: int
    campos_ausentes: tuple[str, ...]
    nulos: dict[str, int]
    vazios: dict[str, int]
    contagens: dict[str, ContagemDeValores]
    competencia_min: date | None
    competencia_max: date | None
    valor: EstatisticaValor
    valor_mediana: float | None
    valor_mediana_exata: bool
    por_detalhe: dict[str, EstatisticaValor]
    normalizacoes: dict[str, Any]
    campos_dominio: tuple[str, ...] = ESQUEMA_SAMP.campos_de_dominio
    campo_competencia: str = "DatCompetencia"

    @property
    def cardinalidades(self) -> dict[str, int]:
        return {nome: contagem.distintos for nome, contagem in self.contagens.items()}

    @property
    def truncados(self) -> tuple[str, ...]:
        return tuple(nome for nome, c in self.contagens.items() if c.truncado)

    def competencias(self) -> dict[str, int]:
        """Linhas por competência, em ordem cronológica — insumo da DQ-COM-004."""
        contagem = self.contagens.get(self.campo_competencia)
        return dict(sorted(contagem.contagens.items())) if contagem else {}

    def dominios_observados(self) -> dict[str, dict[str, int]]:
        """Valores distintos por campo de domínio: o `dominios-observados-{ano}.json`.

        Campo truncado fica de fora: publicar uma contagem parcial como se fosse o domínio
        observado calibraria as listas de docs/03 com meia verdade.
        """
        return {
            nome: self.contagens[nome].como_json()
            for nome in self.campos_dominio
            if nome in self.contagens and not self.contagens[nome].truncado
        }

    def como_json(self) -> dict[str, Any]:
        return {
            "arquivo": self.arquivo,
            "ano": self.ano,
            "geradoEm": self.gerado_em.isoformat(),
            "origem": self.origem.como_json(),
            "linhasTotais": self.linhas,
            "linhasDescartadas": self.linhas_descartadas,
            "camposAusentes": list(self.campos_ausentes),
            "nulosPorCampo": dict(self.nulos),
            "vaziosPorCampo": dict(self.vazios),
            "cardinalidades": self.cardinalidades,
            "cardinalidadesTruncadas": list(self.truncados),
            "periodoCompetencia": {
                "min": self.competencia_min.isoformat() if self.competencia_min else None,
                "max": self.competencia_max.isoformat() if self.competencia_max else None,
            },
            "competencias": self.competencias(),
            "distribuicaoVlrMercado": {
                **self.valor.como_json(),
                "mediana": _arredondar(self.valor_mediana),
                "medianaExata": self.valor_mediana_exata,
            },
            # A unidade de VlrMercado depende de DscDetalheMercado: sem esta quebra, o mínimo e o
            # máximo globais comparam kWh com R$. É também a base das faixas da DQ-ACU-001/002.
            "distribuicaoPorDetalheMercado": {
                nome: self.por_detalhe[nome].como_json() for nome in sorted(self.por_detalhe)
            },
            "normalizacoes": dict(self.normalizacoes),
        }

    def resumo(self) -> str:
        periodo = ""
        if self.competencia_min and self.competencia_max:
            periodo = f"; {self.competencia_min:%Y-%m} a {self.competencia_max:%Y-%m}"
        faltando = sum(self.nulos.values()) + sum(self.vazios.values())
        return (
            f"{self.arquivo}: {self.linhas} linhas{periodo}; "
            f"{faltando} campo(s) sem valor; {len(self.dominios_observados())} domínios catalogados"
        )


@dataclass(slots=True)
class Perfilador:
    """Acumula o perfil bloco a bloco::

    perfilador = Perfilador()
    for bloco in perfilador.perfilar_blocos(normalizador.normalizar_blocos(leitor.blocos())):
        ...
    perfil = perfilador.perfil(arquivo="bruto/samp-2024.csv")

    Espera os blocos **já normalizados** (etapa 3): é sobre o dado tipado que competência é data e
    `VlrMercado` é decimal. Bloco a que falte uma coluna do esquema é perfilado assim mesmo — a
    coluna aparece em `camposAusentes`, que é achado de conformidade, não motivo para abortar.
    """

    esquema: Esquema = ESQUEMA_SAMP
    limite_distintos: int = LIMITE_DISTINTOS
    tamanho_amostra: int = TAMANHO_AMOSTRA
    semente: int = SEMENTE
    campo_valor: str = "VlrMercado"
    campo_competencia: str = "DatCompetencia"
    campo_detalhe: str = "DscDetalheMercado"
    _linhas: int = field(init=False, default=0, repr=False)
    _nulos: dict[str, int] = field(init=False, default_factory=dict, repr=False)
    _vazios: dict[str, int] = field(init=False, default_factory=dict, repr=False)
    _contagens: dict[str, ContagemDeValores] = field(init=False, default_factory=dict, repr=False)
    _vistos: set[str] = field(init=False, default_factory=set, repr=False)
    _valor: EstatisticaValor = field(init=False, default_factory=EstatisticaValor, repr=False)
    _por_detalhe: dict[str, EstatisticaValor] = field(init=False, default_factory=dict, repr=False)
    _amostra: Amostra = field(init=False, repr=False)
    _competencia_min: date | None = field(init=False, default=None, repr=False)
    _competencia_max: date | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        self._amostra = Amostra(tamanho=self.tamanho_amostra, semente=self.semente)

    @property
    def linhas(self) -> int:
        return self._linhas

    def perfilar_blocos(self, blocos: Iterable[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Mede cada bloco e o devolve intacto, para encaixar no meio do pipeline."""
        for bloco in blocos:
            yield self.perfilar(bloco)

    def perfilar(self, bloco: pd.DataFrame) -> pd.DataFrame:
        if bloco.empty:
            return bloco
        self._linhas += len(bloco)
        valores: pa.Array | None = None

        for campo in self.esquema:
            if campo.nome not in bloco.columns:
                continue
            self._vistos.add(campo.nome)
            coluna = _como_array(bloco[campo.nome])
            self._contar_faltantes(campo.nome, coluna)
            if campo.nome == self.campo_valor:
                valores = coluna
                self._observar_valor(coluna)
            else:
                # Contar os distintos de VlrMercado não diria nada e guardaria milhões de chaves.
                self._contagem(campo.nome).observar(coluna)
            if campo.nome == self.campo_competencia:
                self._observar_competencia(coluna)

        if valores is not None and self.campo_detalhe in bloco.columns:
            self._observar_por_detalhe(_como_array(bloco[self.campo_detalhe]), valores)
        return bloco

    def perfil(
        self,
        *,
        arquivo: Path | str | None = None,
        ano: int | None = None,
        origem: Origem | None = None,
        leitura: RelatorioLeitura | None = None,
        normalizacao: RelatorioNormalizacao | None = None,
    ) -> Perfil:
        """Fecha o perfil.

        Os relatórios de leitura e normalização são opcionais porque um perfil pode nascer do
        Parquet, quando a normalização aconteceu em outra execução. Sem eles, `normalizacoes` sai
        vazio — o que é honesto: não se sabe, em vez de zero.
        """
        caminho = Path(arquivo) if arquivo is not None else None
        if origem is None and caminho is not None:
            origem = Origem.do_arquivo(caminho)
        if ano is None and caminho is not None:
            ano = ano_do_arquivo(caminho)
        return Perfil(
            arquivo=caminho.name if caminho else "",
            ano=ano,
            gerado_em=datetime.now(UTC),
            origem=origem or Origem(),
            linhas=self._linhas,
            linhas_descartadas=leitura.linhas_descartadas if leitura else 0,
            campos_ausentes=tuple(n for n in self.esquema.nomes if n not in self._vistos),
            nulos=dict(self._nulos),
            vazios=dict(self._vazios),
            contagens=dict(self._contagens),
            competencia_min=self._competencia_min,
            competencia_max=self._competencia_max,
            valor=self._valor,
            valor_mediana=self._amostra.mediana(),
            valor_mediana_exata=self._amostra.integral,
            por_detalhe=dict(self._por_detalhe),
            normalizacoes=normalizacao.como_perfil(leitura) if normalizacao else {},
            campos_dominio=self.esquema.campos_de_dominio,
            campo_competencia=self.campo_competencia,
        )

    def _contagem(self, nome: str) -> ContagemDeValores:
        if nome not in self._contagens:
            self._contagens[nome] = ContagemDeValores(limite=self.limite_distintos)
        return self._contagens[nome]

    def _contar_faltantes(self, nome: str, coluna: pa.Array) -> None:
        """Nulo e vazio contam separado: ausência de valor e texto em branco são achados distintos.

        A DQ-COM-001 trata os dois como falta de preenchimento, mas quem lê o perfil precisa saber
        se o campo veio vazio no CSV (o padrão do arquivo é "Não se aplica") ou se o valor existia
        e não converteu para o tipo declarado.

        O teste de vazio olha o tipo do Arrow, não o do esquema: um Parquet de outra procedência
        pode trazer como inteiro um campo que o SAMP publica como texto, e perfilar mesmo assim é
        justamente o ponto de `perfilar_parquet`.
        """
        if coluna.null_count:
            self._nulos[nome] = self._nulos.get(nome, 0) + coluna.null_count
        if pa.types.is_string(coluna.type) or pa.types.is_large_string(coluna.type):
            vazios = int(pc.sum(pc.equal(coluna, "")).as_py() or 0)
            if vazios:
                self._vazios[nome] = self._vazios.get(nome, 0) + vazios

    def _observar_valor(self, coluna: pa.Array) -> None:
        self._valor.juntar(_estatistica(coluna))
        flutuantes = pc.cast(coluna, pa.float64()).to_numpy(zero_copy_only=False)
        self._amostra.observar(flutuantes[~np.isnan(flutuantes)])

    def _observar_competencia(self, coluna: pa.Array) -> None:
        extremos = pc.min_max(coluna).as_py()
        menor, maior = extremos["min"], extremos["max"]
        if menor is not None:
            atual = self._competencia_min
            self._competencia_min = menor if atual is None else min(atual, menor)
        if maior is not None:
            atual = self._competencia_max
            self._competencia_max = maior if atual is None else max(atual, maior)

    def _observar_por_detalhe(self, detalhes: pa.Array, valores: pa.Array) -> None:
        """Agrega por `DscDetalheMercado` no próprio Arrow, sem sair do decimal.

        Negativo e zero viram colunas booleanas antes do agrupamento: somá-las no mesmo
        `group_by` custa uma passada, e é o que a DQ-ACU-002 pergunta por detalhe de mercado.
        """
        zero = _zero(valores.type)
        tabela = pa.table(
            {
                "detalhe": detalhes,
                "valor": _para_soma(valores),
                "negativo": pc.less(valores, zero),
                "zero": pc.equal(valores, zero),
            }
        )
        agregado = tabela.group_by("detalhe").aggregate(
            [
                ("valor", "count"),
                ("valor", "min"),
                ("valor", "max"),
                ("valor", "sum"),
                ("negativo", "sum"),
                ("zero", "sum"),
            ]
        )
        for linha in agregado.to_pylist():
            soma = _para_decimal(linha["valor_sum"])
            parcial = EstatisticaValor(
                contagem=int(linha["valor_count"]),
                minimo=_para_decimal(linha["valor_min"]),
                maximo=_para_decimal(linha["valor_max"]),
                soma=soma if soma is not None else Decimal(0),
                negativos=int(linha["negativo_sum"] or 0),
                zeros=int(linha["zero_sum"] or 0),
            )
            self._por_detalhe.setdefault(_chave(linha["detalhe"]), EstatisticaValor()).juntar(
                parcial
            )


def ano_do_arquivo(caminho: Path | str) -> int | None:
    """O ano no nome do arquivo (`samp-2024.csv` -> 2024); `None` se não houver."""
    achado = _ANO_NO_NOME.search(Path(caminho).stem)
    return int(achado.group()) if achado else None


def perfilar_parquet(
    caminho: Path | str,
    *,
    perfilador: Perfilador | None = None,
    ano: int | None = None,
    tamanho_lote: int = TAMANHO_LOTE_PARQUET,
) -> Perfil:
    """Perfila um Parquet já gravado, em lotes.

    É o caminho de quando o Parquet está em cache (reprocessar o CSV só para refazer o perfil
    desperdiçaria a idempotência da etapa 4) e o de auditar um Parquet publicado pela ANEEL.
    """
    caminho = Path(caminho)
    perfilador = perfilador or Perfilador()
    arquivo = pq.ParquetFile(caminho)
    for lote in arquivo.iter_batches(batch_size=tamanho_lote):
        perfilador.perfilar(pa.Table.from_batches([lote]).to_pandas(types_mapper=pd.ArrowDtype))
    return perfilador.perfil(arquivo=caminho, ano=ano)


def gravar_perfil(perfil: Perfil, pasta: Path | str) -> Path:
    """Grava `perfil-{ano}.json` na pasta de saída."""
    return escrever_json(caminho_artefato(pasta, NOME_PERFIL, perfil.ano), perfil.como_json())


def gravar_dominios_observados(perfil: Perfil, pasta: Path | str) -> Path:
    """Grava `dominios-observados-{ano}.json` na pasta de saída."""
    return escrever_json(
        caminho_artefato(pasta, NOME_DOMINIOS, perfil.ano), perfil.dominios_observados()
    )


__all__ = [
    "LIMITE_DISTINTOS",
    "SEMENTE",
    "TAMANHO_AMOSTRA",
    "Amostra",
    "ContagemDeValores",
    "EstatisticaValor",
    "Origem",
    "Perfil",
    "Perfilador",
    "ano_do_arquivo",
    "gravar_dominios_observados",
    "gravar_perfil",
    "perfilar_parquet",
]
