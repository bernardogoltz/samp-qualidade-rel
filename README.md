# samp-dq

Módulo open source em Python para **baixar os arquivos do SAMP e carregá-los no pandas** — o
Sistema de Acompanhamento de Informações de Mercado para Regulação Econômica, publicado pela ANEEL
no portal de [dados abertos](https://dadosabertos.aneel.gov.br).

O SAMP publica um CSV por ano, de 2003 até o ano corrente, com os valores de mercado faturados das
distribuidoras de energia elétrica. Os arquivos são grandes (dezenas a centenas de MB), vêm em
**Latin-1**, separados por `;`, com decimal em vírgula — este módulo cuida disso e entrega
`DataFrame` tipado ou Parquet.

> **Status:** alpha. Download, leitura, tipagem e escrita do Parquet estão prontos e testados.

## Instalação

```bash
git clone https://github.com/bernardogoltz/samp-qualidade-rel.git
cd samp-qualidade-rel
```

O projeto usa [uv](https://docs.astral.sh/uv/):

```bash
uv sync              # cria o ambiente e instala tudo
uv run samp-dq --help
```

## Começando: do zero a um DataFrame

```bash
uv run samp-dq baixar --ano 2024 --saida ./bruto
```

```python
from samp_dq.ingest import LeitorCsv, Normalizador, chave_do_insumo, escrever_parquet

csv = "bruto/samp-2024.csv"

# Converte uma vez para Parquet tipado (4,4 s para 1,3 milhão de linhas)
escrever_parquet(
    Normalizador().normalizar_blocos(LeitorCsv(csv).blocos()),
    "preprocessado/samp-2024.parquet",
    chave=chave_do_insumo(csv),
)
```

```python
import pandas as pd

df = pd.read_parquet("preprocessado/samp-2024.parquet")
df.groupby("SigAgenteDistribuidora")["VlrMercado"].sum()
```

Converter para Parquet primeiro compensa: o arquivo cai de 369 MB para **11,8 MB**, e as leituras
seguintes ficam quase instantâneas, já com os tipos certos.

## Linha de comando

### Ver o que está publicado

```bash
uv run samp-dq listar                 # todos os CSVs, com ano, tamanho e última modificação
uv run samp-dq listar --formato todos # inclui Parquet e o dicionário de dados
uv run samp-dq listar --json          # saída para script
```

```
ANO    FORMATO     TAMANHO  MODIFICADO          NOME
2003   CSV         91.5 MB  2026-07-28 14:45:33 samp-2003.csv
...
2024   CSV        352.1 MB  2026-07-28 15:53:26 samp-2024.csv
```

### Baixar

```bash
uv run samp-dq baixar --ano 2024 --saida ./bruto
uv run samp-dq baixar --ano 2020 --ano 2021 --ano 2022 --saida ./bruto
uv run samp-dq baixar --todos --formato parquet --saida ./bruto
```

O download é **idempotente**: cada arquivo ganha um sidecar `.samp-dq.json` com `ETag`,
`Last-Modified`, tamanho e SHA-256. Rodar de novo com o recurso inalterado não baixa nada.
Downloads interrompidos são retomados via `Range` a partir do `.part` parcial — útil, porque o
portal derruba a conexão em arquivos grandes.

## Como biblioteca

### Descobrir e baixar

```python
from samp_dq.ckan import CkanClient, baixar_recurso

with CkanClient() as cliente:
    dataset = cliente.package_show()

    print(dataset.anos_disponiveis())  # [2003, 2004, ..., 2026]

    recurso = dataset.recurso(ano=2024, formato="CSV")
    resultado = baixar_recurso(cliente, recurso, destino="./bruto")

print(resultado.status, resultado.caminho, resultado.sha256)
```

### Ler o CSV cru

```python
from samp_dq.ingest import LeitorCsv

leitor = LeitorCsv("bruto/samp-2024.csv")
for bloco in leitor.blocos():  # DataFrames de 250 mil linhas, tudo como texto
    ...

print(leitor.relatorio.resumo())  # "samp-2024.csv: 1303447 linhas em 6 blocos"
```

A leitura é sempre **em blocos**, porque o arquivo não cabe confortavelmente em memória. Nesta
etapa nada é interpretado: os valores chegam como texto, exatamente como estão no arquivo. Ajuste
`tamanho_bloco` para trocar memória por número de iterações:

```python
LeitorCsv("bruto/samp-2024.csv", tamanho_bloco=50_000)
```

Se o cabeçalho não bater com as 18 colunas esperadas, a leitura aborta com uma mensagem dizendo o
que mudou. Para inspecionar um arquivo de layout diferente, use `estrito=False`.

### Tipar

```python
from samp_dq.ingest import LeitorCsv, Normalizador

leitor = LeitorCsv("bruto/samp-2024.csv")
normalizador = Normalizador()

for bloco in normalizador.normalizar_blocos(leitor.blocos()):
    ...  # VlrMercado já é decimal; DatCompetencia já é date

print(normalizador.relatorio.resumo())
```

A conversão é **reversível**: vírgula decimal vira ponto, os tipos são aplicados e o espaço de
preenchimento é aparado. O conteúdo em si não é alterado — maiúsculas, acentos e valores como
`"Não se aplica"` chegam ao seu DataFrame como a ANEEL publicou. Onde o texto original importa,
ele é preservado em colunas `_raw` (veja a tabela abaixo).

Valor que não converte (uma data impossível, por exemplo) vira nulo e a linha **não** é
descartada; o `relatorio` conta quantos foram e guarda exemplos.

### Gravar Parquet

```python
from samp_dq.ingest import chave_do_insumo, escrever_parquet

resultado = escrever_parquet(
    normalizador.normalizar_blocos(leitor.blocos()),
    "preprocessado/2024/samp-2024.parquet",
    chave=chave_do_insumo("bruto/samp-2024.csv"),
)
print(resultado.resumo())  # gravado — 1303447 linhas, 11.8 MB
```

A escrita é **atômica** (falha no meio não deixa arquivo truncado nem apaga o Parquet anterior) e
**idempotente**: rodar de novo sobre o mesmo CSV devolve `EM_CACHE` em milissegundos, sem reler
nada. Passe `forcar=True` para regravar.

## Trabalhando com o Parquet

```python
import pandas as pd

# tudo
df = pd.read_parquet("preprocessado/2024/samp-2024.parquet")

# só as colunas de que você precisa — bem mais rápido
df = pd.read_parquet(
    "preprocessado/2024/samp-2024.parquet",
    columns=["DatCompetencia", "SigAgenteDistribuidora", "DscDetalheMercado", "VlrMercado"],
)

# filtrando na leitura, sem trazer o resto para a memória
df = pd.read_parquet(
    "preprocessado/2024/samp-2024.parquet",
    filters=[("DscDetalheMercado", "==", "Energia TUSD (kWh)")],
)
```

### Sobre o tipo de `VlrMercado`

No Parquet, `VlrMercado` é `decimal128(20,6)` — o valor exato, sem erro de arredondamento. Como
isso chega ao pandas depende de como você lê:

```python
# padrão: coluna `object`, com objetos Decimal do Python. Exato, porém lento.
df = pd.read_parquet(caminho)
df.groupby("SigAgenteDistribuidora")["VlrMercado"].sum()  # -> Decimal('36674971.900000')

# dtypes nativos do Arrow: datas e decimais tipados de verdade
df = pd.read_parquet(caminho, dtype_backend="pyarrow")
df["VlrMercado"].dtype  # decimal128(20, 6)[pyarrow]
```

Para contas em volume, gráficos ou qualquer coisa que passe por NumPy, converta para float:

```python
df["VlrMercado"] = df["VlrMercado"].astype("float64")
mercado_mensal = df.groupby("DatCompetencia")["VlrMercado"].sum()
```

### As colunas

| # | Coluna | Tipo | Conteúdo |
|---|---|---|---|
| 1 | `DatGeracaoConjuntoDados` | date | Data em que a ANEEL gerou a publicação |
| 2 | `NumCNPJAgenteDistribuidora` | string | CNPJ da distribuidora (14 dígitos, com zeros à esquerda) |
| 3 | `SigAgenteDistribuidora` | string | Sigla (ex.: `COCEL`) |
| 4 | `NomAgenteDistribuidora` | string | Razão social |
| 5 | `NomTipoMercado` | string | Tipo de mercado (`Regular`, `Sistema de Compensação GD I`…) |
| 6 | `DscModalidadeTarifaria` | string | `Azul`, `Verde`, `Convencional`, `Branca`… |
| 7 | `DscSubGrupoTarifario` | string | `A1`…`A4`, `AS`, `B1`…`B4` |
| 8 | `DscClasseConsumoMercado` | string | `Residencial`, `Industrial`, `Poder público`… |
| 9 | `DscSubClasseConsumidor` | string | Subclasse do consumidor |
| 10 | `DscDetalheConsumidor` | string | Detalhamento adicional |
| 11 | `IdeNucleoCeg` | int64 | Núcleo do código CEG da usina (`0` quando não é geração) |
| 12 | `NumCNPJAgenteAcessante` | string | CNPJ do acessante (às vezes CPF, 11 dígitos) |
| 13 | `NomAgenteAcessante` | string | Razão social do acessante |
| 14 | `DscPostoTarifario` | string | `Ponta`, `Fora ponta`, `Intermediário`, `Não se aplica` |
| 15 | `DscOpcaoEnergia` | string | `Cativo`, `Livre`, `Distribuição`, `Geração`, `Suprimento` |
| 16 | `DscDetalheMercado` | string | A grandeza medida — define a unidade de `VlrMercado` |
| 17 | `DatCompetencia` | date | Mês de competência (sempre no dia 1º) |
| 18 | `VlrMercado` | decimal(20,6) | O valor da grandeza descrita em `DscDetalheMercado` |
| + | `NumCNPJAgenteAcessante_raw` | string | O texto original, antes de aparar espaços |
| + | `VlrMercado_raw` | string | O texto original, com vírgula decimal |

A unidade de `VlrMercado` **depende de `DscDetalheMercado`** — `Energia TUSD (kWh)` está em kWh,
`Receita Demanda (R$)` em reais, `Demanda Faturada (kW)` em kW. Somar valores de detalhes
diferentes mistura unidades:

```python
df.groupby("DscDetalheMercado")["VlrMercado"].sum()  # some sempre dentro do mesmo detalhe
```

O contrato completo das colunas, em código, está em
[`samp_dq/ingest/schema.py`](src/samp_dq/ingest/schema.py).

## Desempenho

Medido com os arquivos reais do portal:

| Arquivo | CSV | Parquet | CSV → Parquet | Releitura |
|---|---:|---:|---:|---:|
| `samp-2003` | 95,9 MB | 4,1 MB | 1,2 s | 0,0 s |
| `samp-2015` | 181,0 MB | 6,9 MB | 2,2 s | 0,0 s |
| `samp-2024` | 369,2 MB | 11,8 MB | 4,4 s | 0,0 s |

Pico de 666 MB de memória, independentemente do tamanho do arquivo — é o que o processamento em
blocos garante.

## Desenvolvimento

O projeto é **test-driven**: todo comportamento novo entra como teste que falha antes da
implementação. Os testes rodam **offline** — as respostas do CKAN são servidas por
`httpx.MockTransport` a partir de fixtures da resposta real.

```bash
uv run pytest                  # suíte offline
uv run pytest -m rede          # smoke tests contra a API da ANEEL (requer internet)
uv run ruff check . && uv run ruff format --check .
uv run mypy
```

## Licença

Código sob licença [MIT](LICENSE).

Os **dados** do SAMP são publicados pela ANEEL sob
[Open Data Commons ODbL](https://opendatacommons.org/licenses/odbl/) — este módulo apenas os baixa
e converte, não os redistribui. Ao publicar resultados derivados, mantenha a atribuição à ANEEL.
