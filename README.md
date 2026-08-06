# samp-dq

Módulo open source em Python para **baixar e analisar a qualidade dos dados do SAMP** — o Sistema de
Acompanhamento de Informações de Mercado para Regulação Econômica, publicado pela ANEEL no portal de
[dados abertos](https://dadosabertos.aneel.gov.br).

O SAMP publica, mensalmente, um arquivo CSV por ano (2003 até o ano corrente) com os valores de
mercado faturados das distribuidoras de energia elétrica. Os arquivos são grandes (dezenas a centenas
de MB), vêm em **Latin-1**, separados por `;`, com decimal em vírgula — este módulo cuida disso.

> **Status:** alpha. Prontos: acesso ao CKAN (listagem + download), o contrato do esquema e a
> leitura do CSV em blocos. Normalização, escrita do Parquet, perfilamento e o catálogo de regras
> vêm nos próximos incrementos.

## Instalação

O projeto usa [uv](https://docs.astral.sh/uv/):

```bash
uv sync              # cria o ambiente e instala tudo
uv run samp-dq --help
```

## Uso

### Listar os recursos publicados

```bash
uv run samp-dq listar                 # todos os CSVs, com ano, tamanho e última modificação
uv run samp-dq listar --formato todos # inclui Parquet e o dicionário de dados
```

### Baixar os CSVs

```bash
uv run samp-dq baixar --ano 2024 --saida ./bruto
uv run samp-dq baixar --ano 2020 --ano 2021 --ano 2022 --saida ./bruto
uv run samp-dq baixar --todos --formato parquet --saida ./bruto
```

O download é **idempotente**: cada arquivo ganha um sidecar `.samp-dq.json` com `ETag`,
`Last-Modified`, tamanho e SHA-256. Rodar de novo com o recurso inalterado não baixa nada. Downloads
interrompidos são retomados via `Range` a partir do `.part` parcial.

### Como biblioteca

```python
from samp_dq.ckan import CkanClient, baixar_recurso

with CkanClient() as cliente:
    dataset = cliente.package_show()
    recurso = dataset.recurso(ano=2024, formato="CSV")
    resultado = baixar_recurso(cliente, recurso, destino="./bruto")

print(resultado.status, resultado.caminho, resultado.sha256)
```

### Ler um CSV bruto

```python
from samp_dq.ingest import LeitorCsv

leitor = LeitorCsv("bruto/samp-2024.csv")
for bloco in leitor.blocos():  # DataFrames de 250 mil linhas, tudo como texto
    ...

print(leitor.relatorio.resumo())  # linhas, blocos e linhas descartadas
```

A leitura é sempre **em blocos** (`samp-2024.csv` tem 369 MB) e não interpreta nada: decimal com
vírgula, espaços de preenchimento e tipos chegam crus, porque é sobre o dado original que as regras
de qualidade precisam decidir. Um cabeçalho fora do contrato aborta a leitura — use `estrito=False`
para inspecionar um arquivo cujo layout mudou.

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
[Open Data Commons ODbL](https://opendatacommons.org/licenses/odbl/) — este módulo apenas os baixa e
analisa, não os redistribui. Ao publicar resultados derivados, mantenha a atribuição à ANEEL.
