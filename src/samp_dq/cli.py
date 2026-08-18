"""Interface de linha de comando do samp-dq.

    samp-dq listar
    samp-dq baixar --ano 2024 --saida ./bruto

A CLI é uma casca fina sobre `samp_dq.ckan`: ela resolve argumentos, mostra progresso e
transforma exceções do módulo em mensagens de erro — nenhuma regra de negócio mora aqui.
"""

from __future__ import annotations

import json as _json
import sys
from pathlib import Path
from typing import Annotated, Any, Literal, NoReturn

import typer

from samp_dq import __version__
from samp_dq.artefatos import NOME_PERFIL, NOME_RESULTADO, caminho_artefato, ler_json
from samp_dq.ckan import CkanClient, Dataset, Formato, Recurso
from samp_dq.ckan.download import ResultadoDownload, StatusDownload, baixar_recurso
from samp_dq.errors import SampDQError
from samp_dq.ingest import LeitorCsv, Normalizador, chave_do_insumo, escrever_parquet
from samp_dq.perfil import (
    Origem,
    Perfil,
    Perfilador,
    ano_do_arquivo,
    gravar_dominios_observados,
    gravar_perfil,
    perfilar_parquet,
)
from samp_dq.qualidade import gravar_resultado, validar

app = typer.Typer(
    name="samp-dq",
    help="Baixa e analisa a qualidade dos dados do SAMP (ANEEL).",
    no_args_is_help=True,
    add_completion=False,
)


def criar_cliente(**kwargs: Any) -> CkanClient:
    """Ponto de injeção: os testes trocam esta função por um portal simulado."""
    return CkanClient(**kwargs)


def _versao(valor: bool) -> None:
    if valor:
        typer.echo(f"samp-dq {__version__}")
        raise typer.Exit


@app.callback()
def principal(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_versao, is_eager=True, help="Mostra a versão."),
    ] = False,
) -> None:
    """Ferramentas de qualidade de dados para o SAMP."""


def _humano(bytes_: int | None) -> str:
    if bytes_ is None:
        return "-"
    tamanho = float(bytes_)
    for unidade in ("B", "KB", "MB", "GB"):
        if tamanho < 1024 or unidade == "GB":
            return f"{tamanho:.1f} {unidade}" if unidade != "B" else f"{int(tamanho)} B"
        tamanho /= 1024
    return f"{tamanho:.1f} GB"


def _resolver_formato(formato: str) -> Formato | None:
    """`todos` significa "sem filtro"."""
    return None if formato.strip().lower() == "todos" else Formato.de_texto(formato)


def _obter_dataset(cliente: CkanClient) -> Dataset:
    return cliente.package_show()


@app.command("listar")
def listar(
    ano: Annotated[
        list[int] | None, typer.Option("--ano", help="Filtra por ano (repetível).")
    ] = None,
    formato: Annotated[str, typer.Option("--formato", help="csv, parquet, pdf ou todos.")] = "csv",
    json_: Annotated[bool, typer.Option("--json", help="Saída em JSON.")] = False,
) -> None:
    """Lista os recursos publicados no dataset SAMP."""
    with _cliente() as cliente:
        dataset = _obter_dataset(cliente)

    recursos = dataset.filtrar(ano=ano or None, formato=_resolver_formato(formato))

    if json_:
        typer.echo(
            _json.dumps(
                [
                    {
                        "id": r.id,
                        "nome": r.nome,
                        "ano": r.ano,
                        "formato": str(r.formato),
                        "tamanho": r.tamanho,
                        "ultima_modificacao": (
                            r.ultima_modificacao.isoformat() if r.ultima_modificacao else None
                        ),
                        "url": r.url,
                    }
                    for r in recursos
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if not recursos:
        typer.echo("Nenhum recurso corresponde ao filtro.")
        return

    typer.echo(f"{'ANO':<6} {'FORMATO':<8} {'TAMANHO':>10}  {'MODIFICADO':<19} NOME")
    for r in recursos:
        modificado = (
            r.ultima_modificacao.strftime("%Y-%m-%d %H:%M:%S") if r.ultima_modificacao else "-"
        )
        ano_txt = str(r.ano) if r.ano else "-"
        typer.echo(
            f"{ano_txt:<6} {r.formato!s:<8} {_humano(r.tamanho):>10}  {modificado:<19} {r.nome}"
        )
    typer.echo(f"\n{len(recursos)} recurso(s) — licença dos dados: {dataset.licenca_titulo}")


@app.command("baixar")
def baixar(
    ano: Annotated[
        list[int] | None, typer.Option("--ano", help="Ano a baixar (repetível).")
    ] = None,
    todos: Annotated[bool, typer.Option("--todos", help="Baixa todos os anos.")] = False,
    formato: Annotated[str, typer.Option("--formato", help="csv ou parquet.")] = "csv",
    saida: Annotated[Path, typer.Option("--saida", help="Pasta de destino.")] = Path("bruto"),
    forcar: Annotated[bool, typer.Option("--forcar", help="Ignora o cache local.")] = False,
    silencioso: Annotated[bool, typer.Option("--silencioso", help="Não mostra progresso.")] = False,
) -> None:
    """Baixa os arquivos do SAMP para uma pasta local."""
    if not ano and not todos:
        raise typer.BadParameter("informe --ano (repetível) ou --todos para baixar tudo.")

    alvo = _resolver_formato(formato) or Formato.CSV

    with _cliente() as cliente:
        dataset = _obter_dataset(cliente)
        recursos = dataset.filtrar(ano=None if todos else ano, formato=alvo)

        if not recursos:
            anos_pedidos = ", ".join(str(a) for a in ano or [])
            disponiveis = ", ".join(str(a) for a in dataset.anos_disponiveis())
            _falhar(
                f"nenhum recurso {alvo} para o(s) ano(s) {anos_pedidos}; disponíveis: {disponiveis}"
            )

        total_baixado = 0
        for recurso in recursos:
            resultado = _baixar_um(cliente, recurso, saida, forcar, silencioso)
            total_baixado += resultado.bytes_baixados

    typer.echo(f"\n{len(recursos)} arquivo(s) em {saida} ({_humano(total_baixado)} da rede).")


def _baixar_um(
    cliente: CkanClient, recurso: Recurso, saida: Path, forcar: bool, silencioso: bool
) -> ResultadoDownload:
    progresso = None if silencioso else _barra(recurso)
    resultado = baixar_recurso(cliente, recurso, saida, forcar=forcar, progresso=progresso)

    if not silencioso:
        typer.echo("\r" + " " * 70, nl=False)  # limpa a linha da barra

    rotulo = {
        StatusDownload.BAIXADO: "baixado",
        StatusDownload.RETOMADO: "retomado",
        StatusDownload.EM_CACHE: "em cache (inalterado)",
    }[resultado.status]
    typer.echo(f"\r{recurso.nome_arquivo}: {rotulo} — {_humano(resultado.tamanho)}")
    for aviso in resultado.avisos:
        typer.echo(f"  aviso: {aviso}")
    return resultado


@app.command("perfilar")
def perfilar(
    entrada: Annotated[
        Path, typer.Option("--entrada", help="CSV bruto do SAMP ou Parquet já convertido.")
    ],
    saida: Annotated[Path, typer.Option("--saida", help="Pasta dos artefatos.")] = Path(
        "preprocessado"
    ),
    ano: Annotated[
        int | None, typer.Option("--ano", help="Ano do arquivo; padrão: o do nome.")
    ] = None,
    forcar: Annotated[bool, typer.Option("--forcar", help="Ignora o cache local.")] = False,
) -> None:
    """Converte o CSV para Parquet, grava o perfil e valida o catálogo de regras.

    Numa passada só: lê, normaliza, grava `samp-{ano}.parquet`, mede e escreve
    `perfil-{ano}.json`, `dominios-observados-{ano}.json` e `resultado-{ano}.json`.
    """
    if not entrada.exists():
        _falhar(f"não encontrei {entrada}")

    ano = ano if ano is not None else ano_do_arquivo(entrada)
    chave = chave_do_insumo(entrada)

    em_cache = _perfil_em_cache(saida, ano, chave) and _resultado_em_cache(saida, ano, chave)
    if not forcar and em_cache:
        typer.echo(f"perfil-{ano}.json: em cache (insumo inalterado)")
        return

    try:
        perfil = _perfilar(entrada, saida, ano, chave, forcar)
    except SampDQError as erro:
        _falhar(str(erro))

    typer.echo(perfil.resumo())
    for caminho in (gravar_perfil(perfil, saida), gravar_dominios_observados(perfil, saida)):
        typer.echo(f"  {caminho}")

    parquet = entrada if entrada.suffix.lower() == ".parquet" else saida / f"samp-{ano}.parquet"
    try:
        resultado = validar(perfil, parquet)
    except SampDQError as erro:
        _falhar(str(erro))
    typer.echo(resultado.resumo())
    typer.echo(f"  {gravar_resultado(resultado, saida)}")


def _perfilar(entrada: Path, saida: Path, ano: int | None, chave: str, forcar: bool) -> Perfil:
    """Perfila a entrada, convertendo-a para Parquet antes quando ela é um CSV."""
    origem = Origem.do_arquivo(entrada, chave=chave)
    perfilador = Perfilador()

    if entrada.suffix.lower() == ".parquet":
        perfilar_parquet(entrada, perfilador=perfilador)
        return perfilador.perfil(arquivo=entrada, ano=ano, origem=origem)

    leitor = LeitorCsv(entrada)
    normalizador = Normalizador()
    destino = saida / f"samp-{ano}.parquet"
    resultado = escrever_parquet(
        perfilador.perfilar_blocos(normalizador.normalizar_blocos(leitor.blocos())),
        destino,
        chave=chave,
        forcar=forcar,
    )
    typer.echo(resultado.resumo())

    if resultado.reaproveitado:
        # O Parquet em cache não consumiu os blocos — o perfil sai dele, sem reler o CSV.
        perfilar_parquet(destino, perfilador=perfilador)
        return perfilador.perfil(arquivo=entrada, ano=ano, origem=origem)

    return perfilador.perfil(
        arquivo=entrada,
        ano=ano,
        origem=origem,
        leitura=leitor.relatorio,
        normalizacao=normalizador.relatorio,
    )


def _perfil_em_cache(saida: Path, ano: int | None, chave: str) -> bool:
    """Perfil anterior do mesmo insumo torna a execução dispensável (docs/04 §10)."""
    anterior = ler_json(caminho_artefato(saida, NOME_PERFIL, ano))
    if not anterior or not chave:
        return False
    return bool(anterior.get("origem", {}).get("chaveInsumo") == chave)


def _resultado_em_cache(saida: Path, ano: int | None, chave: str) -> bool:
    anterior = ler_json(caminho_artefato(saida, NOME_RESULTADO, ano))
    if not anterior or not chave:
        return False
    return bool(anterior.get("origem", {}).get("chaveInsumo") == chave)


@app.command("validar")
def validar_cmd(
    entrada: Annotated[
        Path, typer.Option("--entrada", help="Parquet do SAMP (ou CSV já perfilado na --saida).")
    ],
    saida: Annotated[Path, typer.Option("--saida", help="Pasta dos artefatos.")] = Path(
        "preprocessado"
    ),
    ano: Annotated[
        int | None, typer.Option("--ano", help="Ano do arquivo; padrão: o do nome.")
    ] = None,
) -> None:
    """Aplica o catálogo de regras ao Parquet e grava `resultado-{ano}.json`."""
    if not entrada.exists():
        _falhar(f"não encontrei {entrada}")

    ano = ano if ano is not None else ano_do_arquivo(entrada)
    parquet = entrada if entrada.suffix.lower() == ".parquet" else saida / f"samp-{ano}.parquet"
    if not parquet.exists():
        _falhar(f"não encontrei {parquet}; rode `samp-dq perfilar` antes")

    try:
        perfil = perfilar_parquet(parquet, ano=ano)
        if entrada.suffix.lower() != ".parquet":
            perfil = perfilador_com_origem(perfil, entrada)
        resultado = validar(perfil, parquet)
    except SampDQError as erro:
        _falhar(str(erro))

    typer.echo(resultado.resumo())
    typer.echo(f"  {gravar_resultado(resultado, saida)}")


def perfilador_com_origem(perfil: Perfil, csv: Path) -> Perfil:
    """Troca a origem do perfil do Parquet pela do CSV, quando a validação partiu dele."""
    from dataclasses import replace

    return replace(
        perfil, arquivo=csv.name, origem=Origem.do_arquivo(csv, chave=chave_do_insumo(csv))
    )


def _barra(recurso: Recurso) -> Any:
    """Progresso simples em uma linha; evita dependência de widget de terminal."""

    def relatar(gravados: int, total: int | None) -> None:
        if total:
            pct = 100 * gravados / total
            typer.echo(
                f"\r{recurso.nome_arquivo}: {pct:5.1f}% ({_humano(gravados)} de {_humano(total)})",
                nl=False,
            )
        else:
            typer.echo(f"\r{recurso.nome_arquivo}: {_humano(gravados)}", nl=False)

    return relatar


def _falhar(mensagem: str) -> NoReturn:
    typer.echo(f"erro: {mensagem}", err=True)
    raise typer.Exit(1)


class _cliente:
    """Contexto que abre o cliente CKAN e converte erros do módulo em saída limpa."""

    def __enter__(self) -> CkanClient:
        self._cliente = criar_cliente()
        return self._cliente

    def __exit__(self, tipo: Any, valor: Any, tb: Any) -> Literal[False]:
        self._cliente.fechar()
        if isinstance(valor, SampDQError):
            typer.echo(f"erro: {valor}", err=True)
            raise typer.Exit(1) from valor
        return False


def main() -> None:  # pragma: no cover - ponto de entrada
    sys.exit(app())


if __name__ == "__main__":  # pragma: no cover
    main()
