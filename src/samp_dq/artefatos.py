"""Escrita dos artefatos JSON da pasta corporativa — o `io_artifacts` de docs/04 §12.

Só duas garantias, as mesmas do download e do Parquet: **UTF-8 sem escapar acentos** (o JSON é
lido por pessoas e pelo agente, e `\\u00e7` no lugar de `ç` atrapalha os dois) e **escrita
atômica** — grava-se num `.part` e renomeia-se no fim. Uma execução interrompida nunca deixa
`resultado-{ano}.json` pela metade para o agente ler.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

#: Nomes dos artefatos, conforme docs/04 §6.
NOME_PERFIL = "perfil-{ano}.json"
NOME_DOMINIOS = "dominios-observados-{ano}.json"
NOME_RESULTADO = "resultado-{ano}.json"

_SUFIXO_PARCIAL = ".part"


def caminho_artefato(pasta: Path | str, modelo: str, ano: int | None) -> Path:
    """Resolve `perfil-{ano}.json` e companhia dentro da pasta de saída.

    Ano desconhecido vira `perfil-sem-ano.json`: melhor um nome explícito do que sobrescrever o
    artefato de um ano de verdade.
    """
    return Path(pasta) / modelo.format(ano=ano if ano is not None else "sem-ano")


def escrever_json(caminho: Path | str, dados: Any) -> Path:
    """Grava `dados` como JSON legível, de forma atômica. Devolve o caminho final."""
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    parcial = caminho.with_name(caminho.name + _SUFIXO_PARCIAL)
    texto = json.dumps(dados, ensure_ascii=False, indent=2)
    try:
        parcial.write_text(texto + "\n", encoding="utf-8")
    except BaseException:
        parcial.unlink(missing_ok=True)
        raise
    parcial.replace(caminho)
    return caminho


def ler_json(caminho: Path | str) -> dict[str, Any] | None:
    """Lê um artefato gravado antes; `None` se faltar ou estiver corrompido.

    Artefato ilegível é tratado como ausente de propósito: quem chama vai regravá-lo, que é o
    desfecho certo tanto para arquivo truncado quanto para arquivo editado à mão.
    """
    try:
        dados = json.loads(Path(caminho).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return dados if isinstance(dados, dict) else None


__all__ = [
    "NOME_DOMINIOS",
    "NOME_PERFIL",
    "NOME_RESULTADO",
    "caminho_artefato",
    "escrever_json",
    "ler_json",
]
