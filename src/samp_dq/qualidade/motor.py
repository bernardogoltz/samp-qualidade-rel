"""Motor do catálogo — etapa 6 do pipeline de docs/04.

Percorre o Parquet em lotes, acumula violações e no máximo cinco exemplos por regra, e fecha o
`resultado-{ano}.json`. O perfil entra como atalho para o que já foi medido (período, encoding,
decimais sem parte inteira) e para não reler o CSV.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from samp_dq.ingest.leitura import MAX_EXEMPLOS
from samp_dq.ingest.schema import ESQUEMA_SAMP
from samp_dq.perfil import Perfil
from samp_dq.qualidade.catalogo import (
    CHAVE_ANALITICA,
    CLASSE_DA_SUBCLASSE,
    CLASSE_DO_SUBGRUPO,
    DETALHES_COM_ESTORNO,
    DOMINIOS,
    POSTOS_HORARIOS,
    REGRAS,
    SENTINELA,
    SUBGRUPOS_A,
    SUBGRUPOS_B,
)
from samp_dq.qualidade.resultado import (
    ResultadoRegra,
    ResultadoValidacao,
    agora,
    fechar_regra,
)

TAMANHO_LOTE = 250_000
LIMITE_VALOR = 80


class _Acc:
    """Contador + exemplos de uma regra, preenchido ao longo da passada."""

    __slots__ = ("avaliadas", "exemplos", "nota", "severidade", "status", "violacoes")

    def __init__(self) -> None:
        self.avaliadas = 0
        self.violacoes = 0
        self.exemplos: list[dict[str, Any]] = []
        self.status: str | None = None
        self.severidade: str | None = None
        self.nota = ""

    def avaliar(self, n: int = 1) -> None:
        self.avaliadas += n

    def violar(self, n: int = 1, **exemplo: Any) -> None:
        self.violacoes += n
        if exemplo and len(self.exemplos) < MAX_EXEMPLOS:
            self.exemplos.append({k: v for k, v in exemplo.items() if v is not None})

    def fechar(self, id_regra: str) -> ResultadoRegra:
        return fechar_regra(
            id_regra,
            self.avaliadas,
            self.violacoes,
            self.exemplos,
            status=self.status,
            severidade=self.severidade,
            nota=self.nota,
        )


def validar(
    perfil: Perfil,
    parquet: Path | str,
    *,
    hoje: date | None = None,
    regras: Iterable[str] | None = None,
) -> ResultadoValidacao:
    """Aplica o catálogo ao Parquet já perfilado e devolve o envelope."""
    motor = Motor(perfil, Path(parquet), hoje=hoje or date.today(), filtro=set(regras or ()))
    return motor.executar()


class Motor:
    def __init__(
        self,
        perfil: Perfil,
        parquet: Path,
        *,
        hoje: date,
        filtro: set[str],
    ) -> None:
        self.perfil = perfil
        self.parquet = parquet
        self.hoje = hoje
        self.filtro = filtro
        self.a: dict[str, _Acc] = {r.id: _Acc() for r in REGRAS if self._ativa(r.id)}
        self._hashes_linha: set[int] = set()
        self._hashes_chave: set[int] = set()
        self._cnpj_sigla: dict[str, set[str]] = defaultdict(set)
        self._sigla_cnpj: dict[str, set[str]] = defaultdict(set)
        self._agentes_mes: dict[str, set[str]] = defaultdict(set)
        self._cnpj_dist_ruins: set[str] = set()
        self._cnpj_acess_ruins: set[str] = set()
        self._cnpj_dist_ok: set[str] = set()
        self._cnpj_acess_ok: set[str] = set()
        self._acu003: dict[tuple[str, str], list[float]] = defaultdict(list)
        self._offset = 0

    def _ativa(self, id_regra: str) -> bool:
        return not self.filtro or id_regra in self.filtro

    def _acc(self, id_regra: str) -> _Acc | None:
        return self.a.get(id_regra)

    def executar(self) -> ResultadoValidacao:
        self._regras_de_perfil()
        if self.parquet.exists():
            self._passar_parquet()
        self._fechar_pos_passada()
        regras = tuple(self.a[r.id].fechar(r.id) for r in REGRAS if r.id in self.a)
        recorte = {
            "nulosPorCampo": dict(self.perfil.nulos),
            "cardinalidades": self.perfil.cardinalidades,
            "periodoCompetencia": {
                "min": self.perfil.competencia_min.isoformat()
                if self.perfil.competencia_min
                else None,
                "max": self.perfil.competencia_max.isoformat()
                if self.perfil.competencia_max
                else None,
            },
        }
        return ResultadoValidacao(
            arquivo=self.perfil.arquivo,
            ano=self.perfil.ano,
            gerado_em=agora(),
            linhas_totais=self.perfil.linhas,
            perfil=recorte,
            regras=regras,
            origem=self.perfil.origem.como_json(),
        )

    def _regras_de_perfil(self) -> None:
        p = self.perfil
        if acc := self._acc("DQ-VAL-001"):
            acc.avaliar()
            if p.campos_ausentes:
                acc.violar(
                    campo="cabeçalho",
                    valor=", ".join(p.campos_ausentes),
                )
                acc.nota = f"faltando {', '.join(p.campos_ausentes)}"
        if acc := self._acc("DQ-VAL-002"):
            ileg = p.normalizacoes.get("valoresIlegiveis") or {}
            ilegiveis = int(ileg.get("DatCompetencia", 0))
            ilegiveis += int(
                (p.normalizacoes.get("valoresIlegiveis") or {}).get("DatGeracaoConjuntoDados", 0)
            )
            acc.avaliar(p.linhas)
            if ilegiveis:
                acc.violar(ilegiveis, campo="datas", valor=f"{ilegiveis} valor(es) ilegível(is)")
        if acc := self._acc("DQ-VAL-014"):
            ilegiveis = int((p.normalizacoes.get("valoresIlegiveis") or {}).get("VlrMercado", 0))
            sem_inteiro = int(p.normalizacoes.get("decimaisSemParteInteira") or 0)
            acc.avaliar(p.linhas)
            if ilegiveis:
                acc.violar(ilegiveis, campo="VlrMercado", valor="ilegível")
            if sem_inteiro:
                acc.violar(sem_inteiro, campo="VlrMercado_raw", valor="sem parte inteira")
        if acc := self._acc("DQ-VAL-015"):
            if p.normalizacoes.get("encodingConvertido"):
                acc.avaliar()
            else:
                acc.status = "nao_aplicavel"
                acc.nota = "perfil veio do Parquet; a leitura do CSV não foi presenciada"
        if acc := self._acc("DQ-COM-004"):
            self._cobertura_mensal(acc)
        if acc := self._acc("DQ-CON-004"):
            self._geracao_vs_competencia(acc)
        if acc := self._acc("DQ-CON-008"):
            acc.avaliar(p.linhas)
            n = p.cardinalidades.get("DatGeracaoConjuntoDados", 0)
            if n > 1:
                acc.violar(n, campo="DatGeracaoConjuntoDados", valor=f"{n} datas distintas")
        self._atualidade()
        if acc := self._acc("DQ-ACU-004"):
            acc.status = "nao_aplicavel"
            acc.nota = "exige a série do ano anterior"
        if acc := self._acc("DQ-ACU-005"):
            acc.status = "nao_aplicavel"
            acc.nota = "exige o total anual do ano anterior"

    def _cobertura_mensal(self, acc: _Acc) -> None:
        acc.avaliar()
        ano = self.perfil.ano
        competencias = [
            date.fromisoformat(c) if isinstance(c, str) else c for c in self.perfil.competencias()
        ]
        meses = {d.month for d in competencias if ano is None or d.year == ano}
        if ano is None:
            acc.status = "nao_aplicavel"
            return
        if ano < self.hoje.year:
            faltando = [m for m in range(1, 13) if m not in meses]
            if faltando:
                acc.violar(
                    campo="DatCompetencia",
                    valor=f"faltando mês(es) {faltando}",
                )
        else:
            alvo = set(range(1, max(self.hoje.month - 1, 1) + 1))
            faltando = sorted(alvo - meses)
            if faltando:
                acc.violar(
                    campo="DatCompetencia",
                    valor=f"faltando mês(es) {faltando} no ano corrente",
                )

    def _geracao_vs_competencia(self, acc: _Acc) -> None:
        acc.avaliar()
        geracao = self.perfil.contagens.get("DatGeracaoConjuntoDados")
        if not geracao or not geracao.contagens or self.perfil.competencia_max is None:
            return
        datas = [date.fromisoformat(k) for k in geracao.contagens]
        if max(datas) < self.perfil.competencia_max:
            acc.violar(
                campo="DatGeracaoConjuntoDados",
                valor=f"{max(datas).isoformat()} < {self.perfil.competencia_max.isoformat()}",
            )

    def _atualidade(self) -> None:
        ano_corrente = self.perfil.ano == self.hoje.year
        if acc := self._acc("DQ-ATU-001"):
            if not ano_corrente:
                acc.status = "nao_aplicavel"
                acc.nota = "só se aplica ao arquivo do ano corrente"
            else:
                acc.avaliar()
                ultimo = self.perfil.competencia_max
                minimo = _mes_menos(date(self.hoje.year, self.hoje.month, 1), 2)
                if ultimo is None or ultimo < minimo:
                    acc.violar(
                        campo="DatCompetencia",
                        valor=ultimo.isoformat() if ultimo else "ausente",
                    )
        if acc := self._acc("DQ-ATU-002"):
            if not ano_corrente:
                acc.status = "nao_aplicavel"
                acc.nota = "só se aplica ao arquivo do ano corrente"
            else:
                acc.avaliar()
                texto = self.perfil.origem.ultima_modificacao_ckan
                if not texto:
                    acc.violar(campo="lastModifiedCkan", valor="ausente")
                else:
                    modificado = datetime.fromisoformat(texto.replace("Z", "+00:00")).date()
                    if (self.hoje - modificado).days > 45:
                        acc.violar(campo="lastModifiedCkan", valor=texto)
        if acc := self._acc("DQ-ATU-003"):
            if not ano_corrente:
                acc.status = "nao_aplicavel"
                acc.nota = "só se aplica ao arquivo do ano corrente"
            else:
                acc.avaliar()
                geracao = self.perfil.contagens.get("DatGeracaoConjuntoDados")
                if not geracao or not geracao.contagens:
                    acc.violar(campo="DatGeracaoConjuntoDados", valor="ausente")
                else:
                    datas = [date.fromisoformat(k) for k in geracao.contagens]
                    if (self.hoje - max(datas)).days > 45:
                        acc.violar(
                            campo="DatGeracaoConjuntoDados",
                            valor=max(datas).isoformat(),
                        )

    def _passar_parquet(self) -> None:
        arquivo = pq.ParquetFile(self.parquet)
        for lote in arquivo.iter_batches(batch_size=TAMANHO_LOTE):
            quadro = pa.Table.from_batches([lote]).to_pandas(types_mapper=pd.ArrowDtype)
            self._bloco(quadro)
            self._offset += len(quadro)

    def _bloco(self, df: pd.DataFrame) -> None:
        n = len(df)
        linhas = np.arange(self._offset + 1, self._offset + n + 1)
        self._completude(df, linhas)
        self._dominios(df, linhas)
        self._formatos(df, linhas)
        self._unicidade(df, linhas)
        self._consistencia(df, linhas)
        self._acuracia_bloco(df, linhas)

    def _completude(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any]) -> None:
        if acc := self._acc("DQ-COM-001"):
            acc.avaliar(len(df))
            falta = pd.Series(False, index=df.index)
            for campo in ESQUEMA_SAMP.nomes:
                if campo not in df.columns:
                    falta[:] = True
                    break
                col = df[campo]
                falta = falta | col.isna() | _vazio(col)
            self._marcar(acc, falta, df, linhas, campo="registro")
        if acc := self._acc("DQ-COM-002"):
            acc.avaliar(len(df))
            if "VlrMercado" in df.columns:
                self._marcar(acc, df["VlrMercado"].isna(), df, linhas, campo="VlrMercado")
        if acc := self._acc("DQ-COM-003"):
            acc.avaliar(len(df))
            if "DatCompetencia" in df.columns:
                self._marcar(acc, df["DatCompetencia"].isna(), df, linhas, campo="DatCompetencia")
        if (acc := self._acc("DQ-COM-005")) and "DatCompetencia" in df.columns:
            comp = df["DatCompetencia"].astype("string")
            cnpj = df["NumCNPJAgenteDistribuidora"].astype("string")
            for mes, grupo in pd.DataFrame({"m": comp, "c": cnpj}).groupby("m", dropna=True):
                if mes and str(mes) != "<NA>":
                    self._agentes_mes[str(mes)].update(x for x in grupo["c"] if x and x != "<NA>")

    def _dominios(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any]) -> None:
        pares = (
            ("DQ-VAL-003", "DscSubGrupoTarifario", False, False),
            ("DQ-VAL-004", "NomTipoMercado", False, False),
            ("DQ-VAL-005", "DscClasseConsumoMercado", False, False),
            ("DQ-VAL-006", "DscSubClasseConsumidor", False, False),
            ("DQ-VAL-007", "DscDetalheConsumidor", False, False),
            ("DQ-VAL-008", "DscPostoTarifario", False, False),
            ("DQ-VAL-009", "DscOpcaoEnergia", True, False),
            ("DQ-VAL-016", "DscModalidadeTarifaria", False, True),
        )
        for id_regra, campo, ignora_caixa, aberto in pares:
            acc = self._acc(id_regra)
            if acc is None or campo not in df.columns:
                continue
            dicionario, observados = DOMINIOS[campo]
            col = df[campo].astype("string")
            presente = col.notna() & (col != "") & (col != "<NA>")
            acc.avaliar(int(presente.sum()))
            valores = col.where(presente)
            if ignora_caixa:
                mapa_dic = {v.casefold(): v for v in dicionario}
                mapa_obs = {v.casefold(): v for v in observados}
                chave = valores.str.casefold()
                no_dic = chave.isin(set(mapa_dic))
                no_obs = chave.isin(set(mapa_obs))
                caixa_ok = valores.isin(dicionario) | valores.isin(observados)
                divergente = presente & no_dic & ~caixa_ok
                if divergente.any():
                    acc.severidade = "aviso"
                    self._marcar(acc, divergente, df, linhas, campo=campo)
            else:
                no_dic = valores.isin(dicionario)
                no_obs = valores.isin(observados)
            fora = presente & ~no_dic & ~no_obs
            so_obs = presente & ~no_dic & no_obs
            if fora.any():
                self._marcar(acc, fora, df, linhas, campo=campo)
            elif so_obs.any():
                acc.status = "aviso_defasagem"
                acc.severidade = "aviso"
                self._marcar(acc, so_obs, df, linhas, campo=campo)
            elif aberto and so_obs.any():
                acc.severidade = "aviso"
                self._marcar(acc, so_obs, df, linhas, campo=campo)

    def _formatos(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any]) -> None:
        if (acc := self._acc("DQ-VAL-010")) and "NumCNPJAgenteDistribuidora" in df.columns:
            col = df["NumCNPJAgenteDistribuidora"].astype("string")
            presente = col.notna() & (col != "") & (col != "<NA>")
            acc.avaliar(int(presente.sum()))
            ok = col.str.fullmatch(r"\d{14}").fillna(False)
            self._marcar(acc, presente & ~ok, df, linhas, campo="NumCNPJAgenteDistribuidora")
        if (acc := self._acc("DQ-VAL-011")) and "NumCNPJAgenteAcessante" in df.columns:
            col = df["NumCNPJAgenteAcessante"].astype("string")
            cru = (
                df["NumCNPJAgenteAcessante_raw"].astype("string")
                if "NumCNPJAgenteAcessante_raw" in df.columns
                else col
            )
            presente = col.notna() & (col != "") & (col != "<NA>") & (col != SENTINELA)
            acc.avaliar(int(presente.sum()))
            cnpj = col.str.fullmatch(r"\d{14}").fillna(False)
            cpf = col.str.fullmatch(r"\d{11}").fillna(False)
            espaco = presente & cru.str.match(r".*\s+$", na=False)
            ruim = presente & ~cnpj & ~cpf
            self._marcar(acc, ruim | cpf | espaco, df, linhas, campo="NumCNPJAgenteAcessante")
        if acc := self._acc("DQ-VAL-012"):
            for campo in ESQUEMA_SAMP:
                if not campo.tamanho_max or campo.nome not in df.columns:
                    continue
                col = df[campo.nome].astype("string")
                presente = col.notna() & (col != "") & (col != "<NA>")
                acc.avaliar(int(presente.sum()))
                longo = presente & (col.str.len() > campo.tamanho_max)
                self._marcar(acc, longo, df, linhas, campo=campo.nome)
        if (acc := self._acc("DQ-VAL-013")) and "IdeNucleoCeg" in df.columns:
            col = df["IdeNucleoCeg"]
            presente = col.notna()
            acc.avaliar(int(presente.sum()))
            num = _como_float(col)
            ruim = presente & ((num < 0) | (num > 99_999) | num.isna())
            self._marcar(acc, ruim, df, linhas, campo="IdeNucleoCeg")
        if (acc := self._acc("DQ-CON-005")) and "NumCNPJAgenteDistribuidora" in df.columns:
            self._digito_cnpj(df, linhas, acc)

    def _digito_cnpj(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any], acc: _Acc) -> None:
        dist = df["NumCNPJAgenteDistribuidora"].astype("string")
        presente = dist.str.fullmatch(r"\d{14}").fillna(False)
        acc.avaliar(int(presente.sum()))
        for valor in dist[presente].unique().tolist():
            texto = str(valor)
            if texto in self._cnpj_dist_ok or texto in self._cnpj_dist_ruins:
                continue
            (self._cnpj_dist_ok if _cnpj_valido(texto) else self._cnpj_dist_ruins).add(texto)
        if self._cnpj_dist_ruins:
            self._marcar(
                acc,
                dist.isin(self._cnpj_dist_ruins),
                df,
                linhas,
                campo="NumCNPJAgenteDistribuidora",
            )
        if "NumCNPJAgenteAcessante" in df.columns:
            acess = df["NumCNPJAgenteAcessante"].astype("string")
            cnpj = acess.str.fullmatch(r"\d{14}").fillna(False)
            acc.avaliar(int(cnpj.sum()))
            for valor in acess[cnpj].unique().tolist():
                texto = str(valor)
                if texto in self._cnpj_acess_ok or texto in self._cnpj_acess_ruins:
                    continue
                (self._cnpj_acess_ok if _cnpj_valido(texto) else self._cnpj_acess_ruins).add(texto)
            if self._cnpj_acess_ruins:
                self._marcar(
                    acc,
                    acess.isin(self._cnpj_acess_ruins),
                    df,
                    linhas,
                    campo="NumCNPJAgenteAcessante",
                )

    def _unicidade(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any]) -> None:
        if acc := self._acc("DQ-UNI-001"):
            acc.avaliar(len(df))
            self._duplicatas(acc, df, linhas, list(ESQUEMA_SAMP.nomes), self._hashes_linha)
        if acc := self._acc("DQ-UNI-002"):
            cols = [c for c in CHAVE_ANALITICA if c in df.columns]
            acc.avaliar(len(df))
            self._duplicatas(acc, df, linhas, cols, self._hashes_chave)
        if acc := self._acc("DQ-UNI-003"):
            acc.avaliar(len(df))
            if (
                "NumCNPJAgenteDistribuidora" in df.columns
                and "SigAgenteDistribuidora" in df.columns
            ):
                pares = (
                    df[["NumCNPJAgenteDistribuidora", "SigAgenteDistribuidora"]]
                    .astype("string")
                    .drop_duplicates()
                )
                for cnpj, sigla in pares.itertuples(index=False, name=None):
                    if not cnpj or cnpj == "<NA>" or not sigla or sigla == "<NA>":
                        continue
                    self._cnpj_sigla[str(cnpj)].add(str(sigla))
                    self._sigla_cnpj[str(sigla)].add(str(cnpj))

    def _duplicatas(
        self,
        acc: _Acc,
        df: pd.DataFrame,
        linhas: np.ndarray[Any, Any],
        cols: list[str],
        vistos: set[int],
    ) -> None:
        presentes = [c for c in cols if c in df.columns]
        if not presentes:
            return
        base = df[presentes].astype("string")
        hashes = pd.util.hash_pandas_object(base, index=False).to_numpy()
        dup = np.zeros(len(df), dtype=bool)
        for i, h in enumerate(hashes):
            chave = int(h)
            if chave in vistos:
                dup[i] = True
            else:
                vistos.add(chave)
        self._marcar(acc, pd.Series(dup, index=df.index), df, linhas, campo="chave")

    def _consistencia(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any]) -> None:
        if (acc := self._acc("DQ-CON-001")) and "DatCompetencia" in df.columns:
            acc.avaliar(len(df))
            ano = self.perfil.ano
            if ano is not None:
                anos = pd.to_datetime(df["DatCompetencia"], errors="coerce").dt.year
                tipo = (
                    df["NomTipoMercado"].astype("string")
                    if "NomTipoMercado" in df.columns
                    else pd.Series("", index=df.index)
                )
                refat = tipo.str.contains("Refaturamento", case=False, na=False)
                ruim = anos.notna() & (anos != ano) & ~refat
                self._marcar(acc, ruim, df, linhas, campo="DatCompetencia")
        if (acc := self._acc("DQ-CON-002")) and (
            "DscSubGrupoTarifario" in df.columns and "DscClasseConsumoMercado" in df.columns
        ):
            sub = df["DscSubGrupoTarifario"].astype("string")
            classe = df["DscClasseConsumoMercado"].astype("string")
            acc.avaliar(len(df))
            ruim = pd.Series(False, index=df.index)
            for codigo, classes in CLASSE_DO_SUBGRUPO.items():
                ruim = ruim | ((sub == codigo) & ~classe.isin(classes) & classe.notna())
            self._marcar(acc, ruim, df, linhas, campo="DscClasseConsumoMercado")
        if (acc := self._acc("DQ-CON-003")) and (
            "DscSubClasseConsumidor" in df.columns and "DscClasseConsumoMercado" in df.columns
        ):
            sub = df["DscSubClasseConsumidor"].astype("string")
            classe = df["DscClasseConsumoMercado"].astype("string")
            acc.avaliar(len(df))
            ruim = pd.Series(False, index=df.index)
            for trecho, exigida in CLASSE_DA_SUBCLASSE:
                ruim = ruim | (sub.str.contains(trecho, case=False, na=False) & (classe != exigida))
            self._marcar(acc, ruim, df, linhas, campo="DscSubClasseConsumidor")
        if (acc := self._acc("DQ-CON-006")) and "IdeNucleoCeg" in df.columns:
            acc.avaliar(len(df))
            ceg = _como_float(df["IdeNucleoCeg"]).fillna(0)
            opcao = (
                df["DscOpcaoEnergia"].astype("string").str.casefold()
                if "DscOpcaoEnergia" in df.columns
                else pd.Series("", index=df.index)
            )
            tipo = (
                df["NomTipoMercado"].astype("string").str.casefold()
                if "NomTipoMercado" in df.columns
                else pd.Series("", index=df.index)
            )
            gerador = opcao.eq("geração") | tipo.str.contains("compensação", na=False)
            ruim = (ceg > 0) & ~gerador
            self._marcar(acc, ruim, df, linhas, campo="IdeNucleoCeg")
        if (acc := self._acc("DQ-CON-007")) and (
            "DscPostoTarifario" in df.columns
            and "DscSubGrupoTarifario" in df.columns
            and "DscModalidadeTarifaria" in df.columns
        ):
            acc.avaliar(len(df))
            posto = df["DscPostoTarifario"].astype("string")
            sub = df["DscSubGrupoTarifario"].astype("string")
            mod = df["DscModalidadeTarifaria"].astype("string")
            horario = posto.isin(POSTOS_HORARIOS)
            grupo_a = sub.isin(SUBGRUPOS_A)
            grupo_b = sub.isin(SUBGRUPOS_B)
            branca = mod.eq("Branca")
            ruim_a = horario & ~grupo_a & ~(grupo_b & branca)
            ruim_b = grupo_b & ~branca & (posto != SENTINELA)
            self._marcar(acc, ruim_a | ruim_b, df, linhas, campo="DscPostoTarifario")

    def _acuracia_bloco(self, df: pd.DataFrame, linhas: np.ndarray[Any, Any]) -> None:
        if "VlrMercado" not in df.columns:
            return
        valores = _como_float(df["VlrMercado"])
        detalhe = (
            df["DscDetalheMercado"].astype("string")
            if "DscDetalheMercado" in df.columns
            else pd.Series("", index=df.index)
        )
        if acc := self._acc("DQ-ACU-001"):
            pct = detalhe.str.contains("%", na=False)
            acc.avaliar(int(pct.sum()))
            ruim = pct & ((valores < 0) | (valores > 100))
            self._marcar(acc, ruim, df, linhas, campo="VlrMercado")
        if acc := self._acc("DQ-ACU-002"):
            tipo = (
                df["NomTipoMercado"].astype("string")
                if "NomTipoMercado" in df.columns
                else pd.Series("", index=df.index)
            )
            refat = tipo.str.contains("Refaturamento", case=False, na=False)
            admite = detalhe.apply(_admite_estorno)
            presente = valores.notna()
            acc.avaliar(int(presente.sum()))
            ruim = presente & (valores < 0) & ~admite & ~refat
            self._marcar(acc, ruim, df, linhas, campo="VlrMercado")
        if self._acc("DQ-ACU-003") is not None and "DscSubGrupoTarifario" in df.columns:
            sub = df["DscSubGrupoTarifario"].astype("string")
            grupo = pd.DataFrame({"d": detalhe, "s": sub, "v": valores}).dropna()
            for (det, subg), parte in grupo.groupby(["d", "s"], sort=False):
                self._acu003[(str(det), str(subg))].extend(parte["v"].astype(float).tolist())

    def _fechar_pos_passada(self) -> None:
        if acc := self._acc("DQ-UNI-003"):
            conflitos = [
                (cnpj, sorted(siglas))
                for cnpj, siglas in self._cnpj_sigla.items()
                if len(siglas) > 1
            ] + [
                (sigla, sorted(cnpjs))
                for sigla, cnpjs in self._sigla_cnpj.items()
                if len(cnpjs) > 1
            ]
            if conflitos:
                for chave, outros in conflitos[:MAX_EXEMPLOS]:
                    acc.violar(campo="agente", valor=f"{chave} → {outros}")
                acc.violacoes = len(conflitos)
        if acc := self._acc("DQ-COM-005"):
            meses = sorted(self._agentes_mes)
            acc.avaliar(max(len(meses) - 1, 0))
            for anterior, atual in pairwise(meses):
                faltando = self._agentes_mes[anterior] - self._agentes_mes[atual]
                if faltando:
                    acc.violar(
                        campo="NumCNPJAgenteDistribuidora",
                        valor=f"{atual}: ausentes {sorted(faltando)[:5]}",
                    )
        if acc := self._acc("DQ-ACU-003"):
            total = 0
            ruins = 0
            for (detalhe, subgrupo), serie in self._acu003.items():
                arr = np.asarray(serie, dtype="float64")
                total += len(arr)
                if len(arr) < 8:
                    continue
                mediana = float(np.median(arr))
                mad = float(np.median(np.abs(arr - mediana)))
                if mad == 0:
                    continue
                limiar = 6 * 1.4826 * mad
                n_out = int(np.sum(np.abs(arr - mediana) > limiar))
                if n_out:
                    ruins += n_out
                    acc.violar(
                        n_out,
                        campo="VlrMercado",
                        valor=f"{detalhe}/{subgrupo}: {n_out} além de 6 MAD",
                    )
            acc.avaliar(total)
            acc.violacoes = ruins

    def _marcar(
        self,
        acc: _Acc,
        mascara: pd.Series[Any],
        df: pd.DataFrame,
        linhas: np.ndarray[Any, Any],
        *,
        campo: str,
    ) -> None:
        if not bool(mascara.fillna(False).any()):
            return
        idx = np.flatnonzero(mascara.fillna(False).to_numpy())
        acc.violacoes += len(idx)
        for i in idx:
            if len(acc.exemplos) >= MAX_EXEMPLOS:
                break
            valor = df.iloc[i][campo] if campo in df.columns else None
            acc.exemplos.append(
                {
                    "linha": int(linhas[i]),
                    "campo": campo,
                    "valor": _texto(valor),
                }
            )


def _como_float(col: pd.Series[Any]) -> pd.Series[Any]:
    """Decimal Arrow vira float preservando nulos e o índice — `to_numeric` encurta a série."""
    valores = np.array(
        [float(v) if v is not None and pd.notna(v) else np.nan for v in col.tolist()],
        dtype="float64",
    )
    return pd.Series(valores, index=col.index)


def _vazio(col: pd.Series[Any]) -> pd.Series[Any]:
    tipo = str(col.dtype)
    if "string" not in tipo and tipo not in {"str", "object"}:
        return pd.Series(False, index=col.index)
    texto = col.astype("string")
    return texto.eq("") | texto.eq("<NA>")


def _texto(valor: Any) -> str:
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return ""
    texto = valor.isoformat() if isinstance(valor, date) else str(valor)
    return texto if len(texto) <= LIMITE_VALOR else texto[:LIMITE_VALOR] + "…"


def _admite_estorno(detalhe: object) -> bool:
    texto = str(detalhe)
    return any(p.casefold() in texto.casefold() for p in DETALHES_COM_ESTORNO)


def _mes_menos(competencia: date, meses: int) -> date:
    ano, mes = competencia.year, competencia.month - meses
    while mes <= 0:
        mes += 12
        ano -= 1
    return date(ano, mes, 1)


def _cnpj_valido(digitos: str) -> bool:
    if len(digitos) != 14 or not digitos.isdigit() or len(set(digitos)) == 1:
        return False

    def dv(base: str, pesos: tuple[int, ...]) -> int:
        soma = sum(int(d) * p for d, p in zip(base, pesos, strict=True))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    if dv(digitos[:12], (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)) != int(digitos[12]):
        return False
    return dv(digitos[:13], (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)) == int(digitos[13])


__all__ = ["TAMANHO_LOTE", "validar"]
