"""Camada de normalizacao semantica da NetObs (v6).

Implementa, sobre o catalogo canonico, o que o modelo formal descreve:
  * significado de medicao de cada metrica canonica: (camada, unidade, dimensoes, grandeza,
    estatistica, semantica temporal); duas metricas so sao comparaveis se as seis componentes coincidem;
  * funcao de normalizacao phi_r por fonte r: mapeia atributos brutos para metricas canonicas com
    fator de escala (conversao de unidade); atributos sem regra sao perdas (TLR);
  * validacao de amostras: metrica fora do catalogo, camada/unidade/dimensoes incompativeis e metadados
    de execucao inconsistentes sao rejeitados com motivo explicito;
  * supressao de duplicatas entre fontes (mesma entidade, mesma classe, mesma janela);
  * agregacao que recusa misturar classes distintas.
Nada aqui depende de rede ou de Docker: e puro Python e pode ser desligado (enabled=False) para a ablacao.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from platform.backend.schemas import MetricDefinition, MetricSample

# Aliases declarados: nomes diferentes que medem a mesma grandeza fisica.
QUANTITY_ALIASES: Dict[str, str] = {
    "latency": "icmp_rtt",
    "host_ping_latency": "icmp_rtt",
    "rtt": "icmp_rtt",
    "cpu_util": "cpu_util",
    "docker_cpu_util": "cpu_util",
    "mem_util": "mem_util",
    "docker_mem_util": "mem_util",
}

_STAT_SUFFIX = {"p50": "p50", "p90": "p90", "p95": "p95", "p99": "p99", "avg": "mean", "mean": "mean",
                "max": "max", "min": "min", "p999": "p999"}
_UNIT_TOKENS = {"ms", "s", "pct", "mbps", "bps", "pps", "bytes", "packets", "sec", "us", "total"}


@dataclass(frozen=True)
class Meaning:
    layer: str
    unit: str
    dims: Tuple[str, ...]
    quantity: str
    statistic: str
    temporal: str

    def label(self) -> str:
        return f"{self.layer}/{self.quantity}/{self.statistic}/{self.temporal}[{self.unit}|{','.join(self.dims)}]"


def meaning_of(d: MetricDefinition) -> Meaning:
    toks = d.name.split("_")
    stat = "instant"
    while toks and toks[-1] in _STAT_SUFFIX:
        stat = _STAT_SUFFIX[toks.pop()]
    if d.kind == "summary" and stat == "instant":
        stat = "summary"
    kind = d.kind or "gauge"
    temporal = "cumulative" if kind == "counter" else ("window" if stat not in ("instant",) or kind in ("summary", "ratio") else "instant")
    qtoks = [t for t in toks if t not in _UNIT_TOKENS]
    quantity = "_".join(qtoks) or d.name
    quantity = QUANTITY_ALIASES.get(quantity, quantity)
    return Meaning(layer=str(d.layer), unit=str(d.unit), dims=tuple(sorted(d.dimensions)),
                   quantity=quantity, statistic=stat, temporal=temporal)


def comparable(a: Meaning, b: Meaning) -> bool:
    return a == b


@dataclass(frozen=True)
class Rule:
    canonical: str
    scale: float = 1.0
    offset: float = 0.0
    raw_unit: str = ""


@dataclass
class Rejection:
    reason: str
    metric: str
    detail: str = ""


@dataclass
class IngestResult:
    accepted: List[MetricSample] = field(default_factory=list)
    rejected: List[Rejection] = field(default_factory=list)
    duplicates: int = 0
    dropped_attrs: List[str] = field(default_factory=list)

    def counts(self) -> Dict[str, int]:
        c: Dict[str, int] = defaultdict(int)
        for r in self.rejected:
            c[f"rejected_{r.reason}"] += 1
        c["accepted"] = len(self.accepted)
        c["duplicates"] = self.duplicates
        c["dropped_attrs"] = len(self.dropped_attrs)
        return dict(c)


def _has_dim(s: MetricSample, k: str) -> bool:
    """Uma dimensao declarada esta presente em labels/details ou e derivavel da propria amostra."""
    lab, det = s.labels or {}, s.details or {}
    if lab.get(k) not in (None, "") or det.get(k) not in (None, ""):
        return True
    if k == "node":
        return bool(s.node)
    if k == "src":
        return bool(s.node)                      # para metricas de host/no, a origem e o proprio no
    if k == "interface":
        return ":" in s.node or det.get("iface") is not None
    if k == "link":
        return "->" in s.node
    if k == "switch":
        return bool(s.node)
    if k == "controller":
        return bool(s.node)
    if k == "flow":
        return det.get("flow_id") is not None or lab.get("flow_id") is not None
    if k == "container":
        return det.get("container") is not None or bool(s.node)
    return False


class MixedClassError(ValueError):
    pass


class SemanticLayer:
    """Valida, converte e desduplica amostras. enabled=False reproduz o comportamento anterior (aceita tudo)."""

    def __init__(self, definitions: Sequence[MetricDefinition], enabled: bool = True,
                 dup_window_s: float = 1.0, require_run: bool = True) -> None:
        self.enabled = enabled
        self.defs: Dict[str, MetricDefinition] = {d.name: d for d in definitions}
        self.meanings: Dict[str, Meaning] = {n: meaning_of(d) for n, d in self.defs.items()}
        self.mappings: Dict[str, Dict[str, Rule]] = {}
        self.dup_window_s = dup_window_s
        self.require_run = require_run
        self.runs: Dict[str, Dict[str, str]] = {}      # run_id -> {experiment_id, topology_id}
        self._seen: Dict[Tuple, Tuple[float, str]] = {}  # (node, classe, dims) -> (ts, fonte)

    # ── classes de equivalencia ──────────────────────────────────────────────
    def classes(self) -> Dict[Meaning, List[str]]:
        out: Dict[Meaning, List[str]] = defaultdict(list)
        for n, m in self.meanings.items():
            out[m].append(n)
        return dict(out)

    def class_of(self, metric: str) -> Optional[Meaning]:
        return self.meanings.get(metric)

    # ── registro ─────────────────────────────────────────────────────────────
    def register_source(self, source: str, rules: Mapping[str, Rule]) -> None:
        for raw, rule in rules.items():
            if rule.canonical not in self.defs:
                raise KeyError(f"regra {source}.{raw} aponta para metrica fora do catalogo: {rule.canonical}")
        self.mappings[source] = dict(rules)

    def register_run(self, run_id: str, experiment_id: str, topology_id: str = "") -> None:
        self.runs[run_id] = {"experiment_id": experiment_id, "topology_id": topology_id}

    # ── phi_r: atributos brutos -> amostras canonicas ───────────────────────
    def normalize_raw(self, source: str, node: str, ts: datetime, raw: Mapping[str, float],
                      labels: Optional[Mapping[str, Any]] = None) -> IngestResult:
        res = IngestResult()
        rules = self.mappings.get(source, {})
        for attr, val in raw.items():
            rule = rules.get(attr)
            if rule is None:
                res.dropped_attrs.append(attr)
                continue
            d = self.defs[rule.canonical]
            s = MetricSample(timestamp=ts, node=node, layer=d.layer, metric=rule.canonical,
                             value=float(val) * rule.scale + rule.offset, labels=dict(labels or {}),
                             details={"source": source, "raw_attr": attr})
            self._admit(s, source, res)
        return res

    # ── amostras ja canonicas (ex.: /metrics/samples) ───────────────────────
    def ingest(self, samples: Iterable[MetricSample], source: str = "api") -> IngestResult:
        res = IngestResult()
        for s in samples:
            self._admit(s, source, res)
        return res

    def _admit(self, s: MetricSample, source: str, res: IngestResult) -> None:
        if not self.enabled:
            res.accepted.append(s)
            return
        d = self.defs.get(s.metric)
        if d is None:
            res.rejected.append(Rejection("unknown_metric", s.metric))
            return
        if d.layer is not None and s.layer != d.layer:
            res.rejected.append(Rejection("layer_mismatch", s.metric, f"{s.layer}!={d.layer}"))
            return
        unit = (s.details or {}).get("unit")
        if unit is not None and d.unit is not None and str(unit) != str(d.unit):
            res.rejected.append(Rejection("unit_mismatch", s.metric, f"{unit}!={d.unit}"))
            return
        missing = [k for k in d.dimensions if not _has_dim(s, k)]
        if missing:
            res.rejected.append(Rejection("missing_dimension", s.metric, ",".join(missing)))
            return
        run_id = (s.labels or {}).get("run_id")
        if self.require_run and self.runs:
            if run_id is None:
                res.rejected.append(Rejection("missing_run", s.metric))
                return
            r = self.runs.get(str(run_id))
            if r is None:
                res.rejected.append(Rejection("unknown_run", s.metric, str(run_id)))
                return
            exp = (s.labels or {}).get("experiment_id")
            if exp is not None and str(exp) != r["experiment_id"]:
                res.rejected.append(Rejection("run_experiment_mismatch", s.metric, f"{run_id}:{exp}"))
                return
        # duplicata: mesma entidade, mesma classe e mesmas dimensoes na janela
        m = self.meanings[s.metric]
        dimkey = tuple(sorted((k, str((s.labels or {}).get(k))) for k in m.dims if k in (s.labels or {})))
        key = (s.node, m, dimkey, str(run_id))
        ts = s.timestamp.timestamp()
        prev = self._seen.get(key)
        if prev is not None and abs(ts - prev[0]) <= self.dup_window_s and prev[1] != source:
            res.duplicates += 1
            return
        self._seen[key] = (ts, source)
        res.accepted.append(s)

    # ── agregacao ────────────────────────────────────────────────────────────
    def aggregate(self, samples: Sequence[MetricSample], how: str = "mean") -> float:
        if not samples:
            raise ValueError("sem amostras")
        if self.enabled:
            cls = {self.meanings[s.metric] for s in samples if s.metric in self.meanings}
            if len(cls) > 1:
                raise MixedClassError("agregacao entre classes distintas: " + "; ".join(sorted(c.label() for c in cls)))
        vals = [s.value for s in samples]
        return sum(vals) / len(vals) if how == "mean" else (max(vals) if how == "max" else sum(vals))

    # ── cobertura ────────────────────────────────────────────────────────────
    def observed_pairs(self, samples: Iterable[MetricSample]) -> set:
        return {(s.node, self.meanings[s.metric]) for s in samples if s.metric in self.meanings}


def oci(required: set, observed: set) -> float:
    return len(required & observed) / len(required) if required else 1.0
