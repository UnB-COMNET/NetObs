"use client";

import { useEffect, useMemo, useState } from "react";
import { AreaMini } from "@/components/viz/AreaMini";
import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card } from "@/components/ui/Card";
import { Input, Select } from "@/components/ui/Input";
import { MetricInventoryTable } from "@/features/dashboard/MetricInventoryTable";
import { useMetricSeriesMap } from "@/hooks/useMetricSeriesMap";
import {
  CATEGORIES,
  LAYERS,
  TYPES,
  getLayerMetrics,
  prettyEntityKind,
  type EntityKind,
  type MetricCategory,
  type MetricDimension,
  type MetricLayer,
  type MetricSpec,
  type MetricType,
} from "@/lib/metricsCatalog";
import { cn } from "@/lib/cn";
import { API_BASE, topologyApi, type MetricSample, type Topology } from "@/lib/api";
import { useSSE } from "@/hooks/useSSE";

// Classifica um node real do backend no EntityKind correspondente
function classifyNode(node: string): EntityKind | null {
  const n = node.toLowerCase();
  // Ignorar interfaces/sub-nodes (contêm ":" ou "->") e nodes genéricos
  if (n.includes(":") || n.includes("->")) return null;
  if (["probe", "edge", "gw", "aaa", "platform-topology"].includes(n)) return null;
  if (n.startsWith("h") && /^h\d+$/.test(n)) return "host";
  if (n.startsWith("s") && /^s\d+$/.test(n)) return "switch";
  if (n.startsWith("c") && /^c\d+$/.test(n)) return "controller";
  return null;
}

function defaultLayerForEntity(kind: EntityKind): MetricLayer {
  if (kind === "host") return "L3";
  if (kind === "controller") return "Control Plane";
  return "Dataplane";
}

function isAll(v: string): v is "all" {
  return v === "all";
}

function layerLabel(layer: MetricLayer | "all") {
  if (layer === "all") return "Todas";
  if (layer === "Control Plane") return "Control";
  if (layer === "Dataplane") return "Data";
  return layer;
}

function inferDimensionLabel({
  dimension,
  entityKind,
  entityId,
}: {
  dimension: MetricDimension;
  entityKind: EntityKind;
  entityId: string;
}) {
  if (dimension === "node") return `Node: ${entityId}`;

  if (entityKind === "controller") return "Interface: mgmt0";
  if (entityKind === "host") return `Interface: ${entityId}-eth0`;
  return "Interface: eth1";
}

function downloadTextFile({ filename, content, mime }: { filename: string; content: string; mime: string }) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function toLayerCsv({
  entityKind,
  entityId,
  layer,
  specs,
  currentValues,
}: {
  entityKind: EntityKind;
  entityId: string;
  layer: MetricLayer | "all";
  specs: MetricSpec[];
  currentValues: Record<string, string>;
}) {
  const headers = [
    "entityKind",
    "entityId",
    "layer",
    "metric",
    "key",
    "category",
    "type",
    "unit",
    "dimension",
    "value",
  ];

  const esc = (v: string) => {
    const s = v ?? "";
    if (/[\n\r,\"]/g.test(s)) return `"${s.replaceAll("\"", "\"\"")}"`;
    return s;
  };

  const lines = [headers.join(",")];
  for (const spec of specs) {
    const dim = inferDimensionLabel({ dimension: spec.dimension, entityKind, entityId });
    lines.push(
      [
        entityKind,
        entityId,
        layer,
        spec.name,
        spec.key,
        spec.category,
        spec.type,
        spec.unit ?? "—",
        dim,
        currentValues[spec.key] ?? "—",
      ]
        .map(esc)
        .join(","),
    );
  }
  return lines.join("\n");
}

function categoryBadgeTone(category: MetricCategory): "neutral" | "ok" | "danger" {
  if (category === "Saúde") return "ok";
  if (category === "Erros") return "danger";
  return "neutral";
}

function formatMetricValue(spec: MetricSpec, raw: number) {
  if (spec.kind === "bool") return raw > 0.5 ? "1" : "0";

  if (spec.unit === "%") return `${raw.toFixed(0)}%`;
  if (spec.unit === "ms") return `${raw.toFixed(1)} ms`;
  if (spec.unit === "dBm") return `${raw.toFixed(2)} dBm`;
  if (spec.unit === "Mbps") {
    if (raw >= 1000) return `${(raw / 1000).toFixed(2)} Gbps`;
    return `${raw.toFixed(1)} Mbps`;
  }
  if (spec.unit === "bps") {
    if (raw >= 1e9) return `${(raw / 1e9).toFixed(2)} Gbps`;
    if (raw >= 1e6) return `${(raw / 1e6).toFixed(2)} Mbps`;
    if (raw >= 1e3) return `${(raw / 1e3).toFixed(2)} Kbps`;
    return `${raw.toFixed(0)} bps`;
  }
  if (raw >= 1e9) return `${(raw / 1e9).toFixed(2)}e9`;
  if (raw >= 1e6) return `${(raw / 1e6).toFixed(2)}e6`;
  if (raw >= 1e3) return `${(raw / 1e3).toFixed(2)}e3`;
  return raw.toFixed(2);
}

function priorityScore(entityKind: EntityKind, spec: MetricSpec): number {
  let score = 0;

  if (entityKind === "host") {
    if (["L3", "L4", "L5", "L6", "L7"].includes(spec.layer)) score += 20;
    if (spec.key.includes("tcp")) score += 18;
    if (spec.key.includes("cpu") || spec.key.includes("ram")) score += 14;
    if (spec.key.includes("app_latency")) score += 16;
  }

  if (entityKind === "switch") {
    if (spec.layer === "Dataplane") score += 26;
    if (["L0", "L1", "L2"].includes(spec.layer)) score += 16;
    if (spec.key.includes("table_") || spec.key.includes("flow_")) score += 18;
    if (spec.key.includes("drops") || spec.key.includes("errors")) score += 10;
  }

  if (entityKind === "controller") {
    if (spec.layer === "Control Plane") score += 26;
    if (spec.key.includes("flows_installed") || spec.key.includes("controller_latency")) score += 18;
    if (spec.key.includes("reconnect")) score += 14;
  }

  if (spec.category === "Saúde") score += 3;
  if (spec.category === "Erros") score += 2;

  return score;
}

function DeepDiveMetricCard({
  spec,
  series,
  value,
  entityKind,
  entityId,
}: {
  spec: MetricSpec;
  series: number[];
  value: string;
  entityKind: EntityKind;
  entityId: string;
}) {
  const last = series[series.length - 1] ?? 0;
  const isUp = spec.kind === "bool" ? last > 0.5 : null;
  const tone: "neutral" | "ok" | "danger" =
    spec.kind === "bool" ? (isUp ? "ok" : "danger") : "neutral";

  return (
    <Card className="overflow-hidden">
      <div className="flex items-start justify-between gap-2 border-b border-border-0/50 px-2 py-1.5">
        <div className="min-w-0">
          <div className="truncate font-mono text-[11px] text-fg-0">{spec.name}</div>
          <div className="mt-0.5 flex flex-wrap items-center gap-1.5 text-[10px] text-fg-1">
            <span className="font-mono">{layerLabel(spec.layer)}</span>
            <span className="font-mono">{spec.type}</span>
          </div>
        </div>
        <Badge tone={categoryBadgeTone(spec.category)}>{spec.category === "Erros" ? "Erro" : spec.category}</Badge>
      </div>

      <div className="px-2 py-2">
        <div className="flex items-center justify-center gap-2">
          {spec.kind === "bool" ? (
            <span
              className={cn(
                "h-2.5 w-2.5 rounded-full ring-1 ring-border-0/60",
                tone === "ok" && "bg-accent-ok",
                tone === "danger" && "bg-accent-danger",
              )}
              aria-label={tone === "ok" ? "Up" : "Down"}
              title={tone === "ok" ? "Up" : "Down"}
            />
          ) : null}

          <div className="text-center">
            <div className="font-mono text-sm text-fg-0">
              {spec.kind === "bool" ? (isUp ? "Up" : "Down") : value}
            </div>
            <div className="mt-0.5 text-[10px] text-fg-1">{spec.unit ?? "—"}</div>
          </div>
        </div>

        <div className={cn("mt-2 flex justify-center text-fg-1", tone === "ok" && "text-accent-ok", tone === "danger" && "text-accent-danger")}>
          <AreaMini values={series} className="h-[34px] w-[140px]" />
        </div>
      </div>

      <div className="border-t border-border-0/50 px-2 py-1.5 text-[10px] text-fg-1">
        {inferDimensionLabel({ dimension: spec.dimension, entityKind, entityId })}
      </div>
    </Card>
  );
}

export default function DashboardPage() {
  const [entityKind, setEntityKind] = useState<EntityKind>("switch");
  const [entityId, setEntityId] = useState<string>("s1");
  const [layer, setLayer] = useState<MetricLayer | "all">(() => defaultLayerForEntity("switch"));

  const [category, setCategory] = useState<"all" | MetricCategory>("all");
  const [type, setType] = useState<"all" | MetricType>("all");
  const [q, setQ] = useState<string>("");

  const [topologies, setTopologies] = useState<Topology[]>([]);
  const [topologyId, setTopologyId] = useState<string>("all");

  const [timeFilter, setTimeFilter] = useState<null | { runId: string; from: string; to: string; samplingMs?: number }>(null);
  
  const [useRealData, setUseRealData] = useState<boolean | null>(null);

  useEffect(() => {
    // Buscar configuração do sistema para saber se está em modo real
    fetch(`${API_BASE}/system/settings`)
      .then(r => r.json())
      .then(data => setUseRealData(data?.use_real_data ?? false))
      .catch(() => setUseRealData(null));
    
    const interval = setInterval(() => {
      fetch(`${API_BASE}/system/settings`)
        .then(r => r.json())
        .then(data => setUseRealData(data?.use_real_data ?? false))
        .catch(() => {});
    }, 10000); // Atualiza a cada 10s
    
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    try {
      const raw = localStorage.getItem("netops.dashboard.timeFilter");
      if (!raw) return;
      const parsed = JSON.parse(raw) as any;
      if (!parsed || typeof parsed !== "object") return;
      if (typeof parsed.runId !== "string" || typeof parsed.from !== "string" || typeof parsed.to !== "string") return;
      setTimeFilter({ runId: parsed.runId, from: parsed.from, to: parsed.to, samplingMs: typeof parsed.samplingMs === "number" ? parsed.samplingMs : undefined });
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    topologyApi.list().then(setTopologies).catch(() => {});
  }, []);

  const seriesParams = useMemo(() => {
    const base = { points: 36, intervalMs: 950 };
    if (!timeFilter) return base;
    const fromMs = Date.parse(timeFilter.from);
    const toMs = Date.parse(timeFilter.to);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) return base;

    const durationMs = toMs - fromMs;
    const samplingMs = Math.max(80, Math.min(5000, Math.round(timeFilter.samplingMs ?? 950)));

    const points = Math.max(12, Math.min(72, Math.round(durationMs / samplingMs)));
    const intervalMs = Math.max(200, Math.min(1500, samplingMs));
    return { points, intervalMs };
  }, [timeFilter]);

  // ── Live backend metrics ──────────────────────────────────────────────────
  // Pushed by the backend's SSE stream (/stream/events) as soon as a new
  // collection cycle lands, instead of a fixed 5s REST poll — the backend
  // itself no longer batches on a timer, so polling here just added latency
  // and redundant requests on top of a connection we already hold open.
  const { snapshot, connected: sseConnected } = useSSE();
  const [liveMetrics, setLiveMetrics] = useState<MetricSample[]>([]);
  const [backendOnline, setBackendOnline] = useState<boolean | null>(null); // null = unknown

  useEffect(() => {
    if (snapshot) {
      setLiveMetrics(snapshot.metrics);
      setBackendOnline(true);
    } else if (!sseConnected) {
      setBackendOnline((prev) => (prev === null ? null : false));
    }
  }, [snapshot, sseConnected]);

  // Derivar instâncias reais dos metrics do backend
  const entityInstances = useMemo<Record<EntityKind, string[]>>(() => {
    const map: Record<EntityKind, Set<string>> = { switch: new Set(), host: new Set(), controller: new Set() };
    for (const s of liveMetrics) {
      const kind = classifyNode(s.node);
      if (kind) map[kind].add(s.node);
    }
    return {
      switch: Array.from(map.switch).sort(),
      host: Array.from(map.host).sort(),
      controller: Array.from(map.controller).sort(),
    };
  }, [liveMetrics]);

  useEffect(() => {
    const instances = entityInstances[entityKind];
    if (instances.length > 0 && !instances.includes(entityId)) {
      setEntityId(instances[0]);
    }
    setLayer(defaultLayerForEntity(entityKind));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entityKind, entityInstances]);

  // Build a lookup: metric name → latest value (most recent sample wins)
  const liveValueMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const s of liveMetrics) {
      // Accept both exact key match and case-insensitive metric name match
      map.set(s.metric.toLowerCase(), s.value);
    }
    return map;
  }, [liveMetrics]);

  const entityOptions = entityInstances[entityKind].length > 0
    ? entityInstances[entityKind]
    : [entityId];

  const visibleSpecs = useMemo(() => {
    const query = q.trim().toLowerCase();

    const base = layer === "all"
      ? LAYERS.flatMap((l) => getLayerMetrics(l))
      : getLayerMetrics(layer);
    return base
      .filter((m) => {
        if (!m.entityKinds.includes(entityKind)) return false;
        if (!isAll(category) && m.category !== category) return false;
        if (!isAll(type) && m.type !== type) return false;
        if (query && !m.name.toLowerCase().includes(query) && !m.key.toLowerCase().includes(query)) return false;
        return true;
      })
      .sort((a, b) => {
        const pa = priorityScore(entityKind, a);
        const pb = priorityScore(entityKind, b);
        if (pa !== pb) return pb - pa;
        return a.name.localeCompare(b.name);
      });
  }, [category, entityKind, layer, q, type]);

  const seriesMap = useMetricSeriesMap({ specs: visibleSpecs, points: seriesParams.points, intervalMs: seriesParams.intervalMs });

  const currentValues = useMemo(() => {
    const out: Record<string, string> = {};
    for (const spec of visibleSpecs) {
      // Prefer live backend value; fall back to last simulated value
      const liveRaw =
        liveValueMap.get(spec.key.toLowerCase()) ??
        liveValueMap.get(spec.name.toLowerCase());
      const series = seriesMap[spec.key] ?? [];
      const simLast = series[series.length - 1] ?? spec.sim.initial;
      const raw = liveRaw !== undefined ? liveRaw : simLast;
      out[spec.key] = formatMetricValue(spec, raw);
    }
    return out;
  }, [seriesMap, visibleSpecs, liveValueMap]);


  return (
    <div className="space-y-3">
      <div className="sticky top-12 z-10 rounded-xl border border-border-0/60 bg-bg-0/80 p-2 backdrop-blur">
        {timeFilter ? (
          <div className="mb-2 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border-0/60 bg-bg-1/40 px-2 py-2">
            <div className="min-w-0 text-[11px] text-fg-1">
              Filtro de tempo (Run <span className="font-mono text-fg-0">{timeFilter.runId}</span>):
              <span className="ml-2 font-mono text-fg-0">{new Date(timeFilter.from).toLocaleString()}</span>
              <span className="mx-1">→</span>
              <span className="font-mono text-fg-0">{new Date(timeFilter.to).toLocaleString()}</span>
              <span className="ml-2 font-mono text-fg-1">points={seriesParams.points}</span>
              <span className="ml-1 font-mono text-fg-1">interval={seriesParams.intervalMs}ms</span>
            </div>
            <Button
              variant="ghost"
              className="h-7 px-2 text-[11px]"
              onClick={() => {
                localStorage.removeItem("netops.dashboard.timeFilter");
                setTimeFilter(null);
              }}
            >
              Limpar
            </Button>
          </div>
        ) : null}

        <div className="grid grid-cols-1 gap-2 lg:grid-cols-[1.1fr_1.1fr_1fr_1fr_1.5fr_1fr]">
          <div>
            <div className="mb-1 text-xs font-medium text-fg-1">Entidade</div>
            <Select value={entityKind} onChange={(e) => setEntityKind(e.target.value as EntityKind)}>
              <option value="switch">Switch</option>
              <option value="host">Host</option>
              <option value="controller">Controlador</option>
            </Select>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium text-fg-1">Instância</div>
            <Select value={entityId} onChange={(e) => setEntityId(e.target.value)}>
              {entityOptions.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </Select>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium text-fg-1">Categoria</div>
            <Select value={category} onChange={(e) => setCategory(e.target.value as any)}>
              <option value="all">Todas</option>
              {CATEGORIES.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </Select>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium text-fg-1">Tipo</div>
            <Select value={type} onChange={(e) => setType(e.target.value as any)}>
              <option value="all">Todos</option>
              {TYPES.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </Select>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium text-fg-1">Buscar métrica</div>
            <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="ex: tcp_rtt, link_util, flows" />
          </div>

          <div>
            <div className="mb-1 text-xs font-medium text-fg-1">Topologia</div>
            <Select value={topologyId} onChange={(e) => setTopologyId(e.target.value)}>
              <option value="all">Todas</option>
              {topologies.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name ?? t.id}
                </option>
              ))}
            </Select>
          </div>
        </div>

        <div className="mt-2 flex flex-wrap items-center justify-between gap-2">
          <div className="flex min-w-0 items-center gap-2">
            <div className="text-xs font-medium text-fg-1">Camadas</div>
            <div className="max-w-full overflow-x-auto">
              <div className="flex items-center gap-1">
                <Button
                  key="all"
                  variant={layer === "all" ? "primary" : "ghost"}
                  className={cn("h-7 px-2 text-[11px]", layer === "all" && "bg-bg-2")}
                  onClick={() => setLayer("all")}
                >
                  Todas
                </Button>
                {LAYERS.map((l) => {
                  const active = l === layer;
                  return (
                    <Button
                      key={l}
                      variant={active ? "primary" : "ghost"}
                      className={cn("h-7 px-2 text-[11px]", active && "bg-bg-2")}
                      onClick={() => setLayer(l)}
                    >
                      {layerLabel(l)}
                    </Button>
                  );
                })}
              </div>
            </div>
          </div>

          <div className="text-[11px] text-fg-1">
            contexto: <span className="font-mono">{prettyEntityKind(entityKind)}</span> · <span className="font-mono">{entityId}</span> ·{" "}
            <span className="font-mono">{layerLabel(layer)}</span> ·{" "}
            {topologyId !== "all" && (
              <span className="font-mono text-accent-ok">{topologies.find((t) => t.id === topologyId)?.name ?? topologyId} · </span>
            )}
            <span className="font-mono">{visibleSpecs.length}</span> métricas
            {backendOnline === true && (
              <>
                <span className="ml-2 inline-flex items-center gap-1 font-mono text-accent-ok">
                  <span className="h-1.5 w-1.5 rounded-full bg-accent-ok" />
                  online ({liveMetrics.length})
                </span>
                {useRealData !== null && (
                  <span className={`ml-2 inline-flex items-center gap-1 rounded px-1.5 py-0.5 font-mono text-[10px] font-semibold ${
                    useRealData 
                      ? "bg-red-500/20 text-red-400" 
                      : "bg-green-500/20 text-green-400"
                  }`}>
                    {useRealData ? "🔴 MODO REAL" : "🟢 MODO SINTÉTICO"}
                  </span>
                )}
              </>
            )}
            {backendOnline === false && (
              <span className="ml-2 inline-flex items-center gap-1 font-mono text-fg-1 opacity-60">
                <span className="h-1.5 w-1.5 rounded-full bg-fg-1" />
                offline
              </span>
            )}
          </div>
        </div>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-xs font-medium text-fg-1">
          Camada <span className="font-mono">{layerLabel(layer)}</span>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            className="h-7 px-2 text-[11px]"
            disabled={visibleSpecs.length === 0}
            onClick={() =>
              downloadTextFile({
                filename: `layer-report_${entityKind}_${entityId}_${String(layer).replaceAll(" ", "-")}.csv`,
                content: toLayerCsv({ entityKind, entityId, layer, specs: visibleSpecs, currentValues }),
                mime: "text/csv;charset=utf-8",
              })
            }
          >
            Exportar Relatório (CSV)
          </Button>
          <Button
            variant="ghost"
            className="h-7 px-2 text-[11px]"
            disabled={visibleSpecs.length === 0}
            onClick={() => {
              const payload = {
                generatedAt: new Date().toISOString(),
                context: {
                  entityKind,
                  entityId,
                  layer,
                  category,
                  type,
                  query: q,
                },
                metrics: visibleSpecs.map((spec) => ({
                  key: spec.key,
                  name: spec.name,
                  layer: spec.layer,
                  category: spec.category,
                  type: spec.type,
                  unit: spec.unit ?? "—",
                  dimension: inferDimensionLabel({ dimension: spec.dimension, entityKind, entityId }),
                  value: currentValues[spec.key] ?? "—",
                  series: seriesMap[spec.key] ?? [],
                })),
              };
              downloadTextFile({
                filename: `layer-report_${entityKind}_${entityId}_${String(layer).replaceAll(" ", "-")}.json`,
                content: JSON.stringify(payload, null, 2),
                mime: "application/json;charset=utf-8",
              });
            }}
          >
            Exportar Relatório (JSON)
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-6">
        {visibleSpecs.map((spec) => (
          <DeepDiveMetricCard
            key={`${entityKind}-${entityId}-${layer}-${spec.key}`}
            spec={spec}
            series={seriesMap[spec.key] ?? []}
            value={currentValues[spec.key] ?? "—"}
            entityKind={entityKind}
            entityId={entityId}
          />
        ))}
      </div>

      {visibleSpecs.length === 0 ? (
        <div className="rounded-xl border border-border-0/60 bg-bg-1/40 p-3 text-xs text-fg-1">
          Empty state: <span className="font-mono">{prettyEntityKind(entityKind)}</span> / <span className="font-mono">{entityId}</span> não possui métricas em <span className="font-mono">{layerLabel(layer)}</span> com os filtros atuais.
        </div>
      ) : null}

      <MetricInventoryTable entityKind={entityKind} entityId={entityId} specs={visibleSpecs} currentValues={currentValues} />
    </div>
  );
}
