"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  ReactFlowProvider,
  type Edge,
  type Node,
  type ReactFlowInstance,
} from "reactflow";
import "reactflow/dist/style.css";

import { nodeTypes } from "@/features/topology/nodeTypes";
import { edgeTypes, type LinkStatus } from "@/features/topology/edgeTypes";
import { cn } from "@/lib/cn";
import { topologyApi, metricsApi, type MetricSample, API_BASE } from "@/lib/api";
import { Button } from "@/components/ui/Button";
import { Activity, RefreshCw, X } from "lucide-react";
import { useSSE } from "@/hooks/useSSE";

// ─── Types ─────────────────────────────────────────────────────────────────

type NodeKind = "switch" | "host" | "controller";

type TopologyNodeData = {
  id: string;
  mgmtIp?: string;
  ip?: string;
  dpid?: string;
  status?: "online" | "offline";
  cpuPct?: number;
  memPct?: number;
  responseLatencyMs?: number;
  pktStats?: { rxPps: number; txPps: number; dropPps: number };
};

type SelectedElement =
  | { kind: "node"; nodeId: string }
  | { kind: "edge"; edgeId: string }
  | null;

// ─── Auto-layout ──────────────────────────────────────────────────────────

function layoutNodes(
  rawNodes: { id: string; type?: string; mgmt_ip?: string | null }[],
): Node<TopologyNodeData>[] {
  const controllers = rawNodes.filter((n) => n.type === "controller");
  const switches = rawNodes.filter((n) => n.type === "switch");
  const hosts = rawNodes.filter((n) => n.type === "host");

  const cx = 560;
  const xFor = (arr: typeof rawNodes, idx: number) =>
    cx - ((arr.length - 1) * 220) / 2 + idx * 220;

  const result: Node<TopologyNodeData>[] = [];

  controllers.forEach((n, i) =>
    result.push({
      id: n.id,
      type: "controller" as NodeKind,
      position: { x: xFor(controllers, i), y: 60 },
      data: { id: n.id, mgmtIp: n.mgmt_ip ?? undefined },
    }),
  );
  switches.forEach((n, i) =>
    result.push({
      id: n.id,
      type: "switch" as NodeKind,
      position: { x: xFor(switches, i), y: 260 },
      data: { id: n.id, mgmtIp: n.mgmt_ip ?? undefined },
    }),
  );
  hosts.forEach((n, i) =>
    result.push({
      id: n.id,
      type: "host" as NodeKind,
      position: { x: xFor(hosts, i), y: 460 },
      data: { id: n.id, mgmtIp: n.mgmt_ip ?? undefined },
    }),
  );

  return result;
}

// ─── Deep-Dive helpers ────────────────────────────────────────────────────

function fmt(v: number | undefined, dec = 1, sfx = "") {
  return v !== undefined ? `${v.toFixed(dec)}${sfx}` : "—";
}
function fmtInt(v: number | undefined, sfx = "") {
  return v !== undefined ? `${v.toLocaleString()}${sfx}` : "—";
}

function MetricRow({ label, value, danger }: { label: string; value: string; danger?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-2 py-0.5">
      <span className="text-[10px] text-fg-1">{label}</span>
      <span className={cn("font-mono text-[11px]", danger ? "text-accent-danger" : "text-fg-0")}>{value}</span>
    </div>
  );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <div className="mb-1 text-[9px] font-bold uppercase tracking-widest text-fg-1">{children}</div>
  );
}

function MiniProgressBar({ pct, danger }: { pct: number; danger?: boolean }) {
  return (
    <div className="mt-0.5 h-1 w-full overflow-hidden rounded-full bg-border-0/40">
      <div
        className={cn("h-full rounded-full transition-all", danger ? "bg-accent-danger" : "bg-accent-ok")}
        style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
      />
    </div>
  );
}

// ─── Main component ──────────────────────────────────────────────────────

function TopologyViewInner() {
  const [nodes, setNodes] = useState<Node<TopologyNodeData>[]>([]);
  const [edges, setEdges] = useState<Edge<LinkStatus>[]>([]);
  const [topoName, setTopoName] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  // Track backend data-mode to force a refresh when it changes
  const [dataModeReal, setDataModeReal] = useState<boolean | null>(null);
  const dataModeRef = useRef<boolean | null>(null);

  const [selected, setSelected] = useState<SelectedElement>(null);
  const [nodeMetrics, setNodeMetrics] = useState<Map<string, Map<string, number>>>(new Map());
  const nodeMetricsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const rfRef = useRef<ReactFlowInstance | null>(null);

  // ── Poll system settings to detect mode changes ────────────────────────
  useEffect(() => {
    const checkMode = () => {
      fetch(`${API_BASE}/system/settings`, { cache: "no-store" })
        .then((r) => r.json())
        .then((d: { use_real_data: boolean }) => {
          const newMode = d.use_real_data;
          if (dataModeRef.current !== null && dataModeRef.current !== newMode) {
            // Mode changed — clear node metrics display and reload topology
            setNodeMetrics(new Map());
            setDataModeReal(newMode);
          }
          dataModeRef.current = newMode;
        })
        .catch(() => {});
    };
    checkMode();
    const t = setInterval(checkMode, 10_000); // mode changes are rare; 10s is sufficient
    return () => clearInterval(t);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Center view whenever nodes are first loaded ────────────────────────
  const didFitRef = useRef(false);
  useEffect(() => {
    if (nodes.length > 0 && !didFitRef.current) {
      didFitRef.current = true;
      // Fire multiple times: first pass snaps position, second pass after
      // the browser has painted and ReactFlow has measured the container.
      setTimeout(() => rfRef.current?.fitView({ padding: 0.25, duration: 300 }), 50);
      setTimeout(() => rfRef.current?.fitView({ padding: 0.25, duration: 400 }), 350);
    }
  }, [nodes]);

  // ── Load topology once ─────────────────────────────────────────────────
  const loadTopology = useCallback(() => {
    setLoading(true);
    didFitRef.current = false;
    topologyApi
      .list()
      .then((topos) => {
        if (!topos.length) { setLoading(false); return; }
        const topo = topos[0];
        setTopoName(topo.name ?? topo.id);
        if (topo.nodes?.length) setNodes(layoutNodes(topo.nodes));
        if (topo.links?.length) {
          setEdges(
            topo.links.map((l, i) => ({
              id: `e-${l.source}-${l.target}-${i}`,
              source: l.source,
              target: l.target,
              type: "util",
              animated: false,
              data: {
                mode: "physical",
                utilPct: 0,
                capacityMbps: l.bandwidth_mbps ?? 1000,
                lossPct: l.loss_pct ?? 0,
                up: true,
              } satisfies LinkStatus,
            })),
          );
        }
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  // Load on mount, and whenever data mode changes
  useEffect(() => { loadTopology(); }, [loadTopology, dataModeReal]);

  // ── Live global metrics (updates canvas nodes + edges) ──────────────────
  // Driven by the SSE stream instead of a 5s REST poll: the backend already
  // pushes a fresh snapshot the moment a collection cycle completes, so a
  // separate timer here just duplicated that traffic and added up to 5s of
  // extra staleness on top of it.
  const { snapshot: liveSnapshot } = useSSE();
  useEffect(() => {
    const apply = (samples: MetricSample[]) => {
      setLastRefresh(new Date());
      const byNode = new Map<string, Map<string, number>>();
      for (const s of samples) {
        if (!byNode.has(s.node)) byNode.set(s.node, new Map());
        byNode.get(s.node)!.set(s.metric, s.value);
      }

      setNodes((prev) =>
        prev.map((n) => {
          const m = byNode.get(n.id);
          if (!m) return n;
          const patch: Partial<TopologyNodeData> = {};
          const cpu = m.get("docker_cpu_util_pct") ?? m.get("cpu_util_pct");
          const mem = m.get("docker_mem_util_pct") ?? m.get("mem_util_pct");
          const lat = m.get("controller_latency_ms");
          const connOk = m.get("controller_conn_ok");
          if (cpu !== undefined) patch.cpuPct = cpu;
          if (mem !== undefined) patch.memPct = mem;
          if (lat !== undefined) patch.responseLatencyMs = lat;
          if (connOk !== undefined) patch.status = connOk > 0.5 ? "online" : "offline";
          else if (n.type === "controller") patch.status = "online";
          return Object.keys(patch).length ? { ...n, data: { ...n.data, ...patch } } : n;
        }),
      );

      setEdges((prev) =>
        prev.map((e) => {
          const lkA = `${e.source}->${e.target}`;
          const lkB = `${e.target}->${e.source}`;
          const lm = byNode.get(lkA) ?? byNode.get(lkB);
          if (!lm) return e;
          const util = lm.get("link_util_pct") ?? lm.get("if_in_util_pct");
          const loss = lm.get("link_loss_pct");
          const up = lm.get("if_link_up");
          const d = e.data ?? ({ mode: "physical" } as LinkStatus);
          return {
            ...e,
            animated: ((loss ?? d.lossPct ?? 0) >= 2.5) || (up !== undefined ? up < 0.5 : false),
            data: {
              ...d,
              utilPct: util ?? d.utilPct ?? 0,
              lossPct: loss ?? d.lossPct ?? 0,
              up: up !== undefined ? up >= 0.5 : (d.up ?? true),
            },
          };
        }),
      );
    };

    if (liveSnapshot) {
      apply(liveSnapshot.metrics);
    } else {
      // Cold-start fallback before the first SSE frame arrives.
      metricsApi.latest().then(apply).catch(() => {});
    }
  }, [liveSnapshot]);

  // ── Poll per-node/edge metrics on selection ────────────────────────────
  useEffect(() => {
    if (nodeMetricsTimerRef.current) clearInterval(nodeMetricsTimerRef.current);
    if (!selected) return;

    let key: string;
    if (selected.kind === "node") {
      key = selected.nodeId;
    } else {
      const edge = edges.find((e) => e.id === selected.edgeId);
      if (!edge) return;
      key = `${edge.source}->${edge.target}`;
    }

    const fetch = () => {
      metricsApi.latest({ node: key }).then((samples) => {
        const m = new Map<string, number>();
        for (const s of samples) m.set(s.metric, s.value);
        setNodeMetrics((prev) => new Map(prev).set(key, m));
      }).catch(() => {});
    };
    fetch();
    nodeMetricsTimerRef.current = setInterval(fetch, 5000);
    return () => { if (nodeMetricsTimerRef.current) clearInterval(nodeMetricsTimerRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected]);

  // ── Handlers ──────────────────────────────────────────────────────────
  const onNodeClick = useCallback((_: unknown, node: Node<TopologyNodeData>) => {
    setSelected({ kind: "node", nodeId: node.id });
  }, []);

  const onEdgeClick = useCallback((_: unknown, edge: Edge<LinkStatus>) => {
    setSelected({ kind: "edge", edgeId: edge.id });
  }, []);

  const onPaneClick = useCallback(() => setSelected(null), []);

  const drawerOpen = selected !== null;

  // ── Derived: selected node / edge ─────────────────────────────────────
  const selectedNode = drawerOpen && selected?.kind === "node"
    ? nodes.find((n) => n.id === selected.nodeId) ?? null
    : null;

  const selectedEdge = drawerOpen && selected?.kind === "edge"
    ? edges.find((e) => e.id === selected.edgeId) ?? null
    : null;

  // ── Deep-dive content ─────────────────────────────────────────────────
  const renderDeepDive = () => {
    if (selectedEdge) {
      const lkA = `${selectedEdge.source}->${selectedEdge.target}`;
      const lkB = `${selectedEdge.target}->${selectedEdge.source}`;
      const lm = nodeMetrics.get(lkA) ?? nodeMetrics.get(lkB);
      const d = selectedEdge.data;
      return (
        <div className="space-y-3">
          {/* Header */}
          <div className="flex items-start justify-between gap-2">
            <div>
              <div className="font-mono text-[13px] font-bold text-fg-0">
                {selectedEdge.source} ↔ {selectedEdge.target}
              </div>
              <div className="text-[10px] text-fg-1">
                Capacidade: {d?.capacityMbps
                  ? d.capacityMbps >= 1000
                    ? `${(d.capacityMbps / 1000).toFixed(0)} Gbps`
                    : `${d.capacityMbps} Mbps`
                  : "—"}
              </div>
            </div>
            <span className={cn(
              "flex-shrink-0 rounded-full px-2 py-0.5 text-[10px] font-semibold",
              (d?.up ?? true) ? "bg-accent-ok/15 text-accent-ok" : "bg-accent-danger/15 text-accent-danger",
            )}>
              {(d?.up ?? true) ? "UP" : "DOWN"}
            </span>
          </div>

          {/* Metrics */}
          <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
            <SectionTitle>Métricas do link</SectionTitle>
            <MetricRow label="Utilização" value={`${(lm?.get("link_util_pct") ?? d?.utilPct ?? 0).toFixed(1)}%`} />
            <MetricRow
              label="Perda de pacotes"
              value={`${(lm?.get("link_loss_pct") ?? d?.lossPct ?? 0).toFixed(2)}%`}
              danger={(lm?.get("link_loss_pct") ?? d?.lossPct ?? 0) >= 2.5}
            />
            <MetricRow label="RX pacotes" value={lm?.get("port_rx_pkts") !== undefined ? lm.get("port_rx_pkts")!.toLocaleString() : "—"} />
            <MetricRow label="TX pacotes" value={lm?.get("port_tx_pkts") !== undefined ? lm.get("port_tx_pkts")!.toLocaleString() : "—"} />
            <MetricRow label="Jitter" value={lm?.get("link_jitter_ms") !== undefined ? `${lm.get("link_jitter_ms")!.toFixed(2)} ms` : "—"} />
            <MetricRow label="Erros IF" value={lm?.get("if_errors") !== undefined ? lm.get("if_errors")!.toFixed(0) : "—"} />
          </div>

          {/* All raw metrics */}
          {lm && lm.size > 0 && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>Todas as métricas ({lm.size})</SectionTitle>
              <div className="max-h-48 overflow-auto">
                <table className="w-full border-collapse text-left text-[10px]">
                  <tbody>
                    {Array.from(lm.entries()).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => (
                      <tr key={k} className="border-b border-border-0/20">
                        <td className="py-0.5 pr-2 font-mono text-fg-1">{k}</td>
                        <td className="py-0.5 font-mono text-fg-0">{v.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      );
    }

    if (selectedNode) {
      const nm = nodeMetrics.get(selectedNode.id) ?? new Map<string, number>();
      const cpu = nm.get("docker_cpu_util_pct") ?? nm.get("cpu_util_pct") ?? selectedNode.data.cpuPct;
      const mem = nm.get("docker_mem_util_pct") ?? nm.get("mem_util_pct") ?? selectedNode.data.memPct;
      const lat = nm.get("controller_latency_ms") ?? selectedNode.data.responseLatencyMs;
      const connOk = nm.get("controller_conn_ok");
      const online = connOk !== undefined ? connOk > 0.5 : selectedNode.data.status !== "offline";

      return (
        <div className="space-y-3">
          {/* Header */}
          <div className="flex items-start justify-between gap-2">
            <div>
              <div className="font-mono text-[14px] font-bold text-fg-0">{selectedNode.id}</div>
              <div className="text-[10px] uppercase tracking-wide text-fg-1">{selectedNode.type}</div>
              {selectedNode.data.mgmtIp && (
                <div className="font-mono text-[10px] text-fg-1">{selectedNode.data.mgmtIp}</div>
              )}
            </div>
            <span className={cn(
              "flex-shrink-0 rounded-full px-2 py-0.5 text-[10px] font-semibold",
              online ? "bg-accent-ok/15 text-accent-ok" : "bg-accent-danger/15 text-accent-danger",
            )}>
              {online ? "ONLINE" : "OFFLINE"}
            </span>
          </div>

          {/* Identity */}
          <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
            <SectionTitle>Identidade</SectionTitle>
            <MetricRow label="Tipo" value={selectedNode.type ?? "—"} />
            <MetricRow label="Mgmt IP" value={selectedNode.data.mgmtIp ?? "—"} />
            {selectedNode.type === "switch" && (
              <MetricRow label="DPID" value={selectedNode.data.dpid ?? "—"} />
            )}
            {selectedNode.type === "controller" && (
              <MetricRow label="Latência OF" value={fmt(lat, 1, " ms")} />
            )}
          </div>

          {/* Resources */}
          {(cpu !== undefined || mem !== undefined) && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>Recursos</SectionTitle>
              {cpu !== undefined && (
                <div className="mb-1.5">
                  <div className="flex justify-between text-[10px]">
                    <span className="text-fg-1">CPU</span>
                    <span className={cn("font-mono", cpu > 80 ? "text-accent-danger" : "text-fg-0")}>{cpu.toFixed(1)}%</span>
                  </div>
                  <MiniProgressBar pct={cpu} danger={cpu > 80} />
                </div>
              )}
              {mem !== undefined && (
                <div>
                  <div className="flex justify-between text-[10px]">
                    <span className="text-fg-1">MEM</span>
                    <span className={cn("font-mono", mem > 85 ? "text-accent-danger" : "text-fg-0")}>{mem.toFixed(1)}%</span>
                  </div>
                  <MiniProgressBar pct={mem} danger={mem > 85} />
                </div>
              )}
              {nm.get("docker_net_rx_bytes") !== undefined && (
                <div className="mt-1.5">
                  <MetricRow label="Net RX" value={fmtInt(nm.get("docker_net_rx_bytes"), " B")} />
                  <MetricRow label="Net TX" value={fmtInt(nm.get("docker_net_tx_bytes"), " B")} />
                </div>
              )}
            </div>
          )}

          {/* Network */}
          {(nm.get("latency_ms") !== undefined || nm.get("packet_loss_pct") !== undefined || nm.get("jitter_ms") !== undefined) && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>Rede</SectionTitle>
              {nm.get("latency_ms") !== undefined && <MetricRow label="Latência" value={fmt(nm.get("latency_ms"), 2, " ms")} />}
              {nm.get("packet_loss_pct") !== undefined && (
                <MetricRow label="Perda" value={fmt(nm.get("packet_loss_pct"), 2, "%")} danger={(nm.get("packet_loss_pct") ?? 0) > 1} />
              )}
              {nm.get("jitter_ms") !== undefined && <MetricRow label="Jitter" value={fmt(nm.get("jitter_ms"), 2, " ms")} />}
              {nm.get("throughput_mbps") !== undefined && <MetricRow label="Throughput" value={fmt(nm.get("throughput_mbps"), 1, " Mbps")} />}
            </div>
          )}

          {/* Controller-specific */}
          {selectedNode.type === "controller" && (nm.get("flows_installed") !== undefined || nm.get("alerts_count") !== undefined || nm.get("of_channel_reconnects") !== undefined) && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>OpenFlow / Controle</SectionTitle>
              {nm.get("flows_installed") !== undefined && <MetricRow label="Flows instalados" value={fmtInt(nm.get("flows_installed"))} />}
              {nm.get("flows_removed") !== undefined && <MetricRow label="Flows removidos" value={fmtInt(nm.get("flows_removed"))} />}
              {nm.get("of_channel_reconnects") !== undefined && <MetricRow label="Reconexões OF" value={fmtInt(nm.get("of_channel_reconnects"))} />}
              {nm.get("alerts_count") !== undefined && <MetricRow label="Alertas" value={fmtInt(nm.get("alerts_count"))} />}
              {nm.get("queue_depth") !== undefined && <MetricRow label="Fila" value={fmtInt(nm.get("queue_depth"))} />}
              <div className="mt-1.5">
                <div className="text-[9px] font-bold uppercase tracking-widest text-fg-1">Switches sob gestão</div>
                <div className="mt-1 flex flex-wrap gap-1">
                  {nodes.filter((n) => n.type === "switch").map((s) => (
                    <span key={s.id} className="rounded border border-border-0/50 bg-bg-2/30 px-1.5 py-0.5 font-mono text-[10px] text-fg-0">{s.id}</span>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Switch-specific */}
          {selectedNode.type === "switch" && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>Switch</SectionTitle>
              <div className="grid grid-cols-3 gap-1">
                {(["RX pps", "TX pps", "DROP"] as const).map((label, i) => {
                  const vals = [selectedNode.data.pktStats?.rxPps, selectedNode.data.pktStats?.txPps, selectedNode.data.pktStats?.dropPps];
                  const v = vals[i] ?? 0;
                  return (
                    <div key={label} className="rounded border border-border-0/40 bg-bg-2/20 p-1.5 text-center">
                      <div className="text-[9px] text-fg-1">{label}</div>
                      <div className={cn("font-mono text-[12px]", label === "DROP" && v > 0 ? "text-accent-danger" : "text-fg-0")}>{v}</div>
                    </div>
                  );
                })}
              </div>
              {nm.get("queue_occupancy_pct") !== undefined && (
                <div className="mt-1.5">
                  <MetricRow label="Fila" value={fmt(nm.get("queue_occupancy_pct"), 1, "%")} />
                  <MetricRow label="Queue drops" value={fmtInt(nm.get("queue_drops"))} />
                </div>
              )}
              {nm.get("snmp_tcp_currestab") !== undefined && <MetricRow label="TCP Estab" value={fmtInt(nm.get("snmp_tcp_currestab"))} />}
              {nm.get("snmp_ip_inreceives") !== undefined && <MetricRow label="IP RX total" value={fmtInt(nm.get("snmp_ip_inreceives"))} />}
            </div>
          )}

          {/* Host-specific */}
          {selectedNode.type === "host" && (nm.get("dns_latency_ms") !== undefined || nm.get("http_latency_ms") !== undefined || nm.get("tcp_rtt_ms") !== undefined || nm.get("app_rps") !== undefined) && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>Host</SectionTitle>
              {nm.get("dns_latency_ms") !== undefined && <MetricRow label="DNS lat" value={fmt(nm.get("dns_latency_ms"), 1, " ms")} />}
              {nm.get("dns_success_pct") !== undefined && <MetricRow label="DNS ok" value={fmt(nm.get("dns_success_pct"), 1, "%")} />}
              {nm.get("http_latency_ms") !== undefined && <MetricRow label="HTTP lat" value={fmt(nm.get("http_latency_ms"), 1, " ms")} />}
              {nm.get("http_success_pct") !== undefined && <MetricRow label="HTTP ok" value={fmt(nm.get("http_success_pct"), 1, "%")} />}
              {nm.get("tcp_rtt_ms") !== undefined && <MetricRow label="TCP RTT" value={fmt(nm.get("tcp_rtt_ms"), 2, " ms")} />}
              {nm.get("ss_tcp_sockets_total") !== undefined && <MetricRow label="Sockets TCP" value={fmtInt(nm.get("ss_tcp_sockets_total"))} />}
              {nm.get("app_rps") !== undefined && <MetricRow label="App RPS" value={fmt(nm.get("app_rps"), 1)} />}
            </div>
          )}

          {/* All metrics table */}
          {nm.size > 0 && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-2">
              <SectionTitle>Todas as métricas ({nm.size})</SectionTitle>
              <div className="max-h-60 overflow-auto">
                <table className="w-full border-collapse text-left text-[10px]">
                  <tbody>
                    {Array.from(nm.entries()).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => (
                      <tr key={k} className="border-b border-border-0/20 hover:bg-bg-2/20">
                        <td className="py-0.5 pr-2 font-mono text-fg-1">{k}</td>
                        <td className="py-0.5 font-mono text-fg-0">{v.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {nm.size === 0 && (
            <div className="rounded-lg border border-border-0/50 bg-bg-1/40 p-3 text-center">
              <div className="animate-pulse text-[11px] text-fg-1">
                Carregando métricas de <span className="font-mono text-fg-0">{selectedNode.id}</span>…
              </div>
            </div>
          )}
        </div>
      );
    }

    return null;
  };

  // ── Loading screen ──────────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="flex h-[80vh] items-center justify-center">
        <div className="flex flex-col items-center gap-4 text-fg-1">
          <div className="h-10 w-10 animate-spin rounded-full border-2 border-fg-1/20 border-t-accent-ok" />
          <div className="text-sm">Carregando topologia…</div>
        </div>
      </div>
    );
  }

  if (!nodes.length) {
    return (
      <div className="flex h-[80vh] items-center justify-center">
        <div className="flex flex-col items-center gap-3 text-fg-1">
          <Activity className="h-10 w-10 opacity-30" />
          <div className="text-sm">Nenhuma topologia encontrada no backend.</div>
          <Button variant="ghost" onClick={loadTopology}><RefreshCw className="mr-2 h-4 w-4" /> Tentar novamente</Button>
        </div>
      </div>
    );
  }

  // ── Main render ─────────────────────────────────────────────────────────
  return (
    <div className="flex h-full flex-col gap-0 overflow-hidden">
      {/* Top bar */}
      <div className="flex flex-shrink-0 items-center justify-between border-b border-border-0/50 bg-bg-0/80 px-4 py-2 backdrop-blur">
        <div className="flex items-center gap-3">
          <div className="text-[14px] font-semibold tracking-tight text-fg-0">Topologia</div>
          {topoName && (
            <span className="inline-flex items-center gap-1.5 rounded-full bg-accent-ok/10 px-2.5 py-0.5 text-[11px] font-medium text-accent-ok">
              <span className="h-1.5 w-1.5 rounded-full bg-accent-ok" />
              {topoName}
            </span>
          )}
          <span className="text-[11px] text-fg-1">
            {nodes.filter((n) => n.type === "controller").length} controlador
            {" · "}
            {nodes.filter((n) => n.type === "switch").length} switch
            {" · "}
            {nodes.filter((n) => n.type === "host").length} host
            {" · "}
            {edges.length} link{edges.length !== 1 ? "s" : ""}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {lastRefresh && (
            <span className="flex items-center gap-1 text-[10px] text-fg-1">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-accent-ok" />
              Atualizado {lastRefresh.toLocaleTimeString("pt-BR")}
            </span>
          )}
          <Button variant="ghost" className="h-7 px-2 text-[11px]" onClick={loadTopology}>
            <RefreshCw className="mr-1.5 h-3 w-3" /> Recarregar
          </Button>
        </div>
      </div>

      {/* Canvas + drawer */}
      <div className={cn("flex h-full min-h-0 flex-1 overflow-hidden", drawerOpen ? "gap-0" : "")}>
        {/* ReactFlow canvas */}
        <div className={cn("relative min-w-0 flex-1 transition-all duration-200")} style={{ height: "100%" }}>
          <ReactFlow
            key={topoName ?? "topo"}
            nodes={nodes}
            edges={edges}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onPaneClick={onPaneClick}
            onInit={(instance) => {
              rfRef.current = instance;
              // Delay fitView to let the container be measured by the browser
              setTimeout(() => instance.fitView({ padding: 0.25, duration: 400 }), 50);
              setTimeout(() => instance.fitView({ padding: 0.25, duration: 400 }), 300);
            }}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            nodesConnectable={false}
            nodesDraggable={true}
            elementsSelectable={true}
            deleteKeyCode={null}
          >
            <Background gap={20} size={1} color="rgba(40,49,73,0.7)" />
            <Controls showInteractive={false} />
          </ReactFlow>

          {/* Selection hint */}
          {!drawerOpen && (
            <div className="pointer-events-none absolute bottom-16 left-1/2 -translate-x-1/2 rounded-full border border-border-0/40 bg-bg-0/80 px-3 py-1.5 text-[11px] text-fg-1 backdrop-blur">
              Clique em um nó ou link para ver as informações
            </div>
          )}
        </div>

        {/* Deep-dive drawer */}
        {drawerOpen && (
          <aside className="flex h-full w-[340px] flex-shrink-0 flex-col border-l border-border-0/50 bg-bg-0/90 backdrop-blur">
            {/* Drawer header */}
            <div className="flex flex-shrink-0 items-center justify-between border-b border-border-0/50 px-3 py-2.5">
              <div className="text-[12px] font-semibold text-fg-0">
                {selectedNode ? selectedNode.id : selectedEdge ? `${selectedEdge.source} ↔ ${selectedEdge.target}` : ""}
              </div>
              <button
                className="rounded p-1 text-fg-1 hover:bg-bg-2/40 hover:text-fg-0"
                onClick={() => setSelected(null)}
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
            {/* Drawer body — scrollable */}
            <div className="flex-1 overflow-y-scroll px-3 py-3">
              {renderDeepDive()}
            </div>
          </aside>
        )}
      </div>
    </div>
  );
}

export function TopologyView() {
  return (
    <ReactFlowProvider>
      <TopologyViewInner />
    </ReactFlowProvider>
  );
}
