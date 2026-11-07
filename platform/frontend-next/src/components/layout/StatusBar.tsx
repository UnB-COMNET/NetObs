"use client";

import { useEffect, useMemo, useState } from "react";
import { Badge } from "@/components/ui/Badge";
import { type MetricSample } from "@/lib/api";
import { useSSE } from "@/hooks/useSSE";

function formatGiB(bytes: number) {
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GiB`;
}

function formatPct(v: number) {
  return `${v.toFixed(1)}%`;
}

type ResourceData = {
  cpuPct: number | null;
  memPct: number | null;
  memUsedBytes: number | null;
  source: "real" | "unavailable";
  node: string;
  updatedAt: string | null;
};

const FALLBACK: ResourceData = {
  cpuPct: null,
  memPct: null,
  memUsedBytes: null,
  source: "unavailable",
  node: "—",
  updatedAt: null,
};

function extractResources(samples: MetricSample[]): ResourceData {
  // Prioridade: docker_cpu_util_pct > cpu_util_pct
  const cpuSample =
    samples.find((s) => s.metric === "docker_cpu_util_pct") ??
    samples.find((s) => s.metric === "cpu_util_pct");
  const memPctSample =
    samples.find((s) => s.metric === "docker_mem_util_pct") ??
    samples.find((s) => s.metric === "mem_util_pct");
  const memBytesSample = samples.find((s) => s.metric === "docker_mem_used_bytes");

  if (!cpuSample && !memPctSample) return FALLBACK;

  return {
    cpuPct: cpuSample?.value ?? null,
    memPct: memPctSample?.value ?? null,
    memUsedBytes: memBytesSample?.value ?? null,
    source: "real",
    node: cpuSample?.node ?? memPctSample?.node ?? "—",
    updatedAt: cpuSample?.timestamp ?? memPctSample?.timestamp ?? null,
  };
}

export function StatusBar() {
  const [resources, setResources] = useState<ResourceData>(FALLBACK);
  const [controllerOnline, setControllerOnline] = useState<boolean | null>(null);
  // SSE snapshot already carries every layer/metric; filtering it client-side
  // replaces two separate 5s-polled REST calls with zero extra requests.
  const { snapshot, connected } = useSSE();

  useEffect(() => {
    if (!snapshot) {
      if (!connected) {
        setResources(FALLBACK);
        setControllerOnline(false);
      }
      return;
    }
    const cpuSamples = snapshot.metrics.filter((s) => s.layer === "control");
    setResources(extractResources(cpuSamples));

    const connSample = snapshot.metrics.find((s) => s.metric === "controller_conn_ok");
    if (connSample !== undefined) {
      setControllerOnline(connSample.value > 0.5);
    } else {
      // Se não há sample de conn, assume online se há qualquer dado
      setControllerOnline(cpuSamples.length > 0);
    }
  }, [snapshot, connected]);

  const cpuTone = resources.cpuPct === null ? "neutral" : resources.cpuPct > 85 ? "danger" : resources.cpuPct > 60 ? "neutral" : "ok";
  const memTone = resources.memPct === null ? "neutral" : resources.memPct > 90 ? "danger" : resources.memPct > 70 ? "neutral" : "ok";

  const updatedLabel = useMemo(() => {
    if (!resources.updatedAt) return null;
    try {
      return new Date(resources.updatedAt).toLocaleTimeString("pt-BR");
    } catch {
      return null;
    }
  }, [resources.updatedAt]);

  return (
    <div className="sticky top-0 z-20 flex h-12 items-center justify-between gap-4 border-b border-border-0/60 bg-bg-0/80 px-4 backdrop-blur">
      <div className="flex items-center gap-3">
        <div className="text-[12px] font-semibold tracking-tight text-fg-0">
          Status Global
        </div>
        <div className="h-4 w-px bg-border-0/70" />
        <div className="flex items-center gap-2 text-[12px] text-fg-1">
          <span>Controlador SDN</span>
          <Badge tone={controllerOnline === null ? "neutral" : controllerOnline ? "ok" : "danger"}>
            {controllerOnline === null ? "…" : controllerOnline ? "ONLINE" : "OFFLINE"}
          </Badge>
        </div>
      </div>

      <div className="flex items-center gap-4 text-[12px] text-fg-1">
        {/* CPU */}
        <div className="flex items-center gap-1.5">
          <span>CPU</span>
          <span className="font-mono text-[10px] text-fg-1">({resources.node})</span>
          {resources.cpuPct !== null ? (
            <Badge tone={cpuTone}>{formatPct(resources.cpuPct)}</Badge>
          ) : (
            <Badge tone="neutral">—</Badge>
          )}
        </div>

        {/* Memória */}
        <div className="flex items-center gap-1.5">
          <span>MEM</span>
          {resources.memPct !== null ? (
            <Badge tone={memTone}>
              {resources.memUsedBytes !== null
                ? formatGiB(resources.memUsedBytes)
                : formatPct(resources.memPct)}
            </Badge>
          ) : (
            <Badge tone="neutral">—</Badge>
          )}
        </div>

        {/* Fonte e timestamp */}
        <div className="flex items-center gap-1 text-[10px] text-fg-1">
          {resources.source === "real" ? (
            <>
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
              <span className="font-mono">docker stats</span>
              {updatedLabel && <span className="font-mono opacity-60">{updatedLabel}</span>}
            </>
          ) : (
            <>
              <span className="h-1.5 w-1.5 rounded-full bg-yellow-400" />
              <span className="font-mono text-yellow-400">sem dados</span>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
