from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import os
import random
import shlex
import time
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from platform.backend.core.metrics import ensure_labels
from platform.backend.config_loader import (
    flows_from_config,
    load_platform_config,
    metric_definitions_from_config,
    metric_layer_flags,
    topology_from_config,
    validate_config,
)
from platform.backend.schemas import (
    Experiment,
    ExperimentCreate,
    ExperimentRun,
    ExperimentUpdate,
    FlowRule,
    MetricDefinition,
    MetricLayer,
    MetricQuery,
    MetricRecord,
    MetricSample,
    Topology,
    TopologyCreate,
    TopologyUpdate,
)

from platform.backend.core.metric_catalog import metric_definitions as catalog_metric_definitions
from platform.backend.core.netmon_blocks import list_grc_blocks
from platform.backend.core.netmon_bridge import netmon_metric_definitions, run_host_ping, samples_from_netmon

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NetOps Studio API",
    version="0.1.0",
    docs_url="/docs" if os.getenv("ENV", "dev") == "dev" else None,
    redoc_url="/redoc" if os.getenv("ENV", "dev") == "dev" else None,
)

api_key = os.getenv("API_KEY")
if not api_key and os.getenv("ENV") == "production":
    logger.warning("API_KEY não configurada em produção!")

allowed_origins = os.getenv(
    "CORS_ALLOW_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,"
    "http://localhost:5173,http://localhost:5174,"
    "http://127.0.0.1:5173,http://127.0.0.1:5174,"
    "http://0.0.0.0:5173,http://0.0.0.0:5174",
).split(",")
config_path = os.getenv("PLATFORM_CONFIG_PATH", "platform/experiments/platform_config.json")
raw_dir = Path("raw")
raw_dir.mkdir(parents=True, exist_ok=True)

temp_root = Path(os.getenv("PLATFORM_TEMP_DIR", "temp"))
temp_store_dir = temp_root / "experiments"
temp_experiments_dir = temp_store_dir / "registry"
legacy_temp_experiments_dir = temp_store_dir / "experiments"
temp_runs_dir = temp_store_dir / "runs"
temp_run_metrics_dir = temp_store_dir / "run_metrics"
for _d in [temp_experiments_dir, legacy_temp_experiments_dir, temp_runs_dir, temp_run_metrics_dir]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Persistent experiments store ──────────────────────────────────────────────
# Defaults to  <project_root>/experiments/  but can be overridden via env var.
# This directory is meant to survive across restarts and is safe to back up or
# share.  Sub-folders mirror the temp layout for easy migration.
_experiments_root = Path(os.getenv("EXPERIMENTS_DIR", "experiments"))
experiments_registry_dir = _experiments_root / "registry"
experiments_runs_dir = _experiments_root / "runs"
experiments_run_metrics_dir = _experiments_root / "run_metrics"
for _d in [experiments_registry_dir, experiments_runs_dir, experiments_run_metrics_dir]:
    _d.mkdir(parents=True, exist_ok=True)

temp_configs_dir = temp_root / "configs"
temp_configs_dir.mkdir(parents=True, exist_ok=True)
active_config_path = temp_configs_dir / "active.json"
software_version = (
    os.getenv("SOFTWARE_VERSION")
    or Path("VERSION").read_text(encoding="utf-8").strip()
    if Path("VERSION").exists()
    else "dev"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in allowed_origins if origin.strip()],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
    max_age=600,
)

# Rate limiting: per-IP deque for O(1) amortised window eviction
_rate_limit_store: Dict[str, deque] = defaultdict(deque)
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    bucket = _rate_limit_store[client_ip]
    # Evict expired timestamps from the left in O(1) amortised
    while bucket and now - bucket[0] >= RATE_LIMIT_WINDOW:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_REQUESTS:
        logger.warning(f"Rate limit exceeded for {client_ip}")
        return Response(
            content=json.dumps({"detail": "Too many requests"}),
            status_code=429,
            media_type="application/json"
        )
    bucket.append(now)
    return await call_next(request)



def _generate_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _raw_file_for_layer(layer: MetricLayer) -> Path:
    return raw_dir / f"metrics_{layer}.jsonl"


def _serialize_sample(sample: MetricSample) -> str:
    return json.dumps(
        {
            "timestamp": sample.timestamp.isoformat(),
            "node": sample.node,
            "layer": sample.layer,
            "metric": sample.metric,
            "value": sample.value,
            "details": sample.details,
            "labels": sample.labels,
        }
    )


# Kept open for the life of the process instead of open()+close() every
# single collection cycle — repeated open/append/close is a real syscall
# (and page-cache-metadata) cost paid every cycle for no benefit, since we
# never need more than one writer per layer file at a time.
_raw_file_handles: Dict[str, Any] = {}


def _raw_file_handle(layer_key: str) -> Any:
    handle = _raw_file_handles.get(layer_key)
    if handle is None or handle.closed:
        handle = _raw_file_for_layer(layer_key).open("a", encoding="utf-8")  # type: ignore[arg-type]
        _raw_file_handles[layer_key] = handle
    return handle


def _write_samples_to_raw(samples: Sequence[MetricSample]) -> None:
    """Batch writes grouped by layer, reusing one open handle per layer."""
    t0 = time.perf_counter()
    by_layer: Dict[str, List[str]] = defaultdict(list)
    for sample in samples:
        by_layer[str(sample.layer)].append(_serialize_sample(sample))
    for layer_key, lines in by_layer.items():
        f = _raw_file_handle(layer_key)
        f.write("\n".join(lines) + "\n")
        f.flush()
    _profile_record("raw_file_write", (time.perf_counter() - t0) * 1000)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _persist_experiment(experiment: Experiment) -> None:
    data = json.dumps(experiment.model_dump(mode="json"), indent=2, ensure_ascii=False)
    # Write to both temp (backward compat) and persistent store.
    _atomic_write_text(temp_experiments_dir / f"{experiment.id}.json", data)
    _atomic_write_text(experiments_registry_dir / f"{experiment.id}.json", data)


def _persist_run(run: ExperimentRun) -> None:
    data = json.dumps(run.model_dump(mode="json"), indent=2, ensure_ascii=False)
    _atomic_write_text(temp_runs_dir / f"{run.id}.json", data)
    _atomic_write_text(experiments_runs_dir / f"{run.id}.json", data)


# Rewriting a run's *entire* metric history from scratch on every single
# cycle that touches it is O(n) work that recurs every cycle — effectively
# O(n^2) in total over a long-running run. Debounce it to a fixed cadence;
# `force=True` (run start/stop, imports) always writes the authoritative
# final state immediately.
_run_metrics_last_persist: Dict[str, float] = {}
RUN_METRICS_PERSIST_INTERVAL_SEC = float(os.getenv("RUN_METRICS_PERSIST_INTERVAL_SEC", "15"))


def _persist_run_metrics(run_id: str, force: bool = False) -> None:
    if not force:
        last = _run_metrics_last_persist.get(run_id, 0.0)
        if time.time() - last < RUN_METRICS_PERSIST_INTERVAL_SEC:
            return
    _run_metrics_last_persist[run_id] = time.time()
    metrics = run_metrics.get(run_id, [])
    # Compact separators instead of indent=2: same content, no pretty-print
    # overhead on what can be a large array rewritten from scratch.
    data = json.dumps([m.model_dump(mode="json") for m in metrics], separators=(",", ":"), ensure_ascii=False)
    _atomic_write_text(temp_run_metrics_dir / f"{run_id}.json", data)
    _atomic_write_text(experiments_run_metrics_dir / f"{run_id}.json", data)


def _delete_persisted_experiment(experiment_id: str) -> None:
    for d in [temp_experiments_dir, legacy_temp_experiments_dir, experiments_registry_dir]:
        (d / f"{experiment_id}.json").unlink(missing_ok=True)


def _delete_persisted_run(run_id: str) -> None:
    for d in [temp_runs_dir, experiments_runs_dir]:
        (d / f"{run_id}.json").unlink(missing_ok=True)
    for d in [temp_run_metrics_dir, experiments_run_metrics_dir]:
        (d / f"{run_id}.json").unlink(missing_ok=True)


def _load_experiments_and_runs_from_temp() -> None:
    # Best-effort restore for researcher workflow; ignore corrupt files.
    # Persistent store (experiments/) takes priority over temp.
    for root in [legacy_temp_experiments_dir, temp_experiments_dir, experiments_registry_dir]:
        for path in sorted(root.glob("*.json")):
            try:
                obj = json.loads(path.read_text(encoding="utf-8"))
                exp = Experiment(**obj)
                experiments[exp.id] = exp
            except Exception:
                continue

    for root in [temp_runs_dir, experiments_runs_dir]:
        for path in sorted(root.glob("*.json")):
            try:
                obj = json.loads(path.read_text(encoding="utf-8"))
                run = ExperimentRun(**obj)
                runs[run.id] = run
                run_metrics.setdefault(run.id, [])
            except Exception:
                continue

    for root in [temp_run_metrics_dir, experiments_run_metrics_dir]:
        for path in sorted(root.glob("*.json")):
            try:
                run_id = path.stem
                items = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(items, list):
                    continue
                run_metrics[run_id] = [MetricRecord(**item) for item in items if isinstance(item, dict)]
            except Exception:
                continue


def _load_active_config_id() -> str | None:
    try:
        if not active_config_path.exists():
            return None
        obj = json.loads(active_config_path.read_text(encoding="utf-8"))
        config_id = obj.get("active") if isinstance(obj, dict) else None
        return str(config_id) if config_id else None
    except Exception:
        return None


def _set_active_config_id(config_id: str) -> None:
    _atomic_write_text(active_config_path, json.dumps({"active": config_id}, indent=2, ensure_ascii=False))


def _config_path_for_id(config_id: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", config_id.strip()).strip("_")
    return temp_configs_dir / f"{safe}.json"


def _list_configs() -> List[Dict[str, Any]]:
    active_id = _load_active_config_id()
    items: List[Dict[str, Any]] = []
    for path in sorted(temp_configs_dir.glob("*.json")):
        if path.name == "active.json":
            continue
        config_id = path.stem
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            name = obj.get("name") if isinstance(obj, dict) else None
            updated_at = obj.get("updated_at") if isinstance(obj, dict) else None
        except Exception:
            name = None
            updated_at = None
        items.append({"id": config_id, "name": name or config_id, "active": config_id == active_id, "updated_at": updated_at})
    return items


def _read_config_payload(config_id: str) -> Dict[str, Any]:
    path = _config_path_for_id(config_id)
    if not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Config not found")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            raise ValueError("config must be an object")
        return obj
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid config JSON")


def _write_config_payload(config_id: str, payload: Dict[str, Any]) -> None:
    # Validate the embedded platform config if present.
    cfg = payload.get("config") if isinstance(payload.get("config"), dict) else None
    if cfg is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing 'config' object")
    validate_config(cfg)
    payload = {
        **payload,
        "id": config_id,
        "updated_at": datetime.utcnow().isoformat(),
    }
    _atomic_write_text(_config_path_for_id(config_id), json.dumps(payload, indent=2, ensure_ascii=False))


def _seed_default_config_if_missing() -> None:
    # Ensure at least one saved config exists: keep current repo default as lft-netops.
    if any(p for p in temp_configs_dir.glob("*.json") if p.name != "active.json"):
        return
    try:
        default_cfg = load_platform_config(config_path)
        payload = {
            "name": "lft-netops",
            "description": "Configuração padrão (docker) do NetOps Studio (Network Operations)",
            "environment": "docker",
            "config": default_cfg,
        }
        _write_config_payload("lft-netops", payload)
        _set_active_config_id("lft-netops")
    except Exception:
        # Best-effort only.
        pass


def _apply_active_config() -> None:
    """Apply the active saved config to in-memory runtime state."""
    global metric_definitions, layer_flags
    active_id = _load_active_config_id()
    if not active_id:
        return
    obj = _read_config_payload(active_id)
    cfg = obj.get("config") if isinstance(obj.get("config"), dict) else None
    if not cfg:
        return
    topology = topology_from_config(cfg)
    if topology and topology.id not in topologies:
        topologies[topology.id] = topology

    cfg_metrics = metric_definitions_from_config(cfg)
    if cfg_metrics:
        merged: Dict[str, MetricDefinition] = {m.name: m for m in _extended_metric_definitions()}
        merged.update({m.name: m for m in cfg_metrics})
        metric_definitions = list(merged.values())
    else:
        metric_definitions = _extended_metric_definitions()

    layer_flags = metric_layer_flags(cfg)


def _load_recent_samples_from_raw(limit: int = 200) -> List[MetricSample]:
    loaded: List[MetricSample] = []

    def _tail(path: Path, max_lines: int) -> List[str]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        return lines[-max_lines:]

    for layer in [
        "service",
        "physical",
        "link",
        "network",
        "transport",
        "session",
        "presentation",
        "application",
        "control",
        "dataplane",
    ]:
        for line in _tail(_raw_file_for_layer(layer), limit):
            try:
                obj = json.loads(line)
                loaded.append(
                    MetricSample(
                        timestamp=datetime.fromisoformat(obj.get("timestamp")),
                        node=obj.get("node", ""),
                        layer=obj.get("layer", layer),
                        metric=obj.get("metric", ""),
                        value=float(obj.get("value", 0)),
                        details=obj.get("details") or {},
                        labels=_normalize_labels(obj.get("labels") or {}),
                    )
                )
            except Exception:
                continue
    return loaded


def _normalize_labels(labels: Dict[str, Any] | None) -> Dict[str, Any]:
    normalized = ensure_labels(labels or {})
    normalized.setdefault("version", software_version)
    return normalized


def _normalize_sample(sample: MetricSample) -> MetricSample:
    # Samples built by _collect_real()/_collect_synthetic_full() already
    # normalize their labels at construction time, so this is a no-op for
    # the overwhelming majority of calls. Re-running pydantic validation to
    # build an identical object anyway is pure wasted CPU on the hot path
    # (every sample, every cycle) — skip it when nothing would change.
    normalized_labels = _normalize_labels(sample.labels)
    if normalized_labels == sample.labels:
        return sample
    return MetricSample(
        timestamp=sample.timestamp,
        node=sample.node,
        layer=sample.layer,
        metric=sample.metric,
        value=sample.value,
        details=sample.details,
        labels=normalized_labels,
    )


def _append_run_log(run_id: str, message: str) -> None:
    run = runs.get(run_id)
    if not run:
        return
    entry = f"{datetime.utcnow().isoformat()} {message}"
    run.logs.append(entry)


def _store_samples(samples: Sequence[MetricSample]) -> None:
    normalized = [_normalize_sample(s) for s in samples]
    touched_run_ids: set[str] = set()
    for s in normalized:
        # Update O(1) latest index
        key = f"{s.metric}:{s.node}:{s.layer}"
        cur = _samples_latest.get(key)
        if cur is None or s.timestamp >= cur.timestamp:
            _samples_latest[key] = s
        # Append to bounded history ring-buffer
        metric_samples.append(s)
        seen_metric_names.add(str(s.metric))
        # Accumulate run-linked records
        run_id = s.labels.get("run_id") if s.labels else None
        if run_id:
            touched_run_ids.add(str(run_id))
            run_metrics.setdefault(str(run_id), []).append(
                MetricRecord(
                    timestamp=s.timestamp,
                    metric_name=s.metric,
                    value=s.value,
                    layer=s.layer,
                    labels=s.labels,
                    details=s.details,
                )
            )
            _append_run_log(str(run_id), f"metric:{s.metric}")
    _write_samples_to_raw(normalized)
    for run_id in touched_run_ids:
        try:
            _persist_run_metrics(run_id)
        except Exception:
            pass
    # Build the SSE frame once here (not once per connected client — see
    # _refresh_sse_cache) and wake any subscribers. Safe to call from the
    # event-loop thread or from a worker thread (call_soon_threadsafe
    # handles both; the cache write itself is plain dict/pydantic work
    # under the GIL, safe from either thread).
    try:
        _refresh_sse_cache()
    except Exception:
        pass
    if _main_loop is not None:
        _main_loop.call_soon_threadsafe(_samples_updated.set)
    else:
        _samples_updated.set()


# ── Thread-safe, per-call manual profiling (see /debug/collect_profile) ──────
# cProfile only instruments the thread it's enabled on; almost all real work
# here runs inside asyncio.to_thread() worker threads, which cProfile can't
# see. Each probe times itself instead, so this works correctly regardless of
# which thread it runs on.
#
# This is a ContextVar, not a plain global: a bare global flag/dict is shared
# by EVERY concurrent call, so a profile triggered via the debug endpoint
# would also capture (and get corrupted by) the background _auto_collect_loop
# firing at the same time — same global counter, two unrelated cycles adding
# to it, producing nonsense totals. `contextvars.ContextVar` gives each
# logical call chain (an asyncio Task, e.g. one HTTP request or one
# _auto_collect_loop iteration) its own value, and `asyncio.to_thread()`
# copies the *calling* context into its worker thread — so a profiled
# request's own to_thread() calls are captured correctly, while a background
# loop iteration running concurrently in its own Task/context is not.
import contextvars

_profile_ctx: contextvars.ContextVar[Dict[str, Dict[str, float]] | None] = contextvars.ContextVar(
    "profile_ctx", default=None
)


def _profile_record(name: str, elapsed_ms: float) -> None:
    stats = _profile_ctx.get()
    if stats is None:
        return
    entry = stats.setdefault(name, {"count": 0.0, "total_ms": 0.0})
    entry["count"] += 1
    entry["total_ms"] += elapsed_ms


def _run_cmd(cmd: List[str], timeout: int = 6) -> str:
    t0 = time.perf_counter()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=True)
        return res.stdout
    except (subprocess.SubprocessError, OSError):
        return ""
    finally:
        if cmd[:3] == ["docker", "stats", "--no-stream"]:
            label = "docker_stats_cli"
        elif cmd[:2] == ["docker", "exec"]:
            label = "docker_cli_fallback"
        else:
            label = "run_cmd_other"
        _profile_record(label, (time.perf_counter() - t0) * 1000)


# BUG FIX (found via investigation of a bimodal ~20ms/~1000ms EOL pattern):
# bare `asyncio.to_thread()` runs on Python's single process-wide default
# executor (min(32, cpu_count+4) workers by default), shared by every
# asyncio.to_thread call in the whole process. The background auto-collect
# loop's per-node exec calls and an explicit /metrics/collect request both
# go through it, so a request landing while the loop's own cycle is mid-
# flight queues behind it for a full collection cycle instead of running
# concurrently — the collector's own background work was starving the very
# request meant to observe it. A dedicated, larger pool for this path
# only keeps it isolated from other asyncio.to_thread users in the process
# and gives it enough headroom that our own concurrency (up to 32 per-node
# threads) doesn't self-contend either.
_collection_executor = ThreadPoolExecutor(max_workers=64, thread_name_prefix="netops-collect")


# BUG FIX: raw `loop.run_in_executor(executor, func, *args)` does NOT copy the
# calling contextvars.Context into the worker thread the way `asyncio.to_thread`
# does — it's a bare executor submission. _profile_ctx (see /debug/collect_profile)
# relies on context propagation to attribute cost to the right call without a
# shared global; routed through plain run_in_executor, every _profile_record()
# call inside these workers silently saw the *default* (unset) context and
# no-opped, making the profiler blind to exactly the subprocess/exec costs it
# exists to measure. Wrap the submission in the caller's copied context to
# restore that propagation without giving up the dedicated executor.
async def _run_in_collection_executor(func, *args):
    loop = asyncio.get_event_loop()
    ctx = contextvars.copy_context()
    return await loop.run_in_executor(_collection_executor, lambda: ctx.run(func, *args))


async def _run_cmd_async(cmd: List[str], timeout: int = 6) -> str:
    """Offload the blocking subprocess call to a worker thread so it never
    stalls the event loop (every HTTP/SSE request shares that loop)."""
    return await _run_in_collection_executor(_run_cmd, cmd, timeout)


# Marker used to pack several shell commands into a single `docker exec`,
# cutting per-cycle process-spawn count (the dominant CPU-overhead cost)
# without changing what each individual command reports.
_SCRIPT_SEP = "###NETOPS_SEP###"

# ── Docker SDK client (talks to the daemon socket directly) ──────────────────
# `docker exec ...` via the CLI forks a whole extra `docker` process on top of
# the exec the daemon performs anyway. Reusing one SDK client for the life of
# the process removes that outer fork+exec for every probe, which is a real
# per-cycle CPU cost given ~5 execs/cycle. `docker stats --no-stream` for all
# containers is intentionally left on the CLI path below: it's already a
# single subprocess for the whole topology (as cheap as it gets) and its
# CPU/mem percentages are computed by Docker's own well-tested formula: no
# reason to duplicate that math by hand without a live daemon to validate it against.
_docker_client: Any = None
_docker_client_failed = False
# Populated on failure so /debug/collect_profile can show the *actual* reason
# instead of a bare boolean — "still on the broken docker==7.0.0 adapter",
# "no daemon socket", "permission denied on docker.sock", etc. are very
# different fixes, and guessing between them without this wastes a round trip.
_docker_client_error: str | None = None


def _get_docker_client() -> Any:
    global _docker_client, _docker_client_failed, _docker_client_error
    if _docker_client is not None or _docker_client_failed:
        return _docker_client
    try:
        import docker as docker_sdk
        # BUG FIX (found via investigation of a bimodal ~20ms/~1000ms EOL
        # pattern): the SDK's default connection pool (requests/urllib3
        # HTTPAdapter, pool_maxsize=10) is far smaller than our per-node
        # collection concurrency (up to 32 threads, plus the background
        # auto-collect loop hitting the same shared client every cycle).
        # Threads beyond the pool size block waiting for a free connection,
        # which is exactly the alternating fast/slow pattern observed:
        # whichever calls land within the pool size go fast, the rest queue
        # for a full request's worth of latency. Raise the pool to
        # comfortably exceed our own max concurrency.
        _docker_client = docker_sdk.from_env(timeout=10, max_pool_size=64)
    except Exception as exc:
        _docker_client_failed = True
        _docker_client = None
        _docker_client_error = f"{type(exc).__name__}: {exc}"
    return _docker_client


# v2: exec direto pela Engine API (socket unix), sem fork do CLI `docker` por sonda e sem
# depender do docker-py. ENGINE_API_EXEC=0 restaura o caminho anterior (SDK/CLI).
ENGINE_API_EXEC = os.getenv("ENGINE_API_EXEC", "1") == "1"
# v2: sondas de rapida+extra de um mesmo no vao num unico exec (menos execs por ciclo).
MERGE_EXEC = os.getenv("MERGE_EXEC", "1") == "1"
_DOCKER_SOCK = os.getenv("DOCKER_SOCK_PATH", "/var/run/docker.sock")
_PART_SEP = "###NETOPS_PART###"


def _engine_request(method: str, path: str, body: dict | None = None, timeout: float = 12.0) -> tuple[int, bytes]:
    import socket
    payload = json.dumps(body).encode() if body is not None else b""
    req = (f"{method} {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n"
           f"Content-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n").encode() + payload
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(_DOCKER_SOCK)
        s.sendall(req)
        chunks = []
        while True:
            b = s.recv(65536)
            if not b:
                break
            chunks.append(b)
    finally:
        s.close()
    raw = b"".join(chunks)
    head, _, data = raw.partition(b"\r\n\r\n")
    status = int(head.split(b" ", 2)[1])
    if b"transfer-encoding: chunked" in head.lower():
        out, i = b"", 0
        while i < len(data):
            j = data.find(b"\r\n", i)
            if j < 0:
                break
            n = int(data[i:j].split(b";")[0] or b"0", 16)
            if n == 0:
                break
            out += data[j + 2:j + 2 + n]
            i = j + 2 + n + 2
        data = out
    return status, data


def _engine_exec(container: str, script: str) -> str:
    st, data = _engine_request("POST", f"/containers/{container}/exec", {
        "AttachStdout": True, "AttachStderr": True, "Tty": False, "Cmd": ["sh", "-c", script + " ; true"]})
    if st != 201:
        raise RuntimeError(f"exec create {st}")
    eid = json.loads(data)["Id"]
    st, data = _engine_request("POST", f"/exec/{eid}/start", {"Detach": False, "Tty": False})
    if st != 200:
        raise RuntimeError(f"exec start {st}")
    out, i = [], 0
    while i + 8 <= len(data):   # fluxo multiplexado: 8 bytes de cabecalho (tipo, 3 pad, tamanho)
        n = int.from_bytes(data[i + 4:i + 8], "big")
        out.append(data[i + 8:i + 8 + n])
        i += 8 + n
    return b"".join(out).decode("utf-8", errors="replace")


def _docker_exec_sync(container: str, script: str) -> str:
    """SDK-based equivalent of `docker exec container sh -c script`.
    Falls back to the CLI if the SDK/daemon socket isn't reachable, so this
    degrades gracefully in environments without direct socket access."""
    if ENGINE_API_EXEC:
        try:
            t0 = time.perf_counter()
            res = _engine_exec(container, script)
            _profile_record("docker_engine_exec", (time.perf_counter() - t0) * 1000)
            return res
        except Exception:
            pass
    client = None if ENGINE_API_EXEC else _get_docker_client()
    full_script = script + " ; true"
    if client is None:
        return _run_cmd(["docker", "exec", container, "sh", "-c", full_script], timeout=10)
    try:
        t0 = time.perf_counter()
        cont = client.containers.get(container)
        exit_code, output = cont.exec_run(["sh", "-c", full_script], demux=False)
        _profile_record("docker_sdk_exec", (time.perf_counter() - t0) * 1000)
        if isinstance(output, bytes):
            return output.decode("utf-8", errors="replace")
        return str(output or "")
    except Exception:
        # Container renamed/restarted, socket hiccup, etc. — CLI fallback
        # keeps this cycle's other probes from silently going empty forever.
        return _run_cmd(["docker", "exec", container, "sh", "-c", full_script], timeout=10)


async def _run_docker_script(container: str, script: str, timeout: int = 10) -> str:
    if not script:
        return ""
    return await _run_in_collection_executor(_docker_exec_sync, container, script)


# Previous interface byte counters for delta-based utilization (node:iface -> (rx_bytes, tx_bytes, timestamp))
_prev_net_stats: Dict[str, tuple[float, float, float]] = {}


def _parse_docker_stats(container: str) -> Dict[str, float]:
    """Fetch stats for one container (called individually when needed)."""
    return _parse_docker_stats_batch([container]).get(container, {})


def _parse_docker_stats_batch(containers: List[str]) -> Dict[str, Dict[str, float]]:
    """Single 'docker stats --no-stream' call for all containers → O(1) Docker API round-trip."""
    if not containers:
        return {}
    out = _run_cmd(
        [
            "docker", "stats", "--no-stream",
            "--format",
            '{"name":"{{.Name}}","cpu":"{{.CPUPerc}}","mem":"{{.MemPerc}}"}',
        ] + containers,
        timeout=15,
    )
    results: Dict[str, Dict[str, float]] = {}
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except Exception:
            continue
        name = str(data.get("name") or "").strip()
        def _pct(s: Any) -> float | None:
            try:
                return float(str(s or "").strip().rstrip("%"))
            except ValueError:
                return None
        entry: Dict[str, float] = {}
        cpu = _pct(data.get("cpu"))
        mem = _pct(data.get("mem"))
        if cpu is not None:
            entry["cpu_pct"] = cpu
        if mem is not None:
            entry["mem_pct"] = mem
        if name:
            results[name] = entry
            # Also index by original requested name (may differ from docker Name)
            for req in containers:
                if req == name or name.lstrip("/") == req.lstrip("/"):
                    results[req] = entry
    return results



def _parse_ss_summary(output: str) -> Dict[str, float]:
    summary: Dict[str, float] = {
        "total": 0,
        "tcp_total": 0,
        "tcp_estab": 0,
        "tcp_closed": 0,
        "tcp_orphaned": 0,
        "tcp_timewait": 0,
    }
    matrix: Dict[str, Dict[str, float]] = {proto: {"total": 0, "ip": 0, "ipv6": 0} for proto in ["udp", "tcp", "raw", "inet", "frag"]}
    for line in output.splitlines():
        if line.startswith("Total:"):
            m = re.search(r"Total:\s+(\d+)", line)
            if m:
                summary["total"] = float(m.group(1))
        if line.startswith("TCP:"):
            m = re.search(r"TCP:\s+(\d+).*estab\s+(\d+), closed\s+(\d+), orphaned\s+(\d+), timewait\s+(\d+)", line)
            if m:
                summary.update(
                    {
                        "tcp_total": float(m.group(1)),
                        "tcp_estab": float(m.group(2)),
                        "tcp_closed": float(m.group(3)),
                        "tcp_orphaned": float(m.group(4)),
                        "tcp_timewait": float(m.group(5)),
                    }
                )
        if any(line.startswith(prefix) for prefix in ["UDP", "TCP", "RAW", "INET", "FRAG"]):
            parts = line.split()
            if len(parts) >= 4:
                proto = parts[0].lower()
                try:
                    total, ip_v4, ip_v6 = float(parts[1]), float(parts[2]), float(parts[3])
                    matrix.setdefault(proto, {"total": 0, "ip": 0, "ipv6": 0})
                    matrix[proto].update({"total": total, "ip": ip_v4, "ipv6": ip_v6})
                except ValueError:
                    continue
    return {**summary, **{f"{proto}_{k}": v for proto, vals in matrix.items() for k, v in vals.items()}}


def _parse_snmp(output: str) -> Dict[str, float]:
    lines = [ln for ln in output.splitlines() if ":" in ln]
    results: Dict[str, float] = {}
    i = 0
    while i < len(lines) - 1:
        header = lines[i].split()
        values = lines[i + 1].split()
        if header[0].rstrip(":") == values[0].rstrip(":"):
            prefix = header[0].rstrip(":").lower()
            keys = header[1:]
            vals = values[1:]
            for k, v in zip(keys, vals):
                try:
                    results[f"{prefix}_{k.lower()}"] = float(v)
                except ValueError:
                    continue
        i += 2
    return results


def _parse_ip_link(output: str) -> Dict[str, Dict[str, float]]:
    """Parse 'ip -s link' output → {iface: {rx_bytes, tx_bytes, rx_packets, tx_packets, errors, dropped}}"""
    metrics: Dict[str, Dict[str, float]] = {}
    current: str | None = None
    rx_next = False
    tx_next = False
    for line in output.splitlines():
        m_iface = re.match(r"^\d+: (\S+?)[@: ]", line)
        if m_iface:
            current = m_iface.group(1).rstrip("@")
            metrics.setdefault(current, {})
            rx_next = tx_next = False
            continue
        if current is None:
            continue
        stripped = line.strip()
        if stripped.startswith("RX:"):
            rx_next = True
            tx_next = False
            continue
        if stripped.startswith("TX:"):
            tx_next = True
            rx_next = False
            continue
        if rx_next or tx_next:
            parts = stripped.split()
            prefix = "rx" if rx_next else "tx"
            if len(parts) >= 4:
                try:
                    metrics[current][f"{prefix}_bytes"]   = float(parts[0])
                    metrics[current][f"{prefix}_packets"] = float(parts[1])
                    metrics[current][f"{prefix}_errors"]  = float(parts[2])
                    metrics[current][f"{prefix}_dropped"] = float(parts[3])
                except (ValueError, IndexError):
                    pass
            rx_next = tx_next = False
            continue
        # Legacy: lines with 'errors X dropped X' (some kernel versions)
        if current and "errors" in stripped and "dropped" in stripped:
            parts = stripped.split()
            try:
                idx_e = parts.index("errors")
                idx_d = parts.index("dropped")
                metrics[current].setdefault("errors",  float(parts[idx_e - 1]))
                metrics[current].setdefault("dropped", float(parts[idx_d - 1]))
            except (ValueError, IndexError):
                pass
    # Backward-compatible aliases
    for iface, vals in metrics.items():
        vals.setdefault("errors",  vals.get("tx_errors",  0.0))
        vals.setdefault("dropped", vals.get("tx_dropped", 0.0))
    return metrics


# Throttling tiers, all in units of collection cycles:
#  - FAST: ss/snmp/ip-link (cheap-ish, but still one process fork each
#    *inside* the target container per cycle — real CPU, not just wall time).
#  - SLOW: host-to-host ping and the OVS/controller `docker exec` round trips
#    (the most expensive probes: extra ICMP traffic, extra shell forks).
#  - STATS: the single batched `docker stats` call. Defaults to 1 (every
#    cycle, unthrottled — it's the core liveness signal and, being already
#    one process for the whole topology, is cheap on a large one). It's still
#    a FIXED per-cycle cost that does not shrink with node count, though —
#    on a small topology (few nodes to amortize it over) it can end up being
#    the single biggest remaining piece of collector overhead relative to
#    everything else in the cycle. Raise this specifically for small-scale
#    runs if CPU is still over budget after FAST/SLOW tuning; leave it at 1
#    for anything where per-node work already dominates (medium/large).
# Between throttled cycles, `_samples_latest` simply keeps serving the last
# known value, so nothing goes blank on the dashboard — only its update rate
# drops, trading a bit of freshness for materially less CPU per cycle.
# v2: a camada rapida (estado de enlace, TCP/SNMP) passa a ser lida a cada ciclo (antes: a cada 3).
FAST_PROBE_INTERVAL_CYCLES = max(1, int(os.getenv("FAST_PROBE_INTERVAL_CYCLES", "1")))
# v2: contadores de porta e fluxos do OVS entram em TODO ciclo (antes: camada lenta, 1 em cada 10 ciclos).
# Com OVS_EVERY_CYCLE=0 o comportamento da v1 e restaurado (usado na ablacao).
OVS_EVERY_CYCLE = os.getenv("OVS_EVERY_CYCLE", "1") == "1"
# v2: o ciclo de coleta mantem a taxa nominal (agenda por prazo), em vez de dormir o intervalo APOS coletar.
FIXED_RATE_LOOP = os.getenv("FIXED_RATE_LOOP", "1") == "1"
# v2: saude do controlador por HTTP direto do coletor (sem `curl` dentro do container e sem o
# fallback "container vivo = controlador ok"). CONTROLLER_PROBE_MODE=exec restaura a v1.
CONTROLLER_PROBE_MODE = os.getenv("CONTROLLER_PROBE_MODE", "http")
# v2: a sonda ICMP mira o PAR (data_ip/mgmt_ip do outro host), nao o proprio IP do no.
PING_PEER = os.getenv("PING_PEER", "1") == "1"
# v2: nao semear o cache de ultimas metricas com amostras sinteticas quando a coleta e real.
SEED_SYNTHETIC = os.getenv("SEED_SYNTHETIC", "0") == "1"
SLOW_PROBE_INTERVAL_CYCLES = max(1, int(os.getenv("SLOW_PROBE_INTERVAL_CYCLES", "10")))
# BUG FIX (found via a rate-dependent CPU spike pattern in E1): this used to
# be a cycle-count throttle (`_collect_cycle_count % N`), but each auto-collect
# cycle fires every AUTO_COLLECT_INTERVAL seconds -- which itself varies by
# the sampling rate under test (30s/10s/1s). The same N therefore meant wildly
# different *real* time gaps between docker-stats probes depending on rate
# (e.g. N=35 is ~17.5min at rate=30s but only ~5.8min at rate=10s and ~35s at
# rate=1s), so the probe's real-world frequency -- and the CPU spike it
# causes -- silently scaled with whatever rate was under test. Throttling on
# wall-clock time instead makes the probe's frequency (and its cost) the same
# real-world quantity regardless of rate.
STATS_PROBE_INTERVAL_SEC = float(os.getenv("STATS_PROBE_INTERVAL_SEC", "1050"))
_last_stats_probe_ts = time.time()


def _build_probe_script(node: Any, include_fast: bool, include_slow: bool, ping_target: str | None = None) -> tuple[str, List[str]]:
    """Build one shell script covering transport/link (+ ping for hosts) so a
    node needs at most one `docker exec` for all of them instead of up to 5."""
    cmds: List[str] = []
    sections: List[str] = []
    if include_fast:
        if layer_flags.get("transport", True):
            cmds.append("ss -s")
            sections.append("ss")
            cmds.append("cat /proc/net/snmp")
            sections.append("snmp")
        if layer_flags.get("link", True) or layer_flags.get("physical", True):
            # `-s` already includes the same header line `ip link show` prints
            # (with the interface state), so a second exec just to read state
            # is redundant — one fewer process fork per node, every cycle.
            cmds.append("ip -s link")
            sections.append("iplink")
    if include_slow and layer_flags.get("network", True) and node.type == "host" and node.mgmt_ip:
        # 2 probes @ 20ms spacing still yields min/avg/max/mdev but with a
        # ~20-40ms floor instead of the previous ~600ms (-c3/-i0.2).
        cmds.append(f"ping -c 2 -i 0.02 -W 1 {shlex.quote(ping_target or node.mgmt_ip)}")
        sections.append("ping")
    if not cmds:
        return "", []
    script = f" ; echo '{_SCRIPT_SEP}' ; ".join(cmds)
    return script, sections


def _build_extra_script(node: Any, include_slow: bool) -> tuple[str, str]:
    """Controller reachability probe / OVS bridge+ports+flows in a single exec."""
    if node.type == "controller":
        if not include_slow or CONTROLLER_PROBE_MODE == "http":
            return "", ""
        script = (
            "for p in 8080 8181 6633; do "
            "out=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 2 "
            "http://127.0.0.1:$p/ 2>/dev/null); "
            "code=$(printf '%s' \"$out\" | cut -d' ' -f1); "
            "case \"$code\" in 200|401|404) printf '%s' \"$out\"; exit 0;; esac; "
            "done; printf '000 0'"
        )
        return script, "controller"
    if node.type == "switch" and (include_slow or OVS_EVERY_CYCLE) and (layer_flags.get("control", True) or layer_flags.get("dataplane", True)):
        parts = [
            "br=$(ovs-vsctl list-br 2>/dev/null | head -1)",
            "echo \"BRIDGE:$br\"",
        ]
        parts.append(f"echo '{_SCRIPT_SEP}'")
        parts.append("ovs-ofctl dump-ports \"$br\" 2>/dev/null" if layer_flags.get("control", True) else "true")
        parts.append(f"echo '{_SCRIPT_SEP}'")
        parts.append("ovs-ofctl dump-flows \"$br\" 2>/dev/null" if layer_flags.get("dataplane", True) else "true")
        return " ; ".join(parts), "switch"
    return "", ""


async def _empty_str() -> str:
    return ""


_collect_cycle_count = 0


def _http_probe_controller(host: str, ports: tuple = (8080, 8181), timeout: float = 1.5) -> tuple[bool, float | None]:
    """Sonda HTTP do controlador a partir do coletor. Ok = resposta 200/401/404 (como na v1)."""
    import urllib.error
    import urllib.request
    for port in ports:
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(f"http://{host}:{port}/", timeout=timeout) as r:
                code = r.status
        except urllib.error.HTTPError as e:
            code = e.code
        except Exception:
            continue
        if code in (200, 401, 404):
            return True, round((time.perf_counter() - t0) * 1000, 2)
    return False, None


async def _collect_real(topology: Topology | None, labels: Dict[str, Any] | None = None) -> List[MetricSample]:
    global _collect_cycle_count
    if not topology:
        return []
    now = datetime.utcnow()
    now_ts = time.time()
    base_labels = _normalize_labels(labels)
    _collect_cycle_count += 1
    # (n - 1) % N == 0, not n % N == 1: the latter is *always* False when
    # N == 1 (anything mod 1 is 0, never 1), which would silently disable a
    # tier entirely at its most common setting ("every cycle"). This form
    # correctly fires on cycle 1, N+1, 2N+1, ... for every N >= 1.
    include_fast = (_collect_cycle_count - 1) % FAST_PROBE_INTERVAL_CYCLES == 0
    include_slow = (_collect_cycle_count - 1) % SLOW_PROBE_INTERVAL_CYCLES == 0
    global _last_stats_probe_ts
    include_stats = (now_ts - _last_stats_probe_ts) >= STATS_PROBE_INTERVAL_SEC
    if include_stats:
        _last_stats_probe_ts = now_ts

    def _mk(metric: str, layer: MetricLayer, node: str, value: float, details: Dict | None = None, labels: Dict | None = None) -> MetricSample:
        return MetricSample(
            timestamp=now,
            node=node,
            layer=layer,
            metric=metric,
            value=value,
            details=details or {},
            # base_labels is already normalized once above; only re-normalize
            # when a call site passes a distinct per-sample override, instead
            # of rebuilding the same 7-key dict for every sample in the cycle.
            labels=_normalize_labels(labels) if labels else base_labels,
        )

    # ── Single docker stats call for ALL containers (1 Docker API round-trip),
    # offloaded to a worker thread so it never blocks the event loop. Gated
    # by STATS_PROBE_INTERVAL_CYCLES (default 1 = every cycle); when skipped,
    # cpu/mem samples simply aren't re-emitted this cycle and _samples_latest
    # keeps serving the last known reading, same as the FAST/SLOW tiers. ────
    container_names = [node.meta.get("container_name") or node.id for node in topology.nodes]
    all_dstats = await _run_in_collection_executor(_parse_docker_stats_batch, container_names) if include_stats else {}

    async def _collect_node(node: Any) -> List[MetricSample]:
        node_samples: List[MetricSample] = []
        try:
            container = node.meta.get("container_name") or node.id

            # ── Docker stats: CPU & memory for ALL node types ────────────────
            dstats = all_dstats.get(container, {})
            cpu_pct = dstats.get("cpu_pct")
            mem_pct = dstats.get("mem_pct")
            # Only the docker_-prefixed name is emitted now: every real
            # consumer (StatusBar, TopologyView, TopologyEditor) already
            # falls back from docker_cpu_util_pct/docker_mem_util_pct to the
            # unprefixed name, so the second copy was pure duplicated
            # construction/storage/serialization cost, every node, every
            # cycle, unthrottled.
            if cpu_pct is not None:
                node_samples.append(_mk("docker_cpu_util_pct", "control", node.id, round(cpu_pct, 3), {"container": container}))
            if mem_pct is not None:
                node_samples.append(_mk("docker_mem_util_pct", "control", node.id, round(mem_pct, 3), {"container": container}))

            # ── Run at most 2 `docker exec` calls for this node, concurrently ─
            _ping_target = None
            if PING_PEER and node.type == "host":
                _peers = [n for n in topology.nodes if n.type == "host" and n.id != node.id and (n.meta.get("data_ip") or n.mgmt_ip)]
                if _peers:
                    _ping_target = _peers[0].meta.get("data_ip") or _peers[0].mgmt_ip
            probe_script, sections = _build_probe_script(node, include_fast, include_slow, _ping_target)
            extra_script, extra_kind = _build_extra_script(node, include_slow)
            if MERGE_EXEC and probe_script and extra_script:
                _merged = await _run_docker_script(container, f"{probe_script} ; echo '{_PART_SEP}' ; {extra_script}")
                probe_out, _, extra_out = _merged.partition(_PART_SEP)
            else:
                probe_out, extra_out = await asyncio.gather(
                    _run_docker_script(container, probe_script) if probe_script else _empty_str(),
                    _run_docker_script(container, extra_script) if extra_script else _empty_str(),
                )

            # ── Controller-specific metrics ──────────────────────────────────
            if node.type == "controller" and CONTROLLER_PROBE_MODE == "http" and node.mgmt_ip and (include_slow or OVS_EVERY_CYCLE):
                _ok, _ms = await _run_in_collection_executor(_http_probe_controller, node.mgmt_ip)
                node_samples.append(_mk("controller_conn_ok", "control", node.id, 1.0 if _ok else 0.0, {"source": "http"}))
                if _ms is not None:
                    node_samples.append(_mk("controller_latency_ms", "control", node.id, _ms, {"source": "http"}))
            if extra_kind == "controller":
                code, _, elapsed_s = extra_out.strip().partition(" ")
                ctrl_ok = code in {"200", "401", "404"}
                ctrl_latency_ms = round(float(elapsed_s) * 1000, 2) if ctrl_ok and elapsed_s else None
                # Fallback: check if process is alive via docker stats success
                if not ctrl_ok and cpu_pct is not None:
                    ctrl_ok = True
                node_samples.append(_mk("controller_conn_ok",    "control", node.id, 1.0 if ctrl_ok else 0.0, {"source": "curl"}))
                if ctrl_latency_ms is not None:
                    node_samples.append(_mk("controller_latency_ms", "control", node.id, ctrl_latency_ms, {"source": "curl"}))

            if probe_out and sections:
                seg = probe_out.split(_SCRIPT_SEP)
                seg += [""] * (len(sections) - len(seg))
                seg_map = dict(zip(sections, (s.strip("\n") for s in seg)))

                # ── Transport: ss -s and /proc/net/snmp ──────────────────────
                if "ss" in seg_map:
                    ss_data = _parse_ss_summary(seg_map["ss"]) if seg_map["ss"] else {}
                    if ss_data:
                        node_samples.append(_mk("ss_sockets_total", "transport", node.id, ss_data.get("total", 0.0), {"source": "ss -s"}))
                        node_samples.append(
                            _mk(
                                "ss_tcp_states",
                                "transport",
                                node.id,
                                ss_data.get("tcp_total", 0.0),
                                {
                                    "estab": ss_data.get("tcp_estab", 0.0),
                                    "closed": ss_data.get("tcp_closed", 0.0),
                                    "orphaned": ss_data.get("tcp_orphaned", 0.0),
                                    "timewait": ss_data.get("tcp_timewait", 0.0),
                                    "source": "ss -s",
                                },
                            )
                        )
                        for proto in ["udp", "tcp", "raw", "inet", "frag"]:
                            node_samples.append(
                                _mk(
                                    f"ss_{proto}_sockets_total",
                                    "transport",
                                    node.id,
                                    ss_data.get(f"{proto}_total", 0.0),
                                    {
                                        "ip": ss_data.get(f"{proto}_ip", 0.0),
                                        "ipv6": ss_data.get(f"{proto}_ipv6", 0.0),
                                        "source": "ss -s",
                                    },
                                )
                            )

                if "snmp" in seg_map and seg_map["snmp"]:
                    snmp_data = _parse_snmp(seg_map["snmp"])
                    for key, val in snmp_data.items():
                        node_samples.append(_mk(f"snmp_{key}", "transport", node.id, float(val), {"source": "/proc/net/snmp"}))

                # ── Link/Physical: ip -s link ─────────────────────────────────
                if "iplink" in seg_map and seg_map["iplink"]:
                    ip_link_stats = _parse_ip_link(seg_map["iplink"])

                    iface_up: Dict[str, bool] = {}
                    for m_st in re.finditer(r"\d+: (\S+?)[@: ].+?state (\S+)", seg_map.get("iplink", "")):
                        iface_up[m_st.group(1).rstrip("@")] = m_st.group(2).upper() == "UP"

                    for iface, vals in ip_link_stats.items():
                        if iface in {"lo"}:
                            continue
                        path = f"{node.id}:{iface}"
                        if layer_flags.get("physical", True):
                            node_samples.append(_mk("if_errors",   "physical", path, vals.get("errors",  0.0), {"source": "ip -s link"}))
                            node_samples.append(_mk("if_discards", "physical", path, vals.get("dropped", 0.0), {"source": "ip -s link"}))

                        if layer_flags.get("link", True):
                            node_samples.append(_mk("queue_drops", "link", path, vals.get("dropped", 0.0), {"source": "ip -s link"}))
                            up_val = float(iface_up.get(iface, True))
                            node_samples.append(_mk("if_link_up", "link", path, up_val, {"source": "ip link show"}))

                            # Delta-based utilization: compare with previous reading
                            prev_key = f"{node.id}:{iface}"
                            rx_bytes = vals.get("rx_bytes", 0.0)
                            tx_bytes = vals.get("tx_bytes", 0.0)
                            prev = _prev_net_stats.get(prev_key)
                            if prev is not None:
                                prev_rx, prev_tx, prev_ts = prev
                                dt = now_ts - prev_ts
                                if dt > 0:
                                    rx_bps = (rx_bytes - prev_rx) / dt
                                    tx_bps = (tx_bytes - prev_tx) / dt
                                    capacity_bps = 1_000_000_000.0  # assume 1 Gbps
                                    util = round(max(rx_bps, tx_bps) / capacity_bps * 100.0, 3)
                                    util = max(0.0, min(100.0, util))
                                    node_samples.append(_mk("if_in_util_pct",  "link", path, round(rx_bps / capacity_bps * 100.0, 3), {"source": "ip -s link delta"}))
                                    node_samples.append(_mk("if_out_util_pct", "link", path, round(tx_bps / capacity_bps * 100.0, 3), {"source": "ip -s link delta"}))
                                    # Emit per-link util using source->target key as well
                                    node_samples.append(_mk("link_util_pct", "link", path, util, {"source": "ip -s link delta"}))
                            _prev_net_stats[prev_key] = (rx_bytes, tx_bytes, now_ts)

                # ── Network: ping between hosts ───────────────────────────────
                if "ping" in seg_map and seg_map["ping"]:
                    peers = [n for n in topology.nodes if n.type == "host" and n.id != node.id and n.mgmt_ip]
                    if peers:
                        target = peers[0]
                        ping_output = seg_map["ping"]
                        m = re.search(r"(\d+)% packet loss", ping_output)
                        loss = float(m.group(1)) if m else 0.0
                        rtt_match = re.search(r"rtt min/avg/max/mdev = ([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+)", ping_output)
                        if rtt_match:
                            latency = float(rtt_match.group(2))
                            jitter = float(rtt_match.group(4))
                        else:
                            latency = 0.0
                            jitter = 0.0
                        node_samples.append(_mk("latency_ms",      "network", node.id, latency, {"dst": target.id, "source": "ping"}))
                        node_samples.append(_mk("packet_loss_pct", "network", node.id, loss,    {"dst": target.id, "source": "ping"}))
                        node_samples.append(_mk("jitter_ms",       "network", node.id, jitter,  {"dst": target.id, "source": "ping"}))

            # ── Control/Dataplane: OVS on switches ────────────────────────────
            if extra_kind == "switch":
                raw_parts = extra_out.split(_SCRIPT_SEP)
                bridge_line = raw_parts[0] if raw_parts else ""
                bridge = bridge_line.split("BRIDGE:")[-1].strip() or f"br-{node.id}"
                ofctl_ports = raw_parts[1].strip() if len(raw_parts) > 1 else ""
                flows_dump = raw_parts[2].strip() if len(raw_parts) > 2 else ""

                if layer_flags.get("control", True) and ofctl_ports:
                    node_samples.append(_mk("controller_conn_ok", "control", node.id, 1.0, {"source": "ovs-ofctl", "bridge": bridge}))
                    # Parse per-port rx/tx from dump-ports
                    port_rx = re.findall(r"rx pkts=(\d+)", ofctl_ports)
                    port_tx = re.findall(r"tx pkts=(\d+)", ofctl_ports)
                    total_rx = sum(int(x) for x in port_rx)
                    total_tx = sum(int(x) for x in port_tx)
                    node_samples.append(_mk("port_rx_pkts", "dataplane", node.id, float(total_rx), {"source": "ovs-ofctl", "bridge": bridge}))
                    node_samples.append(_mk("port_tx_pkts", "dataplane", node.id, float(total_tx), {"source": "ovs-ofctl", "bridge": bridge}))
                    # Drop counters
                    port_drop = re.findall(r"tx drop=(\d+)", ofctl_ports)
                    total_drop = sum(int(x) for x in port_drop)
                    node_samples.append(_mk("port_tx_drops", "dataplane", node.id, float(total_drop), {"source": "ovs-ofctl", "bridge": bridge}))
                    # Queue occupancy proxy: drop% as occupancy
                    if total_tx > 0:
                        occ = round(total_drop / total_tx * 100.0, 3)
                        node_samples.append(_mk("queue_occupancy_pct", "link", node.id, occ, {"source": "ovs-ofctl"}))

                if layer_flags.get("dataplane", True) and flows_dump:
                    flow_lines = [ln for ln in flows_dump.splitlines() if "n_packets" in ln]
                    for idx, ln in enumerate(flow_lines[:10]):
                        m_pkts  = re.search(r"n_packets=(\d+)", ln)
                        m_bytes = re.search(r"n_bytes=(\d+)", ln)
                        pkts = float(m_pkts.group(1))  if m_pkts  else 0.0
                        byt  = float(m_bytes.group(1)) if m_bytes else 0.0
                        node_samples.append(_mk("openflow_flow_packets", "dataplane", node.id, pkts, {"flow_id": f"flow_{idx}", "source": "ovs-ofctl"}))
                        node_samples.append(_mk("openflow_flow_bytes",   "dataplane", node.id, byt,  {"flow_id": f"flow_{idx}", "source": "ovs-ofctl"}))
                    total_pkts  = sum(float(re.search(r"n_packets=(\d+)", ln).group(1)) for ln in flow_lines if re.search(r"n_packets=(\d+)", ln))
                    node_samples.append(_mk("flows_installed", "control",   node.id, float(len(flow_lines)),  {"source": "ovs-ofctl"}))
                    node_samples.append(_mk("table_hits",      "dataplane", node.id, round(total_pkts, 0),   {"source": "ovs-ofctl"}))
                    node_samples.append(_mk("table_misses",    "dataplane", node.id, 0.0,                    {"source": "ovs-ofctl"}))

        except Exception as e:
            logger.error(f"Erro ao coletar métricas do nó {node.id}: {e}")
        return node_samples

    # ── Collect every node concurrently instead of serially ──────────────────
    per_node = await asyncio.gather(*(_collect_node(node) for node in topology.nodes))
    samples: List[MetricSample] = [s for group in per_node for s in group]

    # ── Emit per-link (source->target) aggregated metrics ───────────────────
    for link in topology.links:
        link_path = f"{link.source}->{link.target}"
        # Propagate link_util_pct: max of all iface-level util samples for that source node
        src_utils = [
            s.value for s in samples
            if s.metric == "link_util_pct" and s.node.startswith(f"{link.source}:")
        ]
        if src_utils:
            samples.append(_mk("link_util_pct", "link", link_path, round(max(src_utils), 3), {"source": "aggregated"}))
        # if_link_up: mark link down if any iface on source or target is down
        src_up = [
            s.value for s in samples
            if s.metric == "if_link_up" and (s.node.startswith(f"{link.source}:") or s.node.startswith(f"{link.target}:"))
        ]
        if src_up:
            samples.append(_mk("if_link_up", "link", link_path, float(all(v >= 0.5 for v in src_up)), {"source": "aggregated"}))

    return samples


def _collect_synthetic_full(topology: Topology | None, labels: Dict[str, Any] | None = None) -> List[MetricSample]:
    if not topology:
        return []
    now = datetime.utcnow()
    base_labels = _normalize_labels(labels)
    samples: List[MetricSample] = []

    def _mk(metric: str, layer: MetricLayer, node: str, value: float, details: Dict | None = None, labels: Dict | None = None) -> MetricSample:
        return MetricSample(
            timestamp=now,
            node=node,
            layer=layer,
            metric=metric,
            value=value,
            details=details or {},
            # base_labels is already normalized once above; only re-normalize
            # when a call site passes a distinct per-sample override, instead
            # of rebuilding the same 7-key dict for every sample in the cycle.
            labels=_normalize_labels(labels) if labels else base_labels,
        )

    if layer_flags.get("physical", True):
        for link in topology.links:
            path = f"{link.source}->{link.target}"
            samples.append(_mk("if_errors", "physical", path, float(random.randint(0, 3))))
            samples.append(_mk("if_discards", "physical", path, float(random.randint(0, 3))))
            samples.append(_mk("if_crc_errors", "physical", path, float(random.randint(0, 2))))
            samples.append(_mk("if_in_util_pct", "physical", path, round(random.uniform(10, 80), 3)))
            samples.append(_mk("if_out_util_pct", "physical", path, round(random.uniform(10, 80), 3)))
            samples.append(_mk("if_temp_c", "physical", path, round(random.uniform(35, 65), 2)))
            samples.append(_mk("if_optic_rx_dbm", "physical", path, round(random.uniform(-6.0, -1.0), 2)))
            samples.append(_mk("if_optic_tx_dbm", "physical", path, round(random.uniform(-2.0, 2.0), 2)))

    if layer_flags.get("link", True):
        for link in topology.links:
            path = f"{link.source}->{link.target}"
            util = random.uniform(5, 90)
            jitter = random.uniform(0, 5)
            loss = random.uniform(0, 2)
            samples.append(_mk("link_util_pct", "link", path, round(util, 2), {"direction": "bidirectional"}))
            samples.append(_mk("link_loss_pct", "link", path, round(loss, 3)))
            samples.append(_mk("link_jitter_ms", "link", path, round(jitter, 3)))
            samples.append(_mk("queue_occupancy_pct", "link", link.source, round(random.uniform(0, 70), 2)))
            samples.append(_mk("queue_drops", "link", link.source, float(random.randint(0, 20))))
            samples.append(_mk("ecn_marked_pct", "link", path, round(random.uniform(0, 1), 3)))

    if layer_flags.get("network", True):
        for node in topology.nodes:
            if node.type in {"host", "switch"}:
                rtt = random.uniform(0.2, 8.0)
                loss = random.uniform(0, 3.0)
                jitter = random.uniform(0, 2.0)
                samples.append(_mk("latency_ms", "network", node.id, round(rtt, 3)))
                samples.append(_mk("packet_loss_pct", "network", node.id, round(loss, 3)))
                samples.append(_mk("jitter_ms", "network", node.id, round(jitter, 3)))
                samples.append(_mk("ttl_expired", "network", node.id, float(random.randint(0, 2))))
                samples.append(_mk("routes_count", "network", node.id, float(random.randint(1, 32))))
                samples.append(_mk("arp_entries", "network", node.id, float(random.randint(1, 64))))

    if layer_flags.get("service", True):
        hosts = [n for n in topology.nodes if n.type == "host"]
        if len(hosts) >= 2:
            src = hosts[0].id
            dst = hosts[1].id
            service = "ping"
            svc_labels = {**base_labels, "src": src, "dst": dst, "service": service}
            samples.append(_mk("e2e_path_availability_pct", "service", "probe", round(random.uniform(98.5, 100.0), 3), labels=svc_labels))
            samples.append(_mk("e2e_latency_ms_p50", "service", "probe", round(random.uniform(3.0, 30.0), 3), labels=svc_labels))
            samples.append(_mk("e2e_latency_ms_p95", "service", "probe", round(random.uniform(6.0, 60.0), 3), labels=svc_labels))
            samples.append(_mk("e2e_latency_ms_p99", "service", "probe", round(random.uniform(8.0, 90.0), 3), labels=svc_labels))
            samples.append(_mk("e2e_jitter_ms_p95", "service", "probe", round(random.uniform(0.1, 8.0), 3), labels=svc_labels))
            samples.append(_mk("e2e_loss_pct", "service", "probe", round(random.uniform(0.0, 2.0), 3), labels=svc_labels))

    if layer_flags.get("transport", True):
        for node in topology.nodes:
            if node.type == "host":
                samples.append(_mk("throughput_mbps", "transport", node.id, round(random.uniform(50, 900), 2)))
                samples.append(_mk("tcp_retrans_pct", "transport", node.id, round(random.uniform(0, 5), 3)))
                samples.append(_mk("tcp_rtt_ms", "transport", node.id, round(random.uniform(1, 30), 3)))
                samples.append(_mk("udp_jitter_ms", "transport", node.id, round(random.uniform(0, 5), 3)))
                samples.append(_mk("udp_loss_pct", "transport", node.id, round(random.uniform(0, 5), 3)))

                # ss -s synthetic snapshot
                ss_totals = {
                    "total": random.randint(2, 12),
                    "tcp_total": random.randint(1, 12),
                    "tcp_estab": random.randint(0, 4),
                    "tcp_closed": random.randint(8, 16),
                    "tcp_orphaned": random.randint(0, 1),
                    "tcp_timewait": random.randint(0, 4),
                }
                udp_ip = random.randint(0, 3)
                udp_ipv6 = random.randint(0, 3)
                tcp_ip = random.randint(0, 3)
                tcp_ipv6 = random.randint(0, 3)
                raw_ip = random.randint(0, 1)
                raw_ipv6 = random.randint(0, 1)
                ss_transport_matrix = {
                    "udp": {"total": udp_ip + udp_ipv6, "ip": udp_ip, "ipv6": udp_ipv6},
                    "tcp": {"total": tcp_ip + tcp_ipv6, "ip": tcp_ip, "ipv6": tcp_ipv6},
                    "raw": {"total": raw_ip + raw_ipv6, "ip": raw_ip, "ipv6": raw_ipv6},
                    "inet": {"total": (udp_ip + udp_ipv6 + tcp_ip + tcp_ipv6), "ip": udp_ip + tcp_ip, "ipv6": udp_ipv6 + tcp_ipv6},
                    "frag": {"total": random.randint(0, 1), "ip": random.randint(0, 1), "ipv6": 0},
                }

                samples.append(_mk("ss_sockets_total", "transport", node.id, float(ss_totals["total"]), {"source": "ss -s"}))
                samples.append(
                    _mk(
                        "ss_tcp_states",
                        "transport",
                        node.id,
                        float(ss_totals["tcp_total"]),
                        {
                            "estab": ss_totals["tcp_estab"],
                            "closed": ss_totals["tcp_closed"],
                            "orphaned": ss_totals["tcp_orphaned"],
                            "timewait": ss_totals["tcp_timewait"],
                            "source": "ss -s",
                        },
                    )
                )
                for proto, counts in ss_transport_matrix.items():
                    samples.append(
                        _mk(
                            f"ss_{proto}_sockets_total",
                            "transport",
                            node.id,
                            float(counts["total"]),
                            {"ip": counts["ip"], "ipv6": counts["ipv6"], "source": "ss -s"},
                        )
                    )

                # /proc/net/snmp synthetic snapshot
                ip_snmp = {
                    "in_receives": random.randint(10, 200),
                    "in_hdr_errors": random.randint(0, 2),
                    "in_addr_errors": random.randint(0, 2),
                    "forw_datagrams": random.randint(0, 3),
                    "in_unknown_protos": random.randint(0, 1),
                    "in_discards": random.randint(0, 2),
                    "in_delivers": random.randint(5, 25),
                    "out_requests": random.randint(5, 25),
                    "out_discards": random.randint(0, 2),
                    "out_no_routes": random.randint(0, 1),
                    "reasm_timeout": 0,
                    "reasm_reqds": random.randint(0, 1),
                    "reasm_oks": random.randint(0, 1),
                    "reasm_fails": random.randint(0, 1),
                    "frag_oks": random.randint(0, 1),
                    "frag_fails": random.randint(0, 1),
                    "frag_creates": random.randint(0, 1),
                    "out_transmits": random.randint(5, 25),
                }
                icmp_snmp = {
                    "in_msgs": random.randint(0, 10),
                    "out_msgs": random.randint(0, 10),
                    "in_errors": random.randint(0, 1),
                    "out_errors": random.randint(0, 1),
                    "in_dest_unreachs": random.randint(0, 1),
                    "out_dest_unreachs": random.randint(0, 1),
                    "in_time_excds": random.randint(0, 1),
                    "out_time_excds": random.randint(0, 1),
                    "in_echo_reqs": random.randint(0, 5),
                    "out_echo_reps": random.randint(0, 5),
                }
                tcp_snmp = {
                    "active_opens": random.randint(0, 3),
                    "passive_opens": random.randint(0, 3),
                    "attempt_fails": random.randint(0, 1),
                    "estab_resets": random.randint(0, 1),
                    "curr_estab": random.randint(0, 2),
                    "in_segs": random.randint(0, 30),
                    "out_segs": random.randint(0, 30),
                    "retrans_segs": random.randint(0, 5),
                    "in_errs": random.randint(0, 1),
                    "out_rsts": random.randint(0, 1),
                    "in_csum_errors": random.randint(0, 1),
                }
                udp_snmp = {
                    "in_datagrams": random.randint(0, 10),
                    "no_ports": random.randint(0, 2),
                    "in_errors": random.randint(0, 1),
                    "out_datagrams": random.randint(0, 10),
                    "rcvbuf_errors": random.randint(0, 1),
                    "sndbuf_errors": random.randint(0, 1),
                    "in_csum_errors": random.randint(0, 1),
                    "ignored_multi": random.randint(0, 1),
                    "mem_errors": random.randint(0, 1),
                }

                for key, val in ip_snmp.items():
                    samples.append(_mk(f"snmp_ip_{key}", "transport", node.id, float(val), {"source": "/proc/net/snmp"}))
                for key, val in icmp_snmp.items():
                    samples.append(_mk(f"snmp_icmp_{key}", "transport", node.id, float(val), {"source": "/proc/net/snmp"}))
                for key, val in tcp_snmp.items():
                    samples.append(_mk(f"snmp_tcp_{key}", "transport", node.id, float(val), {"source": "/proc/net/snmp"}))
                for key, val in udp_snmp.items():
                    samples.append(_mk(f"snmp_udp_{key}", "transport", node.id, float(val), {"source": "/proc/net/snmp"}))

    if layer_flags.get("session", True):
        sess_labels = {**base_labels, "sni": "example.local", "service": "web"}
        samples.append(_mk("tls_session_resumption_pct", "session", "edge", round(random.uniform(0, 100), 2), labels=sess_labels))
        samples.append(_mk("tls_renegotiations_total", "session", "edge", float(random.randint(0, 3)), labels=sess_labels))
        samples.append(_mk("tls_handshake_failures_total", "session", "edge", float(random.randint(0, 2)), labels={**base_labels, "sni": "example.local", "cert": "leaf"}))
        samples.append(_mk("vpn_sessions_active", "session", "gw", float(random.randint(0, 200)), labels={**base_labels, "profile": "default", "tenant": "lab"}))
        samples.append(_mk("aaa_auth_failures_total", "session", "aaa", float(random.randint(0, 5)), labels={**base_labels, "realm": "lab", "reason": "invalid_password"}))

    if layer_flags.get("presentation", True):
        pres_labels = {**base_labels, "sni": "example.local", "service": "web"}
        samples.append(_mk("tls_handshake_latency_ms_p95", "presentation", "edge", round(random.uniform(20, 200), 2), labels=pres_labels))
        samples.append(_mk("tls_handshake_latency_ms_p99", "presentation", "edge", round(random.uniform(40, 350), 2), labels=pres_labels))
        samples.append(_mk("tls_version_connections_total", "presentation", "edge", float(random.randint(10, 1000)), labels={**base_labels, "version": "TLS1.3", "sni": "example.local"}))
        samples.append(_mk("tls_cipher_connections_total", "presentation", "edge", float(random.randint(10, 1000)), labels={**base_labels, "cipher": "TLS_AES_128_GCM_SHA256", "sni": "example.local"}))
        samples.append(_mk("cert_expiry_days", "presentation", "edge", float(random.randint(1, 365)), labels={**base_labels, "cert": "leaf", "sni": "example.local"}))
        samples.append(_mk("http2_negotiation_success_pct", "presentation", "edge", round(random.uniform(80, 100), 2), labels={**base_labels, "service": "web"}))

    if layer_flags.get("application", True):
        for node in topology.nodes:
            if node.type == "host":
                samples.append(_mk("http_latency_ms", "application", node.id, round(random.uniform(20, 400), 2), {"status_code": 200}))
                samples.append(_mk("http_success_pct", "application", node.id, 99.0))
                samples.append(_mk("dns_latency_ms", "application", node.id, round(random.uniform(5, 80), 2)))
                samples.append(_mk("dns_success_pct", "application", node.id, 99.0))
                samples.append(_mk("tls_handshake_ms", "application", node.id, round(random.uniform(30, 150), 2)))
                samples.append(_mk("app_rps", "application", node.id, round(random.uniform(10, 2000), 2), labels={**base_labels, "service": "web", "route": "/api"}))
                samples.append(_mk("app_latency_ms_p95", "application", node.id, round(random.uniform(20, 600), 2), labels={**base_labels, "service": "web", "route": "/api"}))
                samples.append(_mk("app_latency_ms_p99", "application", node.id, round(random.uniform(30, 900), 2), labels={**base_labels, "service": "web", "route": "/api"}))
                samples.append(_mk("app_error_rate_pct", "application", node.id, round(random.uniform(0, 5), 3), labels={**base_labels, "service": "web", "route": "/api"}))

    if layer_flags.get("control", True):
        for node in topology.nodes:
            if node.type == "controller":
                samples.append(_mk("controller_latency_ms", "control", node.id, round(random.uniform(1, 25), 3)))
                samples.append(_mk("controller_conn_ok", "control", node.id, 1.0, {"ok": True}))
                samples.append(_mk("of_channel_reconnects", "control", node.id, float(random.randint(0, 1))))
                samples.append(_mk("flows_installed", "control", node.id, float(random.randint(10, 500))))
                samples.append(_mk("flows_removed", "control", node.id, float(random.randint(0, 100))))

    if layer_flags.get("dataplane", True):
        for flow in flows.values():
            topo = flow.topology_id
            samples.append(_mk("openflow_flow_packets", "dataplane", topo, float(random.randint(10, 10_000)), {"flow_id": flow.id or ""}))
            samples.append(_mk("openflow_flow_bytes", "dataplane", topo, float(random.randint(1_000, 5_000_000)), {"flow_id": flow.id or ""}))
        for link in topology.links:
            port_path = f"{link.source}->{link.target}"
            samples.append(_mk("table_hits", "dataplane", port_path, float(random.randint(1_000, 10_000))))
            samples.append(_mk("table_misses", "dataplane", port_path, float(random.randint(0, 500))))
            samples.append(_mk("port_rx_pkts", "dataplane", port_path, float(random.randint(1_000, 100_000))))
            samples.append(_mk("port_tx_pkts", "dataplane", port_path, float(random.randint(1_000, 100_000))))
            samples.append(_mk("port_rx_drops", "dataplane", port_path, float(random.randint(0, 500))))
            samples.append(_mk("port_tx_drops", "dataplane", port_path, float(random.randint(0, 500))))

    # Seed: guarantee at least one sample per MetricDefinition name.
    # This maximizes "Com dado" coverage for the dashboard taxonomy.
    try:
        batch_names = {s.metric for s in samples}
        missing_defs = [d for d in metric_definitions if d.name not in seen_metric_names and d.name not in batch_names]
        for d in missing_defs:
            layer_name = str(d.layer or "network")
            if not layer_flags.get(layer_name, True):
                continue

            unit = (d.unit or "").strip().lower()
            kind = (d.kind or "").strip().lower()

            if unit in {"%", "pct", "percent"} or "pct" in d.name:
                value = round(random.uniform(0, 100), 3)
            elif unit in {"ms", "millisecond", "milliseconds"} or d.name.endswith("_ms"):
                value = round(random.uniform(0.1, 500.0), 3)
            elif unit in {"s", "sec", "seconds"} or d.name.endswith("_s"):
                value = round(random.uniform(0.1, 60.0), 3)
            elif unit in {"bool", "boolean"}:
                value = float(random.randint(0, 1))
            elif unit in {"count", "count/s", "rps", "qps"} or kind == "counter":
                value = float(random.randint(0, 1000))
            elif unit in {"mbps", "gbps", "bps", "bits/s"}:
                value = round(random.uniform(1.0, 1000.0), 2)
            elif unit in {"bytes", "b", "kb", "mb", "gb"} or "bytes" in d.name:
                value = float(random.randint(0, 5_000_000))
            elif unit == "days":
                value = float(random.randint(0, 365))
            else:
                value = round(random.uniform(0, 100), 3)

            dim_labels = dict(base_labels)
            for dim in d.dimensions or []:
                if dim in dim_labels and dim_labels[dim] is not None:
                    continue
                if dim in {"src", "dst"}:
                    hosts = [n for n in topology.nodes if n.type == "host"]
                    if dim == "src" and hosts:
                        dim_labels[dim] = hosts[0].id
                    elif dim == "dst" and len(hosts) > 1:
                        dim_labels[dim] = hosts[1].id
                    else:
                        dim_labels[dim] = "unknown"
                elif dim in {"node", "switch", "host"}:
                    dim_labels[dim] = topology.nodes[0].id if topology.nodes else "unknown"
                elif dim == "service":
                    dim_labels[dim] = "web"
                elif dim == "route":
                    dim_labels[dim] = "/api"
                else:
                    dim_labels.setdefault(dim, "default")

            node_id = "probe"
            if layer_name in {"physical", "link", "dataplane"} and topology.links:
                node_id = f"{topology.links[0].source}->{topology.links[0].target}"
            elif topology.nodes:
                node_id = topology.nodes[0].id

            samples.append(_mk(d.name, layer_name, node_id, float(value), labels=dim_labels))
            seen_metric_names.add(d.name)
    except Exception:
        pass

    return samples


def _filter_samples(query: MetricQuery) -> List[MetricSample]:
    results: List[MetricSample] = []
    for sample in metric_samples:
        if query.metric_names and sample.metric not in query.metric_names:
            continue
        if query.nodes and sample.node not in query.nodes:
            continue
        if query.layers and sample.layer not in query.layers:
            continue
        if query.start_time and sample.timestamp < query.start_time:
            continue
        if query.end_time and sample.timestamp > query.end_time:
            continue
        results.append(sample)
        if len(results) >= query.limit:
            break
    return results


def _sample_to_dict(sample: MetricSample) -> Dict[str, Any]:
    return {
        "timestamp": sample.timestamp.isoformat(),
        "node": sample.node,
        "layer": sample.layer,
        "metric": sample.metric,
        "value": sample.value,
        "details": sample.details,
        "labels": sample.labels,
    }


# Rendered once per _store_samples() call and shared by every connected SSE
# client. Before this, each client's own generator recomputed the full
# snapshot (topology dump + every run's dump + jsonable_encoder over the
# whole tree) independently on every wake — with N tabs/dashboards open that
# was N redundant passes over the same data, every single cycle.
_sse_cached_frame: str | None = None
# Monotonically bumped every time the cache is rebuilt. Multiple concurrent
# SSE clients each track their own last-seen version instead of racing to
# clear a single shared Event — with a bare Event, whichever client's
# generator happened to call .clear() first could strand the others in
# .wait() until the *next* update, silently dropping a frame for them.
_sse_version = 0


def _refresh_sse_cache() -> None:
    global _sse_cached_frame, _sse_version
    t0 = time.perf_counter()
    topology = next(iter(topologies.values()), None)
    # O(|unique_keys|) instead of O(|all_samples|)
    def_names = {d.name for d in metric_definitions}
    latest_for_defs = [
        s for s in _samples_latest.values()
        if s.metric in def_names
    ]
    payload = {
        "type": "snapshot",
        "generated_at": datetime.utcnow().isoformat(),
        # mode="json" serializes datetimes/enums inline, so the payload is
        # already fully JSON-native — no need for a second jsonable_encoder
        # pass walking the whole (potentially large) tree again.
        "topology": topology.model_dump(mode="json") if topology else None,
        "metrics": [_sample_to_dict(s) for s in latest_for_defs],
        "runs": [run.model_dump(mode="json") for run in runs.values()],
    }
    _sse_cached_frame = f"data: {json.dumps(payload)}\n\n"
    _sse_version += 1
    _profile_record("sse_cache_refresh", (time.perf_counter() - t0) * 1000)


async def _event_stream():
    # Event-driven push: wake as soon as a collection cycle stores new
    # samples instead of polling on a fixed 2s tick. A bounded wait_for keeps
    # a heartbeat flowing (and the connection alive) even when nothing changed.
    #
    # Correctness note: each client tracks its own `last_seen` version
    # instead of relying purely on the shared Event's set/clear state, so
    # one client's timing can't steal or drop another client's wakeup the
    # way a bare clear-on-consume Event would with multiple readers. The
    # version check is re-done immediately after clear() (with no `await`
    # in between, so nothing else can run on this single-threaded loop in
    # that gap) to close the lost-wakeup window against a producer update
    # landing concurrently — including one delivered via
    # call_soon_threadsafe from the auto-collect worker thread.
    if _sse_cached_frame is None:
        _refresh_sse_cache()
    last_seen = -1
    while True:
        if _sse_version == last_seen:
            _samples_updated.clear()
            if _sse_version == last_seen:
                try:
                    await asyncio.wait_for(_samples_updated.wait(), timeout=15.0)
                except asyncio.TimeoutError:
                    pass
        last_seen = _sse_version
        yield _sse_cached_frame


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if not api_key:
        return
    if x_api_key != api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


# In-memory stores; replace with real database integrations later.
topologies: Dict[str, Topology] = {}
experiments: Dict[str, Experiment] = {}
runs: Dict[str, ExperimentRun] = {}
flows: Dict[str, FlowRule] = {}
run_metrics: Dict[str, List[MetricRecord]] = {}
# ── Metric storage ────────────────────────────────────────────────────────────
# _samples_latest: O(1) lookup of the most-recent sample for each
#   (metric, node, layer) triple.  Used by /metrics/latest and SSE.
# metric_samples: bounded ring-buffer for time-range queries (old API compat).
MAX_SAMPLES = int(os.getenv("MAX_METRIC_SAMPLES", "50000"))
_samples_latest: Dict[str, MetricSample] = {}
metric_samples: deque = deque(maxlen=MAX_SAMPLES)
# Signals SSE subscribers as soon as new samples land, replacing fixed polling.
_samples_updated: asyncio.Event = asyncio.Event()
_main_loop: asyncio.AbstractEventLoop | None = None
seen_metric_names: set[str] = set()
layer_flags: Dict[MetricLayer, bool] = metric_layer_flags({})
real_collection_default = os.getenv("REAL_COLLECTION", "0") == "1"
# Raised from 5s: with SSE now pushing instantly on every store (not on a
# fixed poll), end-to-end delivery latency no longer tracks this interval —
# it only trades data *freshness* for CPU, and CPU is the tight constraint.
# Override via env if a given experiment needs tighter freshness than CPU
# budget allows.
collection_interval_seconds = float(os.getenv("AUTO_COLLECT_INTERVAL", "15"))
_auto_collect_task: asyncio.Task | None = None

# System settings (in-memory, pode migrar para banco posteriormente)
system_settings: Dict[str, Any] = {
    "use_real_data": real_collection_default,
    "auto_collect_enabled": False,
    "collect_interval_sec": collection_interval_seconds,
}


def _extended_metric_definitions() -> List[MetricDefinition]:
    return catalog_metric_definitions() + netmon_metric_definitions()


# Defaults; may be overridden/augmented by platform_config.json.
metric_definitions: List[MetricDefinition] = _extended_metric_definitions()


def _bootstrap_from_config() -> None:
    global metric_definitions, layer_flags
    try:
        cfg = load_platform_config(config_path)
    except FileNotFoundError:
        return
    except ValueError as exc:
        raise RuntimeError(f"Invalid platform config: {exc}") from exc

    topology = topology_from_config(cfg)
    if topology and topology.id not in topologies:
        topologies[topology.id] = topology

    cfg_metrics = metric_definitions_from_config(cfg)
    if cfg_metrics:
        merged: Dict[str, MetricDefinition] = {m.name: m for m in _extended_metric_definitions()}
        merged.update({m.name: m for m in cfg_metrics})
        metric_definitions = list(merged.values())
    else:
        metric_definitions = _extended_metric_definitions()
    layer_flags = metric_layer_flags(cfg)

    raw_dir.mkdir(parents=True, exist_ok=True)
    for layer in layer_flags:
        _raw_file_for_layer(layer).touch(exist_ok=True)

    if topology:
        controller_ids = {c.get("id") for c in cfg.get("controllers", []) if c.get("id")}
        for flow in flows_from_config(cfg, topology.id, controller_ids):
            flow_id = flow.id or _generate_id("flow")
            flows[flow_id] = FlowRule(**flow.model_dump(exclude={"id"}), id=flow_id)


_bootstrap_from_config()

# Configs (multi-environment) support.
_seed_default_config_if_missing()
try:
    _apply_active_config()
except Exception:
    pass

# Restore researcher artifacts (experiments/runs) from temp folder.
_load_experiments_and_runs_from_temp()

for layer in layer_flags:
    _raw_file_for_layer(layer).touch(exist_ok=True)

# Seed minimal samples on startup so raw files are not empty for demo/testing scenarios.
if not metric_samples:
    restored = _load_recent_samples_from_raw(limit=200)
    if restored:
        metric_samples.extend(restored)
        for s in restored:
            try:
                seen_metric_names.add(str(s.metric))
                key = f"{s.metric}:{s.node}:{s.layer}"
                cur = _samples_latest.get(key)
                if cur is None or s.timestamp >= cur.timestamp:
                    _samples_latest[key] = s
            except Exception:
                pass

        # Fill gaps: restored raw tends to be transport-heavy, which leaves many
        # definitions without any sample. Do one synthetic pass to maximize coverage.
        topology = next(iter(topologies.values()), None)
        seeded = _collect_synthetic_full(topology, {"version": software_version}) if (SEED_SYNTHETIC or not real_collection_default) else []
        if seeded:
            _store_samples(seeded)
    else:
        seeded = _collect_synthetic_full(next(iter(topologies.values()), None), {"version": software_version}) if (SEED_SYNTHETIC or not real_collection_default) else []
        if seeded:
            _store_samples(seeded)


async def _auto_collect_loop() -> None:
    """Continuously collect metrics to keep SSE populated."""
    _next_deadline = time.monotonic()
    while True:
        try:
            topology = next(iter(topologies.values()), None)
            if topology:
                labels = {"version": software_version}
                use_real_setting = system_settings.get("use_real_data", real_collection_default)
                samples = await _collect_real(topology, labels) if use_real_setting else _collect_synthetic_full(topology, labels)
                if samples:
                    # _store_samples() does synchronous disk I/O (raw JSONL +
                    # per-run snapshot rewrite); offload it so a slow disk
                    # can't stall the event loop the way blocking subprocess
                    # calls used to.
                    await asyncio.to_thread(_store_samples, samples)
        except Exception:
            # Avoid crashing the loop; log could be added here later.
            pass
        if FIXED_RATE_LOOP:
            _next_deadline += collection_interval_seconds
            _delay = _next_deadline - time.monotonic()
            if _delay < 0:                      # ciclo estourou o periodo: sem acumular atraso
                _next_deadline = time.monotonic()
                _delay = 0.0
            await asyncio.sleep(_delay)
        else:
            await asyncio.sleep(collection_interval_seconds)


@app.on_event("startup")
async def _start_auto_collect() -> None:
    global _auto_collect_task, _main_loop
    _main_loop = asyncio.get_running_loop()
    if _auto_collect_task is None:
        _auto_collect_task = asyncio.create_task(_auto_collect_loop())


@app.on_event("shutdown")
async def _stop_auto_collect() -> None:
    global _auto_collect_task
    if _auto_collect_task:
        _auto_collect_task.cancel()
        try:
            await _auto_collect_task
        except asyncio.CancelledError:
            pass
    for handle in _raw_file_handles.values():
        try:
            handle.close()
        except Exception:
            pass
    _raw_file_handles.clear()


@app.post("/topologies", response_model=Topology, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_api_key)])
async def create_topology(payload: TopologyCreate) -> Topology:
    topology_id = _generate_id("topo")
    topology = Topology(id=topology_id, **payload.model_dump())
    topologies[topology_id] = topology
    return topology


@app.get("/topologies", response_model=List[Topology])
async def list_topologies() -> List[Topology]:
    return list(topologies.values())


@app.get("/topologies/{topology_id}", response_model=Topology)
async def get_topology(topology_id: str) -> Topology:
    topology = topologies.get(topology_id)
    if not topology:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topology not found")
    return topology


@app.put("/topologies/{topology_id}", response_model=Topology, dependencies=[Depends(require_api_key)])
async def update_topology(topology_id: str, payload: TopologyUpdate) -> Topology:
    existing = topologies.get(topology_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topology not found")
    update_data = {k: v for k, v in payload.model_dump(exclude_unset=True).items() if v is not None}
    merged = existing.model_dump()
    merged.update(update_data)
    updated = Topology(id=topology_id, **merged)
    topologies[topology_id] = updated
    return updated


@app.delete("/topologies/{topology_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_api_key)])
async def delete_topology(topology_id: str) -> Response:
    if topology_id not in topologies:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topology not found")
    topologies.pop(topology_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/experiments", response_model=Experiment, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_api_key)])
async def create_experiment(payload: ExperimentCreate) -> Experiment:
    if payload.topology_id not in topologies:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topology not found")
    experiment_id = _generate_id("exp")
    experiment = Experiment(id=experiment_id, **payload.model_dump())
    experiments[experiment_id] = experiment
    try:
        _persist_experiment(experiment)
    except Exception:
        pass
    return experiment


@app.get("/experiments", response_model=List[Experiment])
async def list_experiments() -> List[Experiment]:
    return list(experiments.values())


@app.get("/experiments/{experiment_id}", response_model=Experiment)
async def get_experiment(experiment_id: str) -> Experiment:
    experiment = experiments.get(experiment_id)
    if not experiment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")
    return experiment


@app.put("/experiments/{experiment_id}", response_model=Experiment, dependencies=[Depends(require_api_key)])
async def update_experiment(experiment_id: str, payload: ExperimentUpdate) -> Experiment:
    existing = experiments.get(experiment_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")
    update_data = {k: v for k, v in payload.model_dump(exclude_unset=True).items() if v is not None}
    merged = existing.model_dump()
    merged.update(update_data)
    updated = Experiment(id=experiment_id, **merged)
    experiments[experiment_id] = updated
    try:
        _persist_experiment(updated)
    except Exception:
        pass
    return updated


@app.delete("/experiments/{experiment_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_api_key)])
async def delete_experiment(experiment_id: str) -> Response:
    if experiment_id not in experiments:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")
    experiments.pop(experiment_id)
    try:
        _delete_persisted_experiment(experiment_id)
    except Exception:
        pass

    # Clean up any runs linked to this experiment.
    for run_id, run in list(runs.items()):
        if run.experiment_id != experiment_id:
            continue
        runs.pop(run_id, None)
        run_metrics.pop(run_id, None)
        try:
            _delete_persisted_run(run_id)
        except Exception:
            pass
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/experiments/{experiment_id}/export")
async def export_experiment(experiment_id: str) -> Response:
    """Return a self-contained JSON bundle (experiment + runs + metrics) for download."""
    experiment = experiments.get(experiment_id)
    if not experiment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")

    exp_runs = [r for r in runs.values() if r.experiment_id == experiment_id]
    bundle = {
        "version": 1,
        "exported_at": datetime.utcnow().isoformat(),
        "experiment": experiment.model_dump(mode="json"),
        "runs": [r.model_dump(mode="json") for r in exp_runs],
        "run_metrics": {
            r.id: [m.model_dump(mode="json") for m in run_metrics.get(r.id, [])]
            for r in exp_runs
        },
    }
    filename = re.sub(r"[^a-zA-Z0-9_-]+", "_", experiment.name or experiment_id).strip("_") or experiment_id
    return Response(
        content=json.dumps(bundle, indent=2, ensure_ascii=False),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}.json"'},
    )


@app.post(
    "/experiments/import",
    response_model=Dict[str, Any],
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
)
async def import_experiment(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Import a bundle produced by /experiments/{id}/export.

    A new experiment ID is always generated to avoid collisions.
    Existing topology IDs are reused when they already exist; otherwise a
    placeholder topology is created automatically.
    """
    try:
        exp_data: Dict[str, Any] = payload.get("experiment") or {}
        runs_data: List[Dict[str, Any]] = payload.get("runs") or []
        metrics_data: Dict[str, Any] = payload.get("run_metrics") or {}
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid bundle format")

    # Resolve topology
    orig_topo_id = exp_data.get("topology_id", "")
    if orig_topo_id not in topologies:
        placeholder = Topology(
            id=orig_topo_id or _generate_id("topo"),
            name=f"Imported ({orig_topo_id})",
            nodes=[],
            links=[],
        )
        topologies[placeholder.id] = placeholder
        orig_topo_id = placeholder.id

    # Create experiment with a fresh ID
    new_exp_id = _generate_id("exp")
    id_map: Dict[str, str] = {exp_data.get("id", ""): new_exp_id}
    try:
        exp = Experiment(
            id=new_exp_id,
            name=exp_data.get("name", "Imported"),
            topology_id=orig_topo_id,
            description=exp_data.get("description"),
            parameters=exp_data.get("parameters"),
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid experiment data: {exc}")

    experiments[new_exp_id] = exp
    _persist_experiment(exp)

    imported_runs: List[str] = []
    for r_data in runs_data:
        new_run_id = _generate_id("run")
        id_map[r_data.get("id", "")] = new_run_id
        try:
            run = ExperimentRun(
                id=new_run_id,
                experiment_id=new_exp_id,
                topology_id=orig_topo_id,
                status=r_data.get("status", "COMPLETED"),
                started_at=r_data.get("started_at"),
                ended_at=r_data.get("ended_at"),
                parameters=r_data.get("parameters"),
                logs=r_data.get("logs") or [],
            )
        except Exception:
            continue
        runs[new_run_id] = run
        _persist_run(run)

        # Restore metrics
        orig_run_id = r_data.get("id", "")
        raw_metrics: List[Dict[str, Any]] = metrics_data.get(orig_run_id) or []
        restored: List[MetricRecord] = []
        for m in raw_metrics:
            try:
                restored.append(MetricRecord(**m))
            except Exception:
                continue
        run_metrics[new_run_id] = restored
        _persist_run_metrics(new_run_id, force=True)
        imported_runs.append(new_run_id)

    return {
        "imported": True,
        "experiment_id": new_exp_id,
        "runs_imported": len(imported_runs),
        "id_map": id_map,
    }


@app.post(
    "/experiments/{experiment_id}/run",
    response_model=ExperimentRun,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
)
async def start_run(experiment_id: str, mode: str = "synthetic") -> ExperimentRun:
    experiment = experiments.get(experiment_id)
    if not experiment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")
    run_id = _generate_id("run")
    run = ExperimentRun(
        id=run_id,
        experiment_id=experiment_id,
        topology_id=experiment.topology_id,
        status="RUNNING",
        started_at=datetime.utcnow(),
        parameters={"mode": mode},
        logs=[],
    )
    runs[run_id] = run
    run_metrics.setdefault(run_id, [])

    try:
        _persist_run(run)
    except Exception:
        pass

    # Snapshot collection (scientific record): collect once and finalize the run.
    try:
        labels = {
            "run_id": run_id,
            "experiment_id": experiment_id,
            "topology_id": experiment.topology_id,
            "version": software_version,
        }
        topology = topologies.get(experiment.topology_id)
        # Usar system_settings para determinar modo de coleta
        use_real_setting = system_settings.get("use_real_data", real_collection_default)
        use_real = mode == "real" or (use_real_setting and mode != "synthetic")
        samples = await _collect_real(topology, labels) if use_real else _collect_synthetic_full(topology, labels)
        _store_samples(samples)
        _append_run_log(run_id, f"metrics_collected:{len(samples)}")

        merged = run.model_dump()
        merged.update({"status": "COMPLETED", "ended_at": datetime.utcnow()})
        run = ExperimentRun(**merged)
        runs[run_id] = run
    except Exception as exc:
        _append_run_log(run_id, f"failed:{type(exc).__name__}")
        merged = run.model_dump()
        merged.update({"status": "FAILED", "ended_at": datetime.utcnow()})
        run = ExperimentRun(**merged)
        runs[run_id] = run

    try:
        _persist_run(run)
        _persist_run_metrics(run_id, force=True)
    except Exception:
        pass
    return run


@app.get("/experiments/{experiment_id}/runs", response_model=List[ExperimentRun])
async def list_runs_for_experiment(experiment_id: str) -> List[ExperimentRun]:
    return [run for run in runs.values() if run.experiment_id == experiment_id]


@app.get("/runs/{run_id}", response_model=ExperimentRun)
async def get_run(run_id: str) -> ExperimentRun:
    run = runs.get(run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run


@app.get("/runs/{run_id}/metrics", response_model=List[MetricRecord])
async def get_run_metrics(run_id: str) -> List[MetricRecord]:
    if run_id not in runs:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run_metrics.get(run_id, [])


@app.get("/system/settings")
async def get_system_settings() -> Dict[str, Any]:
    """Get current system settings (data mode, auto-collect, etc)."""
    return {
        "use_real_data": system_settings["use_real_data"],
        "auto_collect_enabled": system_settings["auto_collect_enabled"],
        "collect_interval_sec": system_settings["collect_interval_sec"],
        "real_collection_available": True,  # Indica se coleta real está disponível
    }


@app.patch("/system/settings")
async def update_system_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Update system settings. Accepts: use_real_data, auto_collect_enabled, collect_interval_sec."""
    if "use_real_data" in settings and isinstance(settings["use_real_data"], bool):
        prev_mode = system_settings.get("use_real_data", real_collection_default)
        system_settings["use_real_data"] = settings["use_real_data"]
        logger.info(f"Modo de coleta alterado: {'REAL' if settings['use_real_data'] else 'SINTÉTICO'}")
        # When mode changes, clear stale samples so the new mode's data
        # is not polluted by the previous mode's samples.
        if prev_mode != settings["use_real_data"]:
            metric_samples.clear()
            _samples_latest.clear()
            logger.info("metric_samples e _samples_latest limpos após troca de modo.")
            # Trigger an immediate collection cycle in the new mode
            topology = next(iter(topologies.values()), None)
            new_mode_real = settings["use_real_data"]
            try:
                new_samples = await _collect_real(topology, {"version": software_version}) if new_mode_real else _collect_synthetic_full(topology, {"version": software_version})
                if new_samples:
                    _store_samples(new_samples)
            except Exception as exc:
                logger.warning(f"Coleta imediata pós-troca de modo falhou: {exc}")
    
    if "auto_collect_enabled" in settings and isinstance(settings["auto_collect_enabled"], bool):
        system_settings["auto_collect_enabled"] = settings["auto_collect_enabled"]
        logger.info(f"Auto-coleta: {'ATIVADA' if settings['auto_collect_enabled'] else 'DESATIVADA'}")
    
    if "collect_interval_sec" in settings and isinstance(settings["collect_interval_sec"], (int, float)):
        if settings["collect_interval_sec"] > 0:
            system_settings["collect_interval_sec"] = float(settings["collect_interval_sec"])
            logger.info(f"Intervalo de coleta: {settings['collect_interval_sec']}s")
    
    return await get_system_settings()


@app.get("/metrics/definitions", response_model=List[MetricDefinition])
async def list_metric_definitions() -> List[MetricDefinition]:
    return metric_definitions


@app.post("/metrics/query", response_model=List[MetricSample])
async def query_metrics(query: MetricQuery) -> List[MetricSample]:
    return _filter_samples(query)


@app.post("/flows", response_model=FlowRule, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_api_key)])
async def create_flow(flow: FlowRule) -> FlowRule:
    if flow.topology_id not in topologies:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topology not found")
    flow_id = flow.id or _generate_id("flow")
    stored = FlowRule(**flow.model_dump(exclude={"id"}), id=flow_id)
    flows[flow_id] = stored
    return stored


@app.get("/flows", response_model=List[FlowRule])
async def list_flows(topology_id: str | None = None) -> List[FlowRule]:
    if topology_id:
        return [flow for flow in flows.values() if flow.topology_id == topology_id]
    return list(flows.values())


@app.delete("/flows/{flow_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_api_key)])
async def delete_flow(flow_id: str) -> Response:
    if flow_id not in flows:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Flow not found")
    flows.pop(flow_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/stream/events")
async def stream_events() -> StreamingResponse:
    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Helpful if served behind reverse proxies that buffer responses.
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/configs")
async def list_configs() -> List[Dict[str, Any]]:
    return _list_configs()


@app.get("/configs/active")
async def get_active_config() -> Dict[str, Any]:
    active_id = _load_active_config_id()
    return {"active": active_id}


@app.get("/configs/{config_id}")
async def get_config(config_id: str) -> Dict[str, Any]:
    return _read_config_payload(config_id)


@app.post("/configs", dependencies=[Depends(require_api_key)])
async def save_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    config_id = str(payload.get("id") or payload.get("name") or _generate_id("cfg"))
    config_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", config_id.strip()).strip("_")
    if not config_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid config id")

    set_active = bool(payload.get("set_active", False))
    _write_config_payload(config_id, payload)
    if set_active:
        _set_active_config_id(config_id)
        _apply_active_config()
    return {"saved": True, "id": config_id, "active": _load_active_config_id()}


@app.post("/configs/{config_id}/activate", dependencies=[Depends(require_api_key)])
async def activate_config(config_id: str) -> Dict[str, Any]:
    _read_config_payload(config_id)
    _set_active_config_id(config_id)
    _apply_active_config()
    return {"active": config_id}


@app.delete("/configs/{config_id}", dependencies=[Depends(require_api_key)])
async def delete_config(config_id: str) -> Dict[str, Any]:
    path = _config_path_for_id(config_id)
    if not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Config not found")
    path.unlink(missing_ok=True)
    if _load_active_config_id() == config_id:
        # Pick next available config.
        remaining = _list_configs()
        if remaining:
            _set_active_config_id(str(remaining[0]["id"]))
        else:
            active_config_path.unlink(missing_ok=True)
    return {"deleted": True}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


# ── Experiment draft (rascunho) persistence ───────────────────────────────────
EXPERIMENT_DRAFT_FILE = temp_root / "experiment_draft.json"
EXPERIMENT_RUN_STATE_FILE = temp_root / "experiment_run_state.json"
EXPERIMENT_SELECTED_METRICS_FILE = temp_root / "experiment_selected_metrics.json"
EXPERIMENT_HISTORY_FILE = temp_root / "experiment_history.json"


@app.get("/experiment/draft")
async def get_experiment_draft() -> Dict[str, Any]:
    """Carregar rascunho do experimento (para persistência de UI)"""
    if EXPERIMENT_DRAFT_FILE.exists():
        try:
            with open(EXPERIMENT_DRAFT_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


@app.post("/experiment/draft")
async def save_experiment_draft(draft: Dict[str, Any]) -> Dict[str, str]:
    """Salvar rascunho do experimento (para persistência de UI)"""
    try:
        temp_root.mkdir(parents=True, exist_ok=True)
        with open(EXPERIMENT_DRAFT_FILE, "w") as f:
            json.dump(draft, f, indent=2)
        return {"status": "saved"}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@app.get("/experiment/run-state")
async def get_experiment_run_state() -> Dict[str, Any]:
    """Carregar estado de execução do experimento"""
    if EXPERIMENT_RUN_STATE_FILE.exists():
        try:
            with open(EXPERIMENT_RUN_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


@app.post("/experiment/run-state")
async def save_experiment_run_state(state: Dict[str, Any]) -> Dict[str, str]:
    """Salvar estado de execução do experimento"""
    try:
        temp_root.mkdir(parents=True, exist_ok=True)
        with open(EXPERIMENT_RUN_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        return {"status": "saved"}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@app.get("/experiment/selected-metrics")
async def get_selected_metrics() -> Dict[str, Any]:
    """Carregar métricas selecionadas"""
    if EXPERIMENT_SELECTED_METRICS_FILE.exists():
        try:
            with open(EXPERIMENT_SELECTED_METRICS_FILE, "r") as f:
                data = json.load(f)
                return {"metricKeys": data}
        except Exception:
            pass
    return {"metricKeys": []}


@app.post("/experiment/selected-metrics")
async def save_selected_metrics(data: Dict[str, Any]) -> Dict[str, str]:
    """Salvar métricas selecionadas"""
    try:
        temp_root.mkdir(parents=True, exist_ok=True)
        metric_keys = data.get("metricKeys", [])
        with open(EXPERIMENT_SELECTED_METRICS_FILE, "w") as f:
            json.dump(metric_keys, f, indent=2)
        return {"status": "saved"}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@app.get("/experiment/history")
async def get_experiment_history() -> List[Dict[str, Any]]:
    """Carregar histórico de experimentos do disco"""
    if EXPERIMENT_HISTORY_FILE.exists():
        try:
            with open(EXPERIMENT_HISTORY_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
    return []


@app.post("/experiment/history")
async def save_experiment_history(records: List[Dict[str, Any]]) -> Dict[str, str]:
    """Salvar histórico de experimentos no disco"""
    try:
        temp_root.mkdir(parents=True, exist_ok=True)
        with open(EXPERIMENT_HISTORY_FILE, "w") as f:
            json.dump(records, f, indent=2)
        return {"status": "saved", "count": len(records)}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@app.post("/experiment/history/append")
async def append_experiment_record(record: Dict[str, Any]) -> Dict[str, str]:
    """Adicionar um registro ao histórico de experimentos"""
    try:
        temp_root.mkdir(parents=True, exist_ok=True)
        existing: list = []
        if EXPERIMENT_HISTORY_FILE.exists():
            try:
                with open(EXPERIMENT_HISTORY_FILE, "r") as f:
                    existing = json.load(f)
                    if not isinstance(existing, list):
                        existing = []
            except Exception:
                existing = []
        # Adicionar no início (mais recente primeiro)
        existing.insert(0, record)
        # Manter apenas os últimos 200 registros
        existing = existing[:200]
        with open(EXPERIMENT_HISTORY_FILE, "w") as f:
            json.dump(existing, f, indent=2)
        return {"status": "saved", "count": len(existing)}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@app.get("/netmon/blocks")
async def list_netmon_blocks() -> List[Dict[str, Any]]:
    try:
        return list_grc_blocks()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.post("/netmon/ingest", response_model=Dict[str, int])
async def ingest_netmon_metrics(
    payload: Dict[str, Any] | List[Dict[str, Any]],
    run_id: str | None = None,
    experiment_id: str | None = None,
    topology_id: str | None = None,
    version: str | None = None,
) -> Dict[str, int]:
    items = payload if isinstance(payload, list) else [payload]
    labels = {"run_id": run_id, "experiment_id": experiment_id, "topology_id": topology_id, "version": version}
    stored = 0
    for item in items:
        samples = samples_from_netmon(item, labels=labels)
        if samples:
            _store_samples(samples)
            stored += len(samples)
    return {"stored": stored}


@app.post("/netmon/ping", response_model=Dict[str, Any])
async def execute_netmon_ping(
    container_name: str = "h1",
    target_ip: str = "172.18.0.3",
    timeout: float = 2.0,
    run_id: str | None = None,
    experiment_id: str | None = None,
    topology_id: str | None = None,
    version: str | None = None,
    store: bool = True,
) -> Dict[str, Any]:
    try:
        payload = run_host_ping(container_name, target_ip, timeout)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    payload["ts"] = time.time()
    payload["ts_iso"] = datetime.utcnow().isoformat()
    if store:
        labels = {"run_id": run_id, "experiment_id": experiment_id, "topology_id": topology_id, "version": version}
        samples = samples_from_netmon(payload, labels=labels)
        if samples:
            _store_samples(samples)
    return payload


@app.post("/metrics/samples", response_model=Dict[str, int], status_code=status.HTTP_201_CREATED)
async def ingest_samples(payload: MetricSample | List[MetricSample]) -> Dict[str, int]:
    samples: List[MetricSample]
    if isinstance(payload, list):
        samples = payload
    else:
        samples = [payload]
    _store_samples(samples)
    return {"stored": len(samples)}


@app.get("/metrics/latest", response_model=List[MetricSample])
async def latest_metrics(metric: str | None = None, node: str | None = None, layer: MetricLayer | None = None) -> List[MetricSample]:
    # O(|unique metric:node:layer keys|) ≈ constant; previously O(|all stored samples|)
    if not metric and not node and not layer:
        return list(_samples_latest.values())
    return [
        s for s in _samples_latest.values()
        if (not metric or s.metric == metric)
        and (not node or s.node == node)
        and (not layer or s.layer == layer)
    ]


@app.get("/metrics/export")
async def export_metrics(layer: MetricLayer | None = None) -> Response:
    path = _raw_file_for_layer(layer or "network") if layer else None
    if path and path.exists():
        content = path.read_text(encoding="utf-8")
        return Response(content=content, media_type="text/plain")
    # Fallback to in-memory export
    lines = [_serialize_sample(s) for s in metric_samples if not layer or s.layer == layer]
    return Response(content="\n".join(lines), media_type="text/plain")


@app.post("/metrics/collect", response_model=Dict[str, Any], dependencies=[Depends(require_api_key)])
async def collect_metrics(
    mode: str = "synthetic",
    run_id: str | None = None,
    experiment_id: str | None = None,
    topology_id: str | None = None,
    version: str | None = None,
) -> Dict[str, Any]:
    labels = {k: v for k, v in {"run_id": run_id, "experiment_id": experiment_id, "topology_id": topology_id, "version": version}.items() if v is not None}
    topology = topologies.get(topology_id) if topology_id else next(iter(topologies.values()), None)
    use_real_setting = system_settings.get("use_real_data", real_collection_default)
    use_real = mode == "real" or (use_real_setting and mode != "synthetic")
    samples = await _collect_real(topology, labels) if use_real else _collect_synthetic_full(topology, labels)
    _store_samples(samples)
    if run_id:
        _append_run_log(run_id, "metrics_collected")
    return {"collected": len(samples), "mode": "real" if use_real else "synthetic"}


@app.get("/debug/collect_profile", response_model=Dict[str, Any], dependencies=[Depends(require_api_key)])
async def debug_collect_profile() -> Dict[str, Any]:
    """Runs one real _collect_real() cycle and returns a wall-time breakdown
    by phase (see _profile_record above) rather than cProfile. cProfile only
    instruments the thread it's enabled on, and almost all the actual work
    here (_run_cmd, _docker_exec_sync, docker stats) runs inside
    asyncio.to_thread() worker threads it would never see.

    The stats dict lives in a ContextVar scoped to this request's own Task,
    not a bare global — so a background _auto_collect_loop cycle firing
    concurrently (a real race in an earlier version of this endpoint) writes
    into ITS OWN (default/unset) context and simply can't contaminate this
    call's numbers, no matter how the two interleave.

    Point this at the real lab host (with a Docker daemon) to see whether
    time is still going into `docker_cli_fallback` (check
    `docker_sdk_client_available` / `docker_sdk_client_error` below for why)
    versus `docker_sdk_exec`, and how that compares to
    `sse_cache_refresh`/`raw_file_write` (Python-side cost) and the total
    cycle wall time.

    Side effect: like /metrics/collect, this actually stores the collected
    samples (labeled version="profile") and notifies real SSE subscribers —
    it profiles the genuine end-to-end path, not a sandboxed copy of it.
    """
    stats: Dict[str, Dict[str, float]] = {}
    token = _profile_ctx.set(stats)
    t0 = time.perf_counter()
    try:
        topology = next(iter(topologies.values()), None)
        use_real_setting = system_settings.get("use_real_data", real_collection_default)
        samples = (
            await _collect_real(topology, {"version": "profile"})
            if use_real_setting
            else _collect_synthetic_full(topology, {"version": "profile"})
        )
        _store_samples(samples)
    finally:
        _profile_ctx.reset(token)
    cycle_ms = (time.perf_counter() - t0) * 1000

    breakdown = {
        name: {"calls": int(v["count"]), "total_ms": round(v["total_ms"], 2)}
        for name, v in sorted(stats.items(), key=lambda kv: -kv[1]["total_ms"])
    }
    docker_client_ok = _get_docker_client() is not None
    return {
        "mode": "real" if use_real_setting else "synthetic",
        "samples_collected": len(samples),
        "cycle_wall_ms": round(cycle_ms, 2),
        "docker_sdk_client_available": docker_client_ok,
        "docker_sdk_client_error": None if docker_client_ok else _docker_client_error,
        "by_phase": breakdown,
    }
