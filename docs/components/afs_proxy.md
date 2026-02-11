# Component: afs_proxy

## Purpose

Routes `Afslet.Execute` requests to one of multiple afslet backends with resource-aware scheduling and retries.

## Provides

- gRPC service:
  - `afsletpb.Afslet.Execute` (proxy implementation)
  - `afsproxypb.AfsProxy.Status` (streaming cluster status)
- HTTP endpoints:
  - `/status`
  - `/dispatching` (backward-compatible alias)

## Talks To

- Resolves afslet backend addresses from DNS (`A` records, per-attempt re-resolve).
- Calls each afslet `GetRuntimeStatus` for capacity checks.
- Optionally queries discovery for layerstore status in proxy `Status` stream.
- Optionally queries peer proxies for cluster dispatching queue count.

## Scheduling Model

1. Filter afslet instances that can satisfy requested `cpu_cores` and `memory_mb`.
2. If none can ever satisfy request, return invalid argument.
3. If temporarily insufficient capacity, enter retry loop.
4. Select backend using power-of-two-choices among feasible candidates.
5. Retry dispatch with linear backoff + jitter until success or retry budget exhausted.

## Status APIs

- gRPC `Status` streams:
  - layerstore instances (node, endpoint, layer stats, cache max bytes),
  - afslet instances (reachable + runtime usage),
  - final summary.
- HTTP `/status` returns dispatching queue counters.
  - `include_cluster` defaults to `true`.

## Important Flags

- `-listen`
- `-http-listen`
- `-afslet-target`
- `-proxy-peers-target`
- `-discovery-target`
- `-dispatch-backoff`
- `-status-timeout`
- `-peer-http-timeout`
