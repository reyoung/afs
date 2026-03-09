# AFS Perf Tuning Plan

## 1. Goal

- Target workload: `aplusb_2023` via AFS Python asyncio SDK.
- Target latency:
  - `two-stage` (compile + verify): `< 3.0s` total.
  - `single-stage` (compile+verify in one Execute): `p50 <= 2.8s`, `p95 <= 3.2s`.

## 2. Current Baseline (2026-03-09)

- Two-stage median:
  - `compile_total`: `2.760s`
  - `compile_runc_non_g++`: `0.973s`
  - `verify_total`: `2.187s`
  - `total`: `4.949s`
- Single-stage median:
  - `total`: `2.945s`
  - `runc_total`: `2.490s`
  - `runc_non_inner`: `0.999s`
- No-op A/B on runc namespace pruning (`no-pid/ipc/uts`):
  - OFF median `afs_runc_total`: `1.055s`
  - ON median `afs_runc_total`: `1.142s`
  - Delta: `+0.087s` (`+8.2%`) -> no gain yet in this cluster run.

## 3. Optimization Plan

1. Reproduce and stabilize benchmark methodology
- Pin benchmark to fixed backend node (`node_id`) to reduce cross-node jitter.
- Run at least `20` iterations with warmup and report `p50/p90/p95`.
- Record both timeline-based and `__AFS_RUNC_TIMING__` metrics.

2. Recover runc overhead regression
- Keep `no-pid/ipc/uts` as optional flags, not assumed as default win.
- Profile `afs_runc` stages (`prepare_bundle`, `runc_wait`, `runc_delete`) under ON/OFF.
- Validate impact on different kernels/runc versions before enabling by default.

3. Reduce fixed overhead around runc
- Remove duplicate unmount warnings and avoid redundant cleanup syscalls.
- Reuse session temp dirs where safe to cut mkdir/remove churn.
- Audit log volume on hot path and reduce sync/stderr pressure for benchmark mode.

4. Shift default execution path to single-stage
- Add first-class single-stage flow in runner/proxy client path.
- Keep two-stage fallback behind a flag for debugging/compat.
- Validate correctness on `aplusb_2023` and 3-5 additional problems.

5. Optimize mount and image side latency
- Reuse resolved provider and connection pools aggressively per afslet pod.
- Evaluate partial layer prefetch strategy for known hot images.
- Track mount-ready distribution (`p50/p95`) separately from runc.

6. Define acceptance gate
- CI perf check (nightly) for `aplusb_2023`:
  - single-stage `p50 <= 2.8s`
  - single-stage `p95 <= 3.2s`
  - no-op `afs_runc_total` must not regress by >5%.
