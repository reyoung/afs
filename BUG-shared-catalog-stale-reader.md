# Bug: Shared Catalog Layer Reader Closed Prematurely

## Symptom
When running AFS tasks that share layers (e.g., py_commons layer), if a prior
session fails mid-mount (e.g., during reconcile with context canceled), the
shared catalog's layer reader for that digest gets closed. Subsequent sessions
that reuse the same shared mount hit:

```
layerfuse.shared_elf_read: path=swe-tools/uv/bin/uv ... err=layer reader is closed for digest=sha256:15a3af643a...
```

The container sees `Input/output error` when trying to exec any binary from the
affected layer.

## Root Cause
The shared catalog caches mounted layers across sessions. When a session's
mount runner exits (e.g., context canceled during reconcile), it closes the
DiscoveryBackedReaderAt for its layers. But if that reader is shared via the
catalog, other sessions referencing the same image/layer lose access.

## Reproduction
1. Start an AFS task for image A (which includes py_commons layer)
2. Let it fail mid-mount (e.g., missing layer triggers reconcile, context canceled)
3. Start another AFS task for image B that shares the py_commons layer
4. Image B mount succeeds (TOC cached) but file reads fail with I/O error

## Workaround
Restart the afslet pod to clear the shared catalog state:
```
kubectl -n afs rollout restart deployment/afs-afslet
```

## Fix
The shared catalog should either:
1. Reference-count layer readers and only close when all sessions release, or
2. Detect a closed reader and re-create it on demand (reconnect to a provider)

## Files
- `pkg/afsmount/runner.go` — shared catalog mount logic
- `pkg/layerreader/discovery_reader.go` — DiscoveryBackedReaderAt.Close()
