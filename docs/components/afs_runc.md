# Component: afs_runc

## Purpose

Runs a command on a prepared rootfs via `runc` with CPU/memory/time limits.

## Inputs

- Rootfs path.
- Command argv.
- Resource limits:
  - CPU cores (`-cpu`)
  - memory MB (`-memory-mb`)
  - timeout (`-timeout`)

## Behavior

- Creates temporary OCI bundle with generated `config.json`.
- Uses cgroup constraints for CPU/memory.
- Kills container when timeout is reached.
- Deletes container state after run.

## Network and DNS Model

- Default uses host network (no isolated network namespace).
- Binds host `/etc/resolv.conf` into container read-only for DNS resolution.

## Mounts in OCI Spec

- `/proc`
- `/dev`, `/dev/pts`, `/dev/shm`, `/dev/mqueue`
- `/sys` (read-only)
- `/etc/resolv.conf` (bind ro)

## Important Flags

- `-rootfs`
- `-cpu`
- `-memory-mb`
- `-timeout`
- `-runc-binary`
- `-container-id`
- `-keep-bundle`
