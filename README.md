# AFS (Agent File System)

AFS stands for **Agent File System**.

It is a filesystem-oriented solution for RL workloads (for example, SWE-Agent) to quickly deploy and reuse container image file data.

## Background

In RL clusters, task volume is high, image diversity is high, and pulls are frequent.
If every node repeatedly pulls the same image from the same registry, it causes:

- Excessive registry load
- Bandwidth waste
- Higher startup latency

The goal of AFS is to expand image data reuse from “single node” to “entire cluster”.

## Core Idea

Unlike Docker’s strictly local layer dependency, AFS does not require all layers to exist on the current machine.

As long as a layer (or the corresponding image) is available on some node in the cluster, another node can read it over the network and mount the filesystem.

This means:

- A single node’s disk is no longer the hard limit for image capacity
- The aggregate disk capacity of the cluster can hold a much larger image set
- Repeated downloads of the same image from the registry are reduced

## Components

- `afs_discovery_grpcd`
  - Service discovery and node liveness/status management (heartbeat)
  - Returns endpoints of nodes that contain a target image

- `afs_layerstore_grpcd`
  - Manages layer cache
  - Provides `PullImage / HasImage / HasLayer / StatLayer / ReadLayer`
  - Periodically reports to one or more discovery services after startup

- `afs_mount`
  - Connects only to discovery
  - Finds nodes that contain the image, then mounts
  - Automatically retries by switching providers on read failures

## Quick Flow

1. Start `afs_discovery_grpcd`
2. Start `afs_layerstore_grpcd` on nodes and register to discovery
3. Run `afs_mount` from workload side with target image and mountpoint

## Positioning

AFS currently focuses on image file reuse and acceleration for large-scale concurrent RL jobs.

It is not a replacement for Docker; it is a cross-node image file access layer for agent-centric workloads.
