SHELL := /bin/bash

GO ?= go
CGO_ENABLED ?= 1
HOST_GOOS := $(shell $(GO) env GOOS)
HOST_GOARCH := $(shell $(GO) env GOARCH)

OUT_DIR ?= dist/$(HOST_GOOS)_$(HOST_GOARCH)
BINS := afs_discovery_grpcd afs_layerstore_grpcd afs_mount afs_registry_puller afs_layer_converter afs_runc afslet afs_cli afs_proxy
DOCKER_IMAGE ?= afs-binaries:linux-amd64

.PHONY: all build-local build-linux-docker clean print-vars

all: build-local

build-local:
	@mkdir -p $(OUT_DIR)
	@for b in $(BINS); do \
		echo "==> building $$b ($(HOST_GOOS)/$(HOST_GOARCH), cgo=$(CGO_ENABLED))"; \
		CGO_ENABLED=$(CGO_ENABLED) \
			$(GO) build -trimpath -o $(OUT_DIR)/$$b ./cmd/$$b; \
	done

build-linux-docker:
	docker build --build-arg GO_VERSION=1.25.3 -t $(DOCKER_IMAGE) .

print-vars:
	@echo "GO=$(GO)"
	@echo "HOST_GOOS=$(HOST_GOOS)"
	@echo "HOST_GOARCH=$(HOST_GOARCH)"
	@echo "CGO_ENABLED=$(CGO_ENABLED)"
	@echo "OUT_DIR=$(OUT_DIR)"
	@echo "BINS=$(BINS)"
	@echo "DOCKER_IMAGE=$(DOCKER_IMAGE)"

clean:
	rm -rf dist
