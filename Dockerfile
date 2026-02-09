FROM ubuntu:latest AS builder

ARG GO_VERSION=1.25.3
ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/usr/local/go/bin:$PATH
ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    make \
    pkg-config \
    libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tgz \
    && rm -rf /usr/local/go \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm -f /tmp/go.tgz

WORKDIR /src
COPY . .

RUN mkdir -p /out && \
    echo "==> building afs_discovery_grpcd (${GOOS}/${GOARCH}, cgo=${CGO_ENABLED})" && \
    go build -trimpath -o /out/afs_discovery_grpcd ./cmd/afs_discovery_grpcd && \
    echo "==> building afs_layerstore_grpcd (${GOOS}/${GOARCH}, cgo=${CGO_ENABLED})" && \
    go build -trimpath -o /out/afs_layerstore_grpcd ./cmd/afs_layerstore_grpcd && \
    echo "==> building afs_mount (${GOOS}/${GOARCH}, cgo=${CGO_ENABLED})" && \
    go build -trimpath -o /out/afs_mount ./cmd/afs_mount

FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    fuse3 \
    libfuse3-3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /out/ /usr/local/bin/

CMD ["afs_mount", "-h"]
