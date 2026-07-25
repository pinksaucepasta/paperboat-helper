ARG NODE_BASE_IMAGE=node:24-bookworm-slim@sha256:cb4e8f7c443347358b7875e717c29e27bf9befc8f5a26cf18af3c3dec80e58c5
ARG GO_BASE_IMAGE=golang:1.25-bookworm@sha256:a9c020ee3d1508c7be5435c262434e3d3fc1d0e76a11afeb9ddae7d60bc86aa4

FROM ${GO_BASE_IMAGE} AS paperboat-helper
ARG PAPERBOAT_HELPER_REVISION
ARG PAPERBOAT_HELPER_VERSION
WORKDIR /src/paperboat-helper
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN printf '%s\n' "${PAPERBOAT_HELPER_VERSION}" | grep -Eq '^[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.(0|[1-9][0-9]*)$' \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/pinksaucepasta/paperboat-helper/internal/buildinfo.Version=${PAPERBOAT_HELPER_VERSION} -X github.com/pinksaucepasta/paperboat-helper/internal/buildinfo.Commit=${PAPERBOAT_HELPER_REVISION}" -o /out/pbh ./cmd/pbh

FROM ${NODE_BASE_IMAGE}

ARG NODE_BASE_IMAGE
ARG GO_BASE_IMAGE
ARG TARGETARCH
ARG PAPERBOAT_HELPER_REVISION
ARG PAPERBOAT_HELPER_VERSION
ARG HERDR_VERSION=0.7.4
LABEL org.opencontainers.image.source="https://github.com/pinksaucepasta/paperboat" \
  io.paperboat.image.contract="hosted-helper-v1" \
  io.paperboat.base.node="${NODE_BASE_IMAGE}" \
  io.paperboat.base.go="${GO_BASE_IMAGE}" \
  io.paperboat.source.helper.revision="${PAPERBOAT_HELPER_REVISION}" \
  io.paperboat.source.helper.version="${PAPERBOAT_HELPER_VERSION}" \
  io.paperboat.herdr.version="${HERDR_VERSION}" \
  io.paperboat.protocol.version="1.0"

RUN apt-get update \
  && apt-get install -y --no-install-recommends bash bzip2 ca-certificates curl git openssh-client python3 python3-venv tini xz-utils \
  && rm -rf /var/lib/apt/lists/* \
  && node --version \
  && npm --version \
  && git --version

ARG CHEZMOI_VERSION=2.71.0
RUN case "$TARGETARCH" in \
      amd64) CHEZMOI_SHA256=6ea2040ecc0e82d3dac604289e100b0157afefcd94ebb818e5f6e31655156d34 ;; \
      arm64) CHEZMOI_SHA256=d8fb35f9d43237b4f6d022cad40e1094957b990cfaee5f3b131ded65422b0983 ;; \
      *) echo "unsupported chezmoi architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
  && curl -fsSLo /tmp/chezmoi.tar.gz "https://github.com/twpayne/chezmoi/releases/download/v${CHEZMOI_VERSION}/chezmoi_${CHEZMOI_VERSION}_linux_${TARGETARCH}.tar.gz" \
  && echo "${CHEZMOI_SHA256}  /tmp/chezmoi.tar.gz" | sha256sum -c - \
  && tar -xzf /tmp/chezmoi.tar.gz -C /usr/local/bin chezmoi \
  && rm /tmp/chezmoi.tar.gz \
  && chmod 0755 /usr/local/bin/chezmoi \
  && chezmoi --version

RUN case "$TARGETARCH" in \
      amd64) HERDR_ASSET=herdr-linux-x86_64; HERDR_SHA256=bc0fc02d4ba500f9cac2353a43e67fe036785ecca6eb55378e050fac3c103059 ;; \
      arm64) HERDR_ASSET=herdr-linux-aarch64; HERDR_SHA256=544e0002de42806d1ab64ccdef3a7e7414f24717b0b6b022bc9e57d2eefd26a2 ;; \
      *) echo "unsupported Herdr architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
  && curl -fsSLo /usr/local/bin/herdr "https://github.com/ogulcancelik/herdr/releases/download/v${HERDR_VERSION}/${HERDR_ASSET}" \
  && echo "${HERDR_SHA256}  /usr/local/bin/herdr" | sha256sum -c - \
  && chmod 0755 /usr/local/bin/herdr \
  && herdr --version | grep -Fx "herdr ${HERDR_VERSION}"

ENV PAPERBOAT_HELPER_PROFILE=hosted \
  PAPERBOAT_HELPER_STATE_ROOT=/workspace/.paperboat/helper \
  PAPERBOAT_HELPER_LISTEN_ADDRESS=127.0.0.1:8080 \
  PAPERBOAT_WORKSPACE=/workspace \
  PAPERBOAT_PRESET_DIR=/etc/paperboat/presets.d

COPY --from=paperboat-helper /out/pbh /usr/local/bin/pbh
COPY deploy/hosted-image/bin/paperboat-healthcheck /usr/local/bin/paperboat-healthcheck
COPY deploy/hosted-image/bin/paperboat-workspace-cd.sh /usr/local/bin/paperboat-workspace-cd.sh
COPY deploy/hosted-image/presets.d/ /etc/paperboat/presets.d/

RUN chmod 0755 /usr/local/bin/pbh /usr/local/bin/paperboat-healthcheck /usr/local/bin/paperboat-workspace-cd.sh \
  && chmod 0644 /etc/paperboat/presets.d/*.sh \
  && install -m 0644 /usr/local/bin/paperboat-workspace-cd.sh /etc/profile.d/50-paperboat-workspace.sh \
  && printf '\n. /etc/profile.d/50-paperboat-workspace.sh\n' >> /etc/bash.bashrc \
  && printf 'export PATH="$HOME/.local/bin:$PATH"\n' > /etc/profile.d/40-paperboat-local-bin.sh \
  && mkdir -p /workspace /var/lib/paperboat \
  && chmod 0700 /var/lib/paperboat

WORKDIR /workspace
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 CMD ["/usr/local/bin/paperboat-healthcheck"]
ENTRYPOINT ["/usr/bin/tini", "-s", "--"]
CMD ["/usr/local/bin/pbh", "run"]
