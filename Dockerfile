FROM nexus.gillouche.homelab/docker-hub/python:3.14-slim-trixie AS builder

WORKDIR /build

# hadolint ignore=DL3013
RUN pip install --no-cache-dir --target=/deps kubernetes

FROM nexus.gillouche.homelab/docker-hosted/base/python-distroless:3.14.3

COPY --from=builder /deps /app/deps
COPY src/ /app/src

ENV PYTHONPATH="/app/src:/app/deps"
ENV PYTHONUNBUFFERED="1"

USER nonroot

ENTRYPOINT ["/usr/local/bin/python3", "-m", "rebalancer.main"]
