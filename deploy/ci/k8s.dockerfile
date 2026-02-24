ARG GO_VERSION=1.25

# Cache all dependencies from `ais-k8s/operator`
FROM docker.io/library/golang:${GO_VERSION}-alpine
RUN apk add --no-cache bash curl git make which
ENV LOCAL_BIN="/bin"
# We only need the binaries used in the test-e2e target from ais-k8s
RUN git clone --depth=1 https://github.com/NVIDIA/ais-k8s.git && \
  cd ais-k8s/operator && \
  go mod download && \
  make controller-gen envtest mockgen && \
  cd ../.. && rm -rf ais-k8s && \
  go clean -cache

# We'll need to copy in the latest operator file to test, so sleep and wait for manual exec call
ENTRYPOINT ["sleep", "infinity"]
