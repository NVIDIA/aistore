ARG GO_VERSION=1.24
FROM golang:${GO_VERSION}-alpine
RUN apk add --no-cache bash curl git make which 

# Cache all dependencies from `ais-k8s/operator`
ENV LOCALBIN="/bin"
RUN git clone --depth=1 https://github.com/NVIDIA/ais-k8s.git && \
cd ais-k8s/operator && \
go mod download && \
make kustomize controller-gen envtest golangci-lint mockgen && \
cd ../.. && rm -rf ais-k8s

# We'll need to copy in the latest operator file to test, so sleep and wait for manual exec call
ENTRYPOINT ["sleep", "infinity"]
