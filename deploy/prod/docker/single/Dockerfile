FROM golang:1.23 AS builder

ARG mode
ARG providers

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NOWARNINGS="yes"

ENV AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"}

# NOTE: Uncomment this line if you plan to use shell prompt inside the container.
# RUN apt-get update -yq && apt-get --no-install-recommends -y install curl git coreutils util-linux

RUN mkdir -p /go/src/github.com/NVIDIA/aistore
WORKDIR /go/src/github.com/NVIDIA/aistore

# Clone GitHub and build aisnode.
# To deploy from remote branch, run:
#    git clone -b <BRANCH-NAME> --single-branch https://github.com/NVIDIA/aistore.git
#
RUN git clone https://github.com/NVIDIA/aistore.git && cd aistore && go mod tidy && \
    GOOS="linux" GOARCH="amd64" \
    CGO_ENABLED=0 BUILD_FLAGS="-a -installsuffix cgo" \
    MODE=${mode} make node

FROM alpine:latest

# NOTE: important to reproduce backend providers again
#       for the first (deployment) run
#       where the initial plain-text config gets generated.
ENV AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"}

# TODO: lsof (installed by default from busybox) doesn't handle `-sTCP:LISTEN` and `tcp@host:port`
RUN apk upgrade --no-cache && apk add --no-cache --virtual .build-deps \
    bash \
    sysstat \
    attr \
  ;

WORKDIR /build
COPY entrypoint entrypoint/
COPY utils.sh ./
COPY aisnode_config.sh ./
COPY limits.conf /etc/security/limits.conf
COPY --from=builder /go/bin/aisnode bin/

EXPOSE 51080/tcp

ENTRYPOINT ["sh", "-c", "entrypoint/entrypoint.sh \"$@\"", "--"]
