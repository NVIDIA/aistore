FROM golang:1.18

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN apt-get update -yq
RUN apt-get --no-install-recommends -y install curl git build-essential coreutils

# Clone and build latest aisnode.
RUN git clone https://github.com/NVIDIA/aistore.git && cd aistore && \
    GOOS="linux" GOARCH="amd64" CGO_ENABLED=0 BUILD_FLAGS="-a -installsuffix cgo" \
    AIS_BACKEND_PROVIDERS="ais aws gcp azure" make node


FROM alpine:3.15

RUN apk upgrade --no-cache && \
  apk add --no-cache --virtual .build-deps \
    bash \
    sysstat \
    attr \
    # for 'lsblk' command
    util-linux \
  ;

WORKDIR /build
COPY entrypoint entrypoint/
COPY aisnode_config.sh ./
COPY --from=0 /go/bin/aisnode bin/

EXPOSE 51080/tcp

ENTRYPOINT ["sh", "-c", "entrypoint/entrypoint.sh \"$@\"", "--"]
