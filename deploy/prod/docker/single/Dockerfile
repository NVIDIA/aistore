FROM golang:1.13-alpine

RUN apk upgrade --no-cache && \
  apk add --no-cache --virtual .build-deps \
    bash \
    lsof \
    git \
    sysstat \
    attr \
    make \
    # for 'lsblk' command
    util-linux \
  ;

ENV GOBIN $GOPATH/bin

COPY . $GOPATH/src/github.com/NVIDIA/aistore/
WORKDIR $GOPATH/src/github.com/NVIDIA/aistore

# Cache Go packages in the docker image. This helps for much faster start of
# the Docker container since we will not need to download packages at runtime.
RUN CLDPROVIDER="" make node && CLDPROVIDER="aws" make node && CLDPROVIDER="gcp" make node

EXPOSE 51080/tcp

ENTRYPOINT ["sh", "-c", "deploy/prod/docker/single/entrypoint/entrypoint.sh \"$@\"", "--"]
