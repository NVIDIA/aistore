FROM ubuntu:18.04

RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install curl git ca-certificates wget vim python3-setuptools python3 python3-pip sysstat attr net-tools iproute2 build-essential lsof iputils-ping &&\
  apt-get -y clean all

RUN pip3 install awscli

ENV GOLANG_VERSION 1.12

ENV GOPATH /go
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/aistore/ais

# Installing go
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

RUN mkdir -p /tmp/ais && go get -u github.com/rakyll/gotest

ARG cld_provider=3
ENV CLD_PROVIDER ${cld_provider}

COPY . $GOPATH/src/github.com/NVIDIA/aistore/

ENTRYPOINT [ "sh", "-c", "$GOPATH/src/github.com/NVIDIA/aistore/deploy/test/docker/entrypoint.sh" ]
