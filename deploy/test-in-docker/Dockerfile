FROM ubuntu:22.04

RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install \
	curl \
	git \
	ca-certificates \
	wget \
	vim \
	python3-setuptools \
	python3 \
	python3-pip \
	sysstat attr \
	net-tools \
	iproute2 \
	build-essential \
	lsof \
	iputils-ping &&\
  apt-get -y clean all

RUN pip3 install awscli s3cmd

ENV GOLANG_VERSION="1.22.1"

ENV GOPATH /go
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

# Installing go
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

RUN go install github.com/rakyll/gotest@latest

ARG cld_provider=0
ENV CLD_PROVIDER ${cld_provider}

COPY . $GOPATH/src/github.com/NVIDIA/aistore/

ENTRYPOINT [ "sh", "-c", "$GOPATH/src/github.com/NVIDIA/aistore/deploy/test-in-docker/entrypoint.sh" ]
