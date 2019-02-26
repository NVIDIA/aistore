FROM ubuntu:18.04

RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install \
      lsof \
      curl \
      git \
      ca-certificates \
      wget \
      vim \
      python \
      sysstat \
      attr \
      net-tools \
      iproute2 \
      gnupg \
      iputils-ping &&\
  apt-get -y clean all

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y nodejs

RUN wget https://bootstrap.pypa.io/get-pip.py &&\
  python get-pip.py &&\
  pip install awscli

# Setting ENV variables
ENV GOLANG_VERSION 1.12

# Reassign arguments to environment variables so run.sh can use them
ARG GOBASE
ENV GOPATH $GOBASE
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

# Installing go
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

# Install statsd
RUN git clone https://github.com/etsy/statsd.git

ENV CONFDIR /tmp/.conf
ENV CONFFILE ${CONFDIR}/ais.json
ENV CONFFILE_COLLECTD ${CONFDIR}/collectd.conf
ENV CONFFILE_STATSD ${CONFDIR}/statsd.conf

ENV MOUNTPATH /tmp/ais

ARG QUICK
ENV QUICK=${QUICK}

COPY config.sh config.sh
COPY entrypoint/entrypoint.sh entrypoint.sh

EXPOSE 8080 9080 10080

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
