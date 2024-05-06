FROM ubuntu:22.04

RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install \
      lsof \
      curl \
      git \
      ca-certificates \
      wget \
      vim \
      python3 \
      python3-distutils \
      python3-apt \
      sysstat \
      attr \
      net-tools \
      iproute2 \
      make \
      gnupg \
      iputils-ping &&\
  apt-get -y clean all

RUN wget https://bootstrap.pypa.io/get-pip.py &&\
  python3 get-pip.py &&\
  pip install awscli s3cmd

# Setting ENV variables
ENV GOLANG_VERSION="1.22.1"

# Reassign arguments to environment variables so run.sh can use them
ENV GOPATH /go
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

# Installing go
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

# Install statsd
RUN git clone https://github.com/etsy/statsd.git

ENV AIS_CONF_DIR /tmp/.conf
ENV AIS_CONF_FILE ${AIS_CONF_DIR}/ais.json
ENV AIS_LOCAL_CONF_FILE ${AIS_CONF_DIR}/ais_local.json
ENV COLLECTD_CONF_FILE ${AIS_CONF_DIR}/collectd.conf
ENV STATSD_CONF_FILE ${AIS_CONF_DIR}/statsd.conf

ENV MOUNTPATH /tmp/ais

COPY aisnode_config.sh aisnode_config.sh
COPY entrypoint/entrypoint.sh entrypoint.sh
COPY limits.conf /etc/security/limits.conf

RUN mkdir -p $GOPATH/src/github.com/NVIDIA && \
    cd $GOPATH/src/github.com/NVIDIA && \
    git clone https://github.com/NVIDIA/aistore.git && \
    cd ${GOPATH}/src/github.com/NVIDIA/aistore && \
    AIS_BACKEND_PROVIDERS="aws azure gcp" make node


WORKDIR "${GOPATH}/src/github.com/NVIDIA/aistore"

EXPOSE 51080 9080 10080

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
