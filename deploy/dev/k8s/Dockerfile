FROM ubuntu:18.04

RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install \
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
      iputils-ping &&\
  apt-get -y clean all

RUN wget https://bootstrap.pypa.io/get-pip.py &&\
  python get-pip.py &&\
  pip install awscli


# Setting ENV variables
ENV GOLANG_VERSION 1.13
ENV AIS_CONF_FILE /etc/ais/ais.json

# Reassign arguments to environment variables so run.sh can use them
ARG GOBASE
ENV GOPATH $GOBASE
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/aistore

RUN mkdir /etc/ais
RUN mkdir /usr/nvidia
RUN mkdir -p $GOPATH/src/github.com/NVIDIA

# Installing go
RUN mkdir -p "$GOPATH/bin"
RUN chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

ARG AIS_CLD_PROVIDER
ENV CLD $AIS_CLD_PROVIDER

# Dry Run
ARG AIS_NO_DISK_IO
ENV NO_DISK $AIS_NO_DISK_IO

ARG AIS_DRY_OBJ_SIZE
ENV OBJ_SIZE $AIS_DRY_OBJ_SIZE

ARG TARGETS
ENV TARGET_CNT $TARGETS
COPY ais.json $AIS_CONF_FILE

EXPOSE 8080

RUN cd $GOPATH/src/github.com/NVIDIA && git clone https://github.com/NVIDIA/aistore.git && cd $WORKDIR

RUN echo "\
cd $WORKDIR && AIS_CLD_PROVIDER=${CLD} make node \n \
cd $GOBIN && $GOBIN/aisnode -config=\$1 -role=\$2 -ntargets=\$3 -nodiskio=\$4 -nonetio=\$5 -dryobjsize=\$6 -alsologtostderr=true  \n" \
> /run/run.sh
RUN chmod +x /run/run.sh
CMD ["sh","-c", "AIS_DAEMON_ID=`echo $HOSTNAME` /run/run.sh $AIS_CONF_FILE $AIS_NODE_ROLE $TARGET_CNT $NO_DISK $OBJ_SIZE"]
