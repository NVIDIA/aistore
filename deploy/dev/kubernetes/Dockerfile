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
ENV GOLANG_VERSION 1.12
ENV CONFFILE /etc/ais/ais.json

# Reassign arguments to environment variables so run.sh can use them
ARG GOBASE
ENV GOPATH $GOBASE
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/aistore/ais

RUN mkdir /etc/ais
RUN mkdir /usr/nvidia
RUN mkdir -p $GOPATH/src/github.com/NVIDIA

# Installing go
RUN mkdir -p "$GOPATH/bin"
RUN chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

ARG CLDPROVIDER
ENV CLD $CLDPROVIDER

# Dry Run
ARG NODISKIO
ENV NODISK $NODISKIO

ARG DRYOBJSIZE
ENV OBJSIZE $DRYOBJSIZE

ARG TARGETS
ENV TARGETCNT $TARGETS
COPY ais.json $CONFFILE

EXPOSE 8080

ENV VERSION `git rev-parse --short HEAD`
ENV BUILD `date +%FT%T%z`

RUN cd $GOPATH/src/github.com/NVIDIA && git clone https://github.com/NVIDIA/aistore.git && cd $WORKDIR

RUN echo "\
cd $WORKDIR && GODEBUG=madvdontneed=1 go install -tags=\"${CLD}\" -ldflags \"-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'\" $WORKDIR/setup/aisnode.go  \n \
cd $GOBIN && $GOBIN/aisnode -config=\$1 -role=\$2 -ntargets=\$3 -nodiskio=\$4 -nonetio=\$5 -dryobjsize=\$6 -alsologtostderr=true  \n" \
> /run/run.sh
RUN chmod +x /run/run.sh
CMD ["sh","-c", "AIS_DAEMONID=`echo $HOSTNAME` /run/run.sh $CONFFILE $ROLE $TARGETCNT $NODISK $OBJSIZE"]
