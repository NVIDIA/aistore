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
ENV GOLANG_VERSION 1.11
ENV CONFFILE /etc/dfc/dfc.json

# Reassign argumets to environment variables so run.sh can use them
ARG GOBASE
ENV GOPATH $GOBASE
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/dfcpub/dfc

RUN mkdir /etc/dfc
RUN mkdir /usr/nvidia

# Installing go
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

ARG TARGETS
ENV TARGETCNT $TARGETS
COPY dfc.json $CONFFILE

EXPOSE 8080 9080 10080

RUN echo "cd $WORKDIR && go build && go install && go install $WORKDIR/setup/dfc.go \ncd $GOBIN && $GOBIN/dfc -config=\$1 -role=\$2 -ntargets=$TARGETCNT  -alsologtostderr=true \n" \
> /run/run.sh
RUN chmod +x /run/run.sh
CMD ["sh","-c", "DFCDAEMONID=`echo $HOSTNAME` /run/run.sh $CONFFILE $ROLE $TARGETS"]
