FROM ubuntu

ENV GOLANG_VERSION 1.10
ENV GOPATH /go
ENV GOBIN /go/bin
ENV PATH $GOBIN:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/dfcpub/dfc

RUN apt-get update &&\
  set -eux &&\
  apt-get -y install curl &&\
  apt-get --no-install-recommends -y install git &&\
  apt-get --no-install-recommends -y install lsof &&\
  apt-get --no-install-recommends -y install attr &&\
  apt-get --no-install-recommends -y install wget &&\
  apt-get --no-install-recommends -y install vim &&\
  apt-get --no-install-recommends -y install python &&\
  apt-get --no-install-recommends -y install sysstat &&\
  apt-get -y clean all
RUN mkdir -p "$GOPATH/src" "$GOBIN" && chmod -R 777 "$GOPATH"
RUN curl -LO  https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz
RUN tar -C /usr/local -xvzf go$GOLANG_VERSION.linux-amd64.tar.gz
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py
RUN pip install awscli
RUN rm -rf go$GOLANG_VERSION.linux-amd64.tar.gz

RUN go get -u -v github.com/NVIDIA/dfcpub/dfc && cd $WORKDIR
WORKDIR $WORKDIR
