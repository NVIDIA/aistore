FROM ubuntu:xenial

ENV GOLANG_VERSION 1.12

ENV PATH   /usr/local/go/bin:$PATH
ENV HOME   /root/
ENV GOPATH $HOME/ais/
ENV GOBIN  $GOPATH/bin
ENV AISSRC $GOPATH/src/github.com/NVIDIA/aistore/ais

RUN apt-get update && apt-get install -y \
        curl \ 
        jq

RUN mkdir -p "${GOPATH}/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

RUN mkdir -p $HOME/ais/bin;
RUN mkdir -p $HOME/ais/pkg;
RUN mkdir -p $HOME/ais/src;
RUN go get -v github.com/NVIDIA/aistore/ais

COPY ais_test/run_unittest.sh /root/run_unittest.sh
RUN chmod a+x /root/run_unittest.sh

CMD /bin/bash

