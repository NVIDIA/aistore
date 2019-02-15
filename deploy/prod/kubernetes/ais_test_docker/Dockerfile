FROM ubuntu:xenial

ENV PATH /usr/local/go/bin:$PATH
ENV HOME   /root/
ENV GOPATH $HOME/ais/
ENV GOBIN  $GOPATH/bin
ENV AISSRC $GOPATH/src/github.com/NVIDIA/aistore/ais

RUN apt-get update && apt-get install -y       \
                                          curl \
					  git
    
ADD https://storage.googleapis.com/golang/go1.11.linux-amd64.tar.gz /tmp/go1.11.linux-amd64.tar.gz
RUN tar -C /usr/local/ -xvzf /tmp/go1.11.linux-amd64.tar.gz > /dev/null
RUN rm -rf /tmp/go1.11.linux-amd64.tar.gz

RUN mkdir -p $HOME/ais/bin;
RUN mkdir -p $HOME/ais/pkg;
RUN mkdir -p $HOME/ais/src;
RUN /usr/local/go/bin/go get -v github.com/NVIDIA/aistore/ais

COPY ais_test/run_unittest.sh /root/run_unittest.sh
RUN chmod a+x /root/run_unittest.sh

CMD /bin/bash

