FROM ubuntu

ARG CLDPROVIDER
ENV CLD $CLDPROVIDER

#Setting ENV variables 
ENV GOLANG_VERSION 1.11.4
ENV CONFFILE /etc/ais/ais.json
ENV ROLE proxy
ENV TARGETS 1000
ENV GOPATH /go
ENV GOBIN /go/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/aistore/ais

#Installing go
RUN apt-get update &&\
  set -eux &&\
  apt-get -y install curl &&\
  apt-get --no-install-recommends -y install git &&\
  apt-get --no-install-recommends -y install wget &&\
  apt-get --no-install-recommends -y install vim &&\
  apt-get --no-install-recommends -y install python &&\
  apt-get --no-install-recommends -y install sysstat &&\
  apt-get -y clean all
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO  https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz
RUN tar -C /usr/local -xvzf go$GOLANG_VERSION.linux-amd64.tar.gz
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py
RUN pip install awscli
RUN rm -rf go$GOLANG_VERSION.linux-amd64.tar.gz

#Get Source code with a specific hash 
RUN go get -u -v github.com/NVIDIA/aistore/ais
ARG HASH
RUN rm -rf $GOPATH/src/github.com/NVIDIA/aistore
RUN cd $GOPATH/src/github.com/NVIDIA &&\
    git clone https://github.com/NVIDIA/aistore.git &&\
    cd $WORKDIR &&\
    git reset --hard $HASH
WORKDIR $WORKDIR

ENV VERSION `git rev-parse --short HEAD`
ENV BUILD `date +%FT%T%z`
RUN go install -tags="${CLD}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/ais.go

RUN apt-get -y remove wget
RUN mkdir /etc/ais
RUN mkdir /usr/nvidia
COPY ais.json $CONFFILE

# Statsd
ENV STATSD_PATH /opt/statsd
ENV STATSD_CONF statsd.conf
RUN apt-get -y install nodejs -y
RUN git clone git://github.com/etsy/statsd.git $STATSD_PATH
RUN apt-get -y install nodejs-legacy
COPY $STATSD_CONF $STATSD_PATH

# Collectd
RUN apt-get -y install collectd collectd-utils
COPY collectd.conf /etc/collectd


RUN echo "\
service collectd start \n \
node $STATSD_PATH/stats.js $STATSD_PATH/$STATSD_CONF& \n \
$GOBIN/ais -config=\$1 -role=\$2 -ntargets=\$3  -alsologtostderr=true \n" \
> /run/run.sh
RUN chmod +x /run/run.sh
CMD ["sh","-c", "AIS_DAEMONID=`echo $HOSTNAME` /run/run.sh $CONFFILE $ROLE $TARGETS"]

HEALTHCHECK cmd curl --fail http://127.0.0.1:8080/v1/health || exit 1

