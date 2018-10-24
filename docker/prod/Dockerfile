FROM ubuntu

#Setting ENV variables 
ENV GOLANG_VERSION 1.9
ENV CONFFILE /etc/dfc/dfc.json
ENV ROLE proxy
ENV TARGETS 1000
ENV GOPATH /go
ENV GOBIN /go/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV WORKDIR $GOPATH/src/github.com/NVIDIA/dfcpub/dfc

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
RUN go get -u -v github.com/NVIDIA/dfcpub/dfc
ARG HASH
RUN rm -rf $GOPATH/src/github.com/NVIDIA/dfcpub
RUN cd $GOPATH/src/github.com/NVIDIA &&\
    git clone https://github.com/NVIDIA/dfcpub.git &&\
    cd $WORKDIR &&\
    git reset --hard $HASH
WORKDIR $WORKDIR
RUN go get -v
RUN go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$HASH" setup/dfc.go

RUN apt-get -y remove wget
RUN mkdir /etc/dfc
RUN mkdir /usr/nvidia
COPY dfc.json $CONFFILE

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
$GOBIN/dfc -config=\$1 -role=\$2 -ntargets=\$3  -alsologtostderr=true \n" \
> /run/run.sh
RUN chmod +x /run/run.sh
CMD ["sh","-c", "DFCDAEMONID=`echo $HOSTNAME` /run/run.sh $CONFFILE $ROLE $TARGETS"]

HEALTHCHECK cmd curl --fail http://127.0.0.1:8080/v1/health || exit 1

