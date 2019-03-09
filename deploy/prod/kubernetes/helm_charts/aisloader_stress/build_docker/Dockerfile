#
# Dockerfile to build an AIS docker image in which we include a copy of
# your GOPATH tree. Must be run from the intended $GOPATH, and must pass the
# path to the start script at build time, ie something like:
#
# cd $GOPATH
# docker build -f blah/blah/Dockerfile --build-arg start_script=blah/blah/aisloader_docker_start.sh
#

FROM ubuntu:bionic
ARG start_script
RUN mkdir -p /usr/local/bin

RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs collectd collectd-utils git iputils-ping netcat golang-go
RUN pip install awscli
RUN cd /opt && git clone git://github.com/etsy/statsd.git

RUN wget http://launchpadlibrarian.net/235493456/sysstat_11.2.0-1_amd64.deb -O /tmp/sysstat_11.2.0-1_amd64.deb && dpkg -i /tmp/sysstat_11.2.0-1_amd64.deb && rm /tmp/sysstat_11.2.0-1_amd64.deb

ENV GOPATH /go
# Avoid a COPY of . which may lead to pain
COPY src /go/src
COPY bin /go/bin
COPY pkg /go/pkg

COPY $start_script /docker_start.sh
RUN chmod a+x /docker_start.sh

CMD ["bash","-c", "/docker_start.sh"]