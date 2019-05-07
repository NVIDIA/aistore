#
# Dockerfile to build an AIS docker image in which we include a copy of
# your GOPATH tree. Must be run from the intended $GOPATH, ie something
# like:
#
# cd $GOPATH
# docker build -f blah/blah/Dockerfile_debug ...
#
# The entry point CMD expects that at $GOPATH/bin/aisnode is the ais binary, or if that
# is absent that is should go run setup/aisnode.go
#

FROM ubuntu:bionic
ARG start_script
RUN mkdir -p /usr/local/bin

RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs git iputils-ping netcat golang-go
RUN pip install awscli
RUN cd /opt && git clone git://github.com/etsy/statsd.git

ENV GOPATH /go
# Avoid a COPY of . which may lead to pain
COPY go /go

COPY $start_script /ais_docker_start.sh
RUN chmod a+x /ais_docker_start.sh

CMD ["bash","-c", "./ais_docker_start.sh"]
