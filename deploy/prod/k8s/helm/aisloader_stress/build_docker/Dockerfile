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

RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs git iputils-ping netcat golang-go
RUN pip install awscli
RUN cd /opt && git clone git://github.com/etsy/statsd.git

COPY aisloader /aisloader

COPY $start_script /docker_start.sh
RUN chmod a+x /docker_start.sh

CMD ["bash","-c", "/docker_start.sh"]