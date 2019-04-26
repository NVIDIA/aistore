#
# Dockerfile to build an AIS docker image in which we run from a standalone ais binary
#
FROM ubuntu:bionic
RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs git iputils-ping netcat make coreutils
RUN mkdir -p /usr/local/bin
COPY ais_binary/aisnode /usr/local/bin/aisnode
RUN pip install awscli
RUN cd /opt && git clone git://github.com/etsy/statsd.git
COPY ais_docker_start.sh ais_docker_start.sh
RUN chmod a+x ais_docker_start.sh
CMD ["bash","-c", "./ais_docker_start.sh"]
