#
# Dockerfile to build an aisnode Docker image
#
FROM ubuntu:bionic
ARG start_script
RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs git iputils-ping netcat make coreutils
RUN mkdir -p /usr/local/bin
RUN pip install awscli
RUN cd /opt && git clone git://github.com/etsy/statsd.git
COPY $start_script /ais_docker_start.sh
RUN chmod a+x /ais_docker_start.sh
COPY aisnode /usr/local/bin/aisnode
COPY git-showbranch.out /
CMD ["bash", "-c", "./ais_docker_start.sh"]
