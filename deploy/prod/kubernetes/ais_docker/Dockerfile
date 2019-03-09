#
# Dockerfile to build an AIS docker image in which we run from a standalone ais binary
#
FROM ubuntu:bionic
RUN mkdir -p /usr/local/bin
COPY ais_binary/ais /usr/local/bin/ais
RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs collectd collectd-utils git iputils-ping netcat make
RUN pip install awscli
RUN cd /opt && git clone git://github.com/etsy/statsd.git
RUN wget http://launchpadlibrarian.net/235493456/sysstat_11.2.0-1_amd64.deb -O /tmp/sysstat_11.2.0-1_amd64.deb && dpkg -i /tmp/sysstat_11.2.0-1_amd64.deb && rm /tmp/sysstat_11.2.0-1_amd64.deb
COPY ais_docker_start.sh ais_docker_start.sh
RUN chmod a+x ais_docker_start.sh
CMD ["bash","-c", "./ais_docker_start.sh"]
