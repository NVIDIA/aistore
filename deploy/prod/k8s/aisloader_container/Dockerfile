FROM ubuntu:bionic
ARG start_script
RUN mkdir -p /usr/local/bin

RUN apt-get update && apt-get install -y wget sysstat python-pip curl nodejs git iputils-ping netcat golang-go redis-tools
RUN cd /opt && git clone git://github.com/etsy/statsd.git

COPY aisloader /aisloader

COPY $start_script /docker_start.sh
RUN chmod a+x /docker_start.sh

CMD ["bash","-c", "/docker_start.sh"]