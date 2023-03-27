FROM ubuntu:22.04

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:/usr/local/go/bin:${PATH}"
ENV GOLANG_VERSION="1.20"
ENV HADOOP_VERSION="3.3.2"

RUN apt-get update -yq
RUN apt-get --no-install-recommends -y install lsb-release sudo default-jre default-jdk
RUN apt-get --no-install-recommends -y install curl git sysstat attr build-essential lsof fuse coreutils
RUN apt-get --no-install-recommends -y install openjdk-8-jdk ssh openssh-server

# Install Go.
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz

# Install Hadoop.
RUN curl -LO  http://mirrors.sonic.net/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz &&\
  tar -xzf hadoop-${HADOOP_VERSION}.tar.gz > /dev/null 2>&1 &&\
  rm -rf hadoop-${HADOOP_VERSION}.tar.gz
