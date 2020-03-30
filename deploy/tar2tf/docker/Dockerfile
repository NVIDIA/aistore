FROM golang:1.13-buster

RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install curl git ca-certificates wget vim python3-setuptools python3 python3-pip \
   python3-venv sysstat attr net-tools iproute2 build-essential lsof iputils-ping fuse &&\
  apt-get -y clean all

RUN pip3 install awscli

ARG cld_provider=0
ENV CLD_PROVIDER ${cld_provider}

COPY python/tar2tf/requirements-jupyter.txt .
RUN pip3 install virtualenv && virtualenv -p /usr/bin/python3 /venv && . /venv/bin/activate && \
    pip3 install -r requirements-jupyter.txt && rm requirements-jupyter.txt

COPY . $GOPATH/src/github.com/NVIDIA/aistore/

WORKDIR $GOPATH/src/github.com/NVIDIA/aistore/python/tar2tf
EXPOSE 8888

ENTRYPOINT [ "sh", "-c", "$GOPATH/src/github.com/NVIDIA/aistore/deploy/tar2tf/docker/entrypoint.sh" ]
