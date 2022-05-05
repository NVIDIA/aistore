FROM golang:1.18

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN apt-get update -yq
RUN apt-get --no-install-recommends -y install curl git sysstat attr build-essential lsof fuse coreutils python3-pip python3-setuptools s3cmd
RUN pip3 install yapf pylint awscli

# Install `kubectl`.
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl
RUN kubectl version --client

# Cache all dependencies and install the linter.
RUN git clone --depth=1 https://github.com/NVIDIA/aistore.git && cd aistore && \
    go mod download && \
    make lint-update && \
    cd .. && rm -rf aistore
