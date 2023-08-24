FROM golang:1.20

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN apt-get update -yq
RUN apt-get --no-install-recommends -y install curl git sysstat attr build-essential lsof coreutils python3-pip python3-setuptools s3cmd docker.io uuid-runtime
# Python source-build requirements
RUN apt-get --no-install-recommends -y install wget libbz2-dev libncursesw5-dev unzip libreadline-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libffi-dev zlib1g-dev xz-utils lzma liblzma-dev

# Install Python 3.11 from source
RUN curl -o python3.11.tgz https://www.python.org/ftp/python/3.11.1/Python-3.11.1.tgz
RUN tar xf python3.11.tgz
RUN (cd Python-3.11.1 && ./configure --enable-optimizations && make -j4 && make install)
RUN pip3 install --upgrade pip
RUN pip3 install awscli black[jupyter]

# Install `kubectl`.
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl
RUN kubectl version --client

# Cache all dependencies and install the linter.
RUN git clone --depth=1 https://github.com/NVIDIA/aistore.git && cd aistore && \
go mod download && \
cd cmd/cli && go mod download && cd ../.. && \
make lint-update-ci && \
make install-python-deps && \
cd .. && rm -rf aistore
