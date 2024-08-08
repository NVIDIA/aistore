FROM golang:1.22

RUN apt update -yq
RUN apt-get --no-install-recommends -y install curl git sysstat attr build-essential lsof coreutils python3 python3-pip python3-setuptools s3cmd docker.io uuid-runtime bc gettext-base
RUN python3 --version
RUN python3 -m pip config set global.break-system-packages true
RUN pip3 install --upgrade pip && pip3 install awscli black[jupyter]

# Install `kubectl`.
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
      chmod +x ./kubectl && \
      mv ./kubectl /usr/local/bin/kubectl && \
      kubectl version --client

# Install `kind`.
RUN go install sigs.k8s.io/kind@v0.23.0

# Cache all dependencies from `aistore` and install the linter.
RUN git clone --depth=1 https://github.com/NVIDIA/aistore.git && cd aistore && \
      go mod download && \
      cd cmd/cli && go mod download && cd ../.. && \
      make lint-update-ci && \
      make install-python-deps && \
      cd .. && rm -rf aistore

# Cache all dependencies from `ais-k8s/operator`.
RUN git clone --depth=1 https://github.com/NVIDIA/ais-k8s.git && cd ais-k8s/operator && \
      go mod download && \
      make kustomize controller-gen envtest golangci-lint && \
      cd ../.. && rm -rf ais-k8s
