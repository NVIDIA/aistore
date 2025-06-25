FROM quay.io/podman/stable:latest

ARG GO_VERSION=1.24.2

RUN dnf -y update && \
    dnf -y install \
       attr \
       awk \
       bc \
       cargo \
       coreutils \
       curl \
       gcc \
       gettext \
       git \
       lsof \
       make \
       openssl \
       podman-docker \
       procps-ng \
       python3-pip \
       python3-setuptools \
       python3.11 \
       s3cmd \
       sysstat \
       tar \
       uuid \
       which \
       yq \
    && dnf clean all

# Install Go
RUN curl -fSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz \
    && tar -C /usr/local -xzf /tmp/go.tar.gz \
    && rm /tmp/go.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOBIN="/bin"

# Configure Python
RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
RUN python3 --version \
 && python3 -m pip install --upgrade pip \
 && python3 -m pip install awscli black[jupyter]

# Install Kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/kubectl

# Install kapp
RUN curl -LO "https://github.com/carvel-dev/kapp/releases/latest/download/kapp-linux-amd64" \
    && chmod +x kapp-linux-amd64 \
    && mv kapp-linux-amd64 /usr/local/bin/kapp

# Configure Podman
RUN sed -i \
  -e 's|^utsns=".*"|utsns="private"|' \
  -e 's|^cgroupns=".*"|cgroupns="private"|' \
  -e 's|^cgroups=".*"|cgroups="enabled"|' \
  /etc/containers/containers.conf

# Cache all dependencies from `aistore`
RUN git clone --depth=1 https://github.com/NVIDIA/aistore.git && \
cd aistore && \
go mod download && \
cd cmd/cli && go mod download && cd ../.. && \
make lint-update-ci && \
make install-python-deps && \
cd .. && rm -rf aistore

# Cache all dependencies from `ais-k8s/operator`
ENV LOCALBIN=$GOBIN
RUN git clone --depth=1 https://github.com/NVIDIA/ais-k8s.git && \
cd ais-k8s/operator && \
go mod download && \
make kustomize controller-gen envtest golangci-lint mockgen kind cloud-provider-kind && \
cd ../.. && rm -rf ais-k8s

# Image for internal KinD tests in ais-k8s
COPY operator-test.tar operator-test.tar

# Install `uv` and multiple Python versions for testing
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN uv python install 3.8 3.9 3.10 3.11 3.12 3.13

# Create a directory for K8s logs
RUN mkdir -p /ais/log

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/bin/bash"]
