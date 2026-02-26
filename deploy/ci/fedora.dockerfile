FROM quay.io/podman/stable:latest

ARG GO_VERSION=1.25

RUN dnf -y --setopt=install_weak_deps=False install \
  attr \
  awk \
  buildah \
  bc \
  coreutils \
  curl \
  gcc \
  gcc-c++ \
  gettext \
  git \
  jq \
  lsof \
  make \
  openssl \
  podman-docker \
  procps-ng \
  s3cmd \
  sysstat \
  tar \
  uuid \
  which \
  xxhash \
  yq \
  && dnf clean all

# Install Go
RUN curl -fSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz \
  && tar -C /usr/local -xzf /tmp/go.tar.gz \
  && rm /tmp/go.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOBIN="/bin"

# Install `uv` and multiple Python versions for testing
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN uv python install 3.8 3.9 3.10 3.11 3.12 3.13 3.14 \
  && uv cache clean

# Configure Python in the PATH with a general-purpose venv
ENV PATH="/opt/venv/bin:${PATH}"
# Tell make targets which PIP command to use
ENV PIP="pip"
# seed installs pip, setuptools, wheel
RUN uv venv --python 3.14 --seed /opt/venv \
  && uv pip install --no-cache --python /opt/venv/bin/python black[jupyter] \
  && ln -sf pip /opt/venv/bin/pip3

# Install Kubectl and kapp
RUN curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
  && chmod +x kubectl \
  && mv kubectl /usr/local/bin/kubectl \
  && curl -LO "https://github.com/carvel-dev/kapp/releases/latest/download/kapp-linux-amd64" \
  && chmod +x kapp-linux-amd64 \
  && mv kapp-linux-amd64 /usr/local/bin/kapp

# Configure Podman to allow KinD clusters to set up cgroup hierarchies within the CI container
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
  cd .. && rm -rf aistore \
  && go clean -cache

# Cache all dependencies from `ais-k8s/operator` for make targets run directly in CI container
# Note these directories must match what the ais-k8s repo uses for gitlab-ci
ENV LOCAL_BIN=/ci-tools/bin
ENV PATH="${LOCAL_BIN}:${PATH}"
ENV LOCAL_MANIFESTS=/ci-tools/manifests
RUN git clone --depth=1 https://github.com/NVIDIA/ais-k8s.git && \
  cd ais-k8s/operator && \
  go mod download && \
  CGO_ENABLED=0 make cache-test-deps && \
  cd ../.. && rm -rf ais-k8s \
  && go clean -cache

# Image for internal KinD tests in ais-k8s with pre-loaded dependencies
COPY operator-test.tar.gz operator-test.tar.gz

COPY --chmod=755 entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/bin/bash"]
