#
# Dockerfile to build an AIS init container Docker image
#
FROM alpine:3.12.3

RUN apk add bash curl gettext jq

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl   && \
    chmod +x kubectl && mv kubectl /usr/local/bin/kubectl

CMD ["/bin/sh", "-c", "/bin/bash"]
