FROM alpine:latest
RUN apk add curl bash
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl \
   && chmod +x kubectl && mv kubectl /usr/local/bin/kubectl

CMD /bin/bash
