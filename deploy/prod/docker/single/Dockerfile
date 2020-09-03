FROM alpine:3.11

RUN apk upgrade --no-cache && \
  apk add --no-cache --virtual .build-deps \
    bash \
    sysstat \
    attr \
    # for 'lsblk' command
    util-linux \
  ;

WORKDIR /build
COPY entrypoint entrypoint/
COPY aisnode_config.sh ./
COPY aisnode bin/

EXPOSE 51080/tcp

ENTRYPOINT ["sh", "-c", "entrypoint/entrypoint.sh \"$@\"", "--"]
