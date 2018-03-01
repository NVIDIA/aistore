#!/bin/bash
#NGHTTP
sudo apt-get install g++ make binutils autoconf automake autotools-dev libtool pkg-config \
      zlib1g-dev libcunit1-dev libssl-dev libxml2-dev libev-dev libevent-dev libjansson-dev \
      libc-ares-dev libjemalloc-dev libsystemd-dev \
      cython python3-dev python-setuptools

wget https://github.com/nghttp2/nghttp2/releases/download/v1.31.0/nghttp2-1.31.0.tar.bz2
tar xf nghttp2-1.31.0.tar.bz2
cd nghttp2-1.31.0
./configure --enable-apps
make install
cd -
rm nghttp2-1.31.0.tar.bz2
