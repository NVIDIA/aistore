#!/bin/bash

PRJ_DIR=$1
OUT_DIR=$2
TMP_DIR=/tmp/gh-pages-aistore/

mkdir -p $TMP_DIR
cp -r $PRJ_DIR/* $TMP_DIR
rm -rf $TMP_DIR/3rdparty

find $TMP_DIR -type f ! -name "*.(md|png|xml|jpeg|jpg)" -exec rm {} \;

find $TMP_DIR -type d -print | xargs rmdir 2>/dev/null

mkdir -p $OUT_DIR
cp -r $TMP_DIR/* $OUT_DIR/

