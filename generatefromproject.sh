#!/bin/bash

PRJ_DIR=$1
OUT_DIR=$2
TMP_DIR=/tmp/gh-pages-aistore

mkdir -p $TMP_DIR
cp -r "$PRJ_DIR"/* $TMP_DIR
rm -rf $TMP_DIR/3rdparty $TMP_DIR/python-client

find $TMP_DIR -type f ! -regex ".*\(md\|png\|xml\|jpeg\|json\|jpg\|gif\)" -exec rm -f {} \;

find $TMP_DIR -type d -empty -delete

for f in $(find $TMP_DIR -type f)
do
  if [[ $(echo "$f" | grep "\(.*\)README.md") != "" ]]
  then
    dname=$(dirname $f)
    dname=${dname#"$TMP_DIR"}
    dname=${dname#/}
    dnamecap=${dname^^}

    echo $f $dname $dnamecap

    TEXT="---\nlayout: post\ntitle: $dnamecap\npermalink: $dname\nredirect_from:\n- $dname/README.md/\n---"
    if [[ $dname == "" ]]
    then
      TEXT="---\nlayout: post\ntitle: AIStore\npermalink: /\nredirect_from:\n- /README.md/\n---"
    fi

    sed -i "1i${TEXT}\n" $f
  elif [[ $(echo "$f" | grep "\(.*\).md") != "" ]]
  then
    dname=$(dirname $f)
    dname=${dname#"$TMP_DIR"}
    dname=${dname#/}
    bname=$(basename $f .md)
    bnamecap=${bname^^}

    echo $f $dname/$bname $bnamecap

    TEXT="---\nlayout: post\ntitle: $bnamecap\npermalink: $dname/$bname\nredirect_from:\n- $dname/$bname.md/\n---"
    sed -i "1i${TEXT}\n" $f
  fi
done

mkdir -p "$OUT_DIR"
cp -r $TMP_DIR/* "$OUT_DIR"/
rm -rf $TMP_DIR

