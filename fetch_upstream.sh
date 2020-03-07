#!/usr/bin/env bash

OUT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd)"
TMP_DIR=/tmp/gh-pages-aistore

mkdir -p $TMP_DIR
git clone https://github.com/NVIDIA/aistore.git $TMP_DIR
rm -rf $TMP_DIR/3rdparty $TMP_DIR/python-client $TMP_DIR/cmn/tests $TMP_DIR/.git
find $TMP_DIR -type f | grep -v "\.\(md\|png\|xml\|jpeg\|json\|jpg\|gif\)" | xargs rm
find $TMP_DIR -type d -empty -delete

for f in $(find $TMP_DIR -type f); do
  if [[ $(echo "$f" | grep "\(.*\)README.md") != "" ]]; then
    dname=$(dirname $f)
    dname=${dname#"$TMP_DIR"}
    dname=${dname#/}
    dnamecap=${dname^^}

    echo $f $dname $dnamecap

    TEXT="---\nlayout: post\ntitle: $dnamecap\npermalink: $dname\nredirect_from:\n- $dname/README.md/\n---\n"
    if [[ $dname == "" ]]; then
      TEXT="---\nlayout: post\ntitle: AIStore\npermalink: /\nredirect_from:\n- /README.md/\n---\n"
    fi

    echo "$(echo -e ${TEXT} | cat - $f)" > $f # insert ${TEXT} as first lines
  elif [[ $(echo "$f" | grep "\(.*\).md") != "" ]]; then
    dname=$(dirname $f)
    dname=${dname#"$TMP_DIR"}
    dname=${dname#/}
    bname=$(basename $f .md)
    bnamecap=${bname^^}

    echo $f $dname/$bname $bnamecap

    TEXT="---\nlayout: post\ntitle: $bnamecap\npermalink: $dname/$bname\nredirect_from:\n- $dname/$bname.md/\n---\n"
    echo "$(echo -e ${TEXT} | cat - $f)" > $f # insert ${TEXT} as first lines
  fi
done

cp -r $TMP_DIR/* "$OUT_DIR"/
rm -rf $TMP_DIR

