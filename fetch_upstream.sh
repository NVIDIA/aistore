#!/usr/bin/env bash

OUT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd)"
TMP_DIR=/tmp/gh-pages-aistore

mkdir -p $TMP_DIR
git clone https://github.com/NVIDIA/aistore.git $TMP_DIR
rm -rf $TMP_DIR/3rdparty $TMP_DIR/python-client $TMP_DIR/cmn/tests $TMP_DIR/.git
find $TMP_DIR -type f | grep -v "\.\(md\|png\|xml\|jpeg\|json\|jpg\|gif\)" | xargs rm
find $TMP_DIR -type d -empty -delete

for f in $(find $TMP_DIR -type f); do
  dname=$(dirname $f)
  dname=${dname#"$TMP_DIR"}
  dname=${dname#/}

  text=""
  if [[ $(echo "$f" | grep "\(.*\)README.md") != "" ]]; then
    dnamecap=${dname^^}

    echo $f $dname $dnamecap

    text="---\nlayout: post\ntitle: $dnamecap\npermalink: $dname\nredirect_from:\n  - $dname/README.md/\n---\n"
    if [[ $dname == "" ]]; then
      text="---\nlayout: post\ntitle: AIStore\npermalink: /\nredirect_from:\n  - /README.md/\n  - README.md/\n---\n"
    fi
  elif [[ $(echo "$f" | grep "\(.*\).md") != "" ]]; then
    bname=$(basename $f .md)
    bnamecap=${bname^^}

    echo $f $dname/$bname $bnamecap

    text="---\nlayout: post\ntitle: $bnamecap\npermalink: $dname/$bname\nredirect_from:\n  - $dname/$bname.md/\n---\n"
  else
    continue
  fi

  echo "$(echo -e ${text} | cat - $f)" > $f # insert ${text} as first lines
  ex -sc '%s/"\/docs/"\/aistore\/docs/g' -cx $f # `"/docs/..."` => `"/aistore/docs/..."`
  ex -sc '%s/](\//](\/aistore\//g' -cx $f # `(/...` => `(/aistore/...`
done

cp -r $TMP_DIR/* "$OUT_DIR"/
rm -rf $TMP_DIR

