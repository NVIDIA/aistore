xmeta is a low-level utility to format (or extract into plain text)
assorted AIS control structures.

```console
Usage of xmeta:
  -f string
        override automatic format detection (values are smap, bmd, rmd, conf, vmd, mt)
  -h    print usage and exit
  -in string
        fully-qualified input filename
  -out string
        output filename (optional when extracting)
  -x    true: extract AIS-formatted metadata type, false: pack and AIS-format plain-text metadata
Build:
        go install xmeta.go

Examples:
        xmeta -h                                          - show usage
        # Smap:
        xmeta -x -in=~/.ais0/.ais.smap                    - extract Smap to STDOUT
        xmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt - extract Smap to /tmp/smap.txt
        xmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap       - format plain-text /tmp/smap.txt
        # BMD:
        xmeta -x -in=~/.ais0/.ais.bmd                     - extract BMD to STDOUT
        xmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt   - extract BMD to /tmp/bmd.txt
        xmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd         - format plain-text /tmp/bmd.txt
        # RMD:
        xmeta -x -in=~/.ais0/.ais.rmd                     - extract RMD to STDOUT
        xmeta -x -in=~/.ais0/.ais.rmd -out=/tmp/rmd.txt   - extract RMD to /tmp/rmd.txt
        xmeta -in=/tmp/rmd.txt -out=/tmp/.ais.rmd         - format plain-text /tmp/rmd.txt
        # Config:
        xmeta -x -in=~/.ais0/.ais.conf                    - extract Config to STDOUT
        xmeta -x -in=~/.ais0/.ais.conf -out=/tmp/conf.txt - extract Config to /tmp/config.txt
        xmeta -in=/tmp/conf.txt -out=/tmp/.ais.conf       - format plain-text /tmp/config.txt
        # VMD:
        xmeta -x -in=~/.ais0/.ais.vmd                     - extract VMD to STDOUT
        xmeta -x -in=~/.ais0/.ais.vmd -out=/tmp/vmd.txt   - extract VMD to /tmp/vmd.txt
        xmeta -in=/tmp/vmd.txt -out=/tmp/.ais.vmd         - format plain-text /tmp/vmd.txt
        # EC Metadata:
        xmeta -x -in=/data/@ais/abc/%mt/readme            - extract Metadata to STDOUT with auto-detection (by directory name)
        xmeta -x -in=./readme -f mt                       - extract Metadata to STDOUT with explicit source format
```
