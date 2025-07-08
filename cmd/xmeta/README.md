# `xmeta`: AIS Metadata Tool

`xmeta` is a low-level utility for extracting and formatting various internal AIS (AIStore) metadata structures into human-readable plain text, or vice versa.

It supports multiple metadata types including:
**Smap**, **BMD**, **RMD**, **Config**, **VMD**, **EC Metadata**, **LOM**, and **ETL Metadata (EMD)**.

---

## Usage

```bash
Usage of xmeta:
  -f string
        Override automatic format detection.
        Accepted values: smap, bmd, rmd, conf, vmd, mt, lom, emd
  -h    Show help and exit
  -in string
        Fully-qualified input filename
  -out string
        Output filename (optional for extraction)
  -x    
        Mode switch:
          true  - extract AIS-formatted metadata into plain text
          false - pack plain text into AIS-formatted metadata
```

---

## Build

```bash
cd <aistore-dir>
make xmeta
```

---

## Examples

### General Help

```bash
xmeta -h
```

---

### Smap

```bash
xmeta -x -in=~/.ais0/.ais.smap                      # Extract to STDOUT
xmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt   # Extract to file
xmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap         # Format plain text to AIS format
```

### BMD

```bash
xmeta -x -in=~/.ais0/.ais.bmd                      # Extract
xmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt    # Extract to file
xmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd          # Format
```

### RMD

```bash
xmeta -x -in=~/.ais0/.ais.rmd                      # Extract
xmeta -x -in=~/.ais0/.ais.rmd -out=/tmp/rmd.txt    # Extract to file
xmeta -in=/tmp/rmd.txt -out=/tmp/.ais.rmd          # Format
```

### Config

```bash
xmeta -x -in=~/.ais0/.ais.conf                    # Extract
xmeta -x -in=~/.ais0/.ais.conf -out=/tmp/conf.txt # Extract to file
xmeta -in=/tmp/conf.txt -out=/tmp/.ais.conf       # Format
```

### VMD

```bash
xmeta -x -in=~/.ais0/.ais.vmd                    # Extract
xmeta -x -in=~/.ais0/.ais.vmd -out=/tmp/vmd.txt # Extract to file
xmeta -in=/tmp/vmd.txt -out=/tmp/.ais.vmd       # Format
```

### EC Metadata (mt)

```bash
xmeta -x -in=/data/@ais/abc/%mt/readme           # Auto-detect format from path
xmeta -x -in=./readme -f mt                       # Explicit format override
```

### LOM (readonly, no auto-detection)

```bash
xmeta -x -in=/data/@ais/abc/%ob/img001.tar -f lom                      # Extract
xmeta -x -in=/data/@ais/abc/%ob/img001.tar -out=/tmp/lom.txt -f lom    # Extract to file
```

### EMD (ETL Metadata)

```bash
xmeta -x -in=~/.ais0/.ais.emd                    # Extract
xmeta -x -in=~/.ais0/.ais.emd -out=/tmp/emd.txt  # Extract to file
```
