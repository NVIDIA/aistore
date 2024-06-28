## Virtual directories

Unlike hierarchical POSIX, object storage is flat, treating forward slash ('/') in object names as simply another symbol.

But that's not the entire truth. The other part of it is that user may want to operate (list, train, copy, transform, etc.)
on a subset of objects in a dataset that, for the lack of better word, looks _exactly_ like a directory.

In fact, user often wants to do exactly that.
Train, for instance, on all audio files under `en_es_synthetic/v1/train/`, or similar.

> In object storages, the term for quote/unquote "what looks like a directory" is _virtual directory_ or _synthetic directory_.

The motivation may become clearer if I say that the entire real-life dataset
contains many millions of objects and numerous _virtual directories_, including the aforementioned `en_es_synthetic/v1/train/`.

Needless to say, aistore provides for all of that, and more. There is a certainty subtlety, however, that makes sense to illustrate on examples.

## But first, the rules

* normally, remote backends do not return _virtual directories_, with two exceptions:
  - `list-objects` operation is non-recursive (API `apc.LsNoRecursion` in the control message, CLI `--nr` switch);
  - the bucket in question contains some sort of special directory that shows up anyway (e.g. bucket inventory).
* `list-objects` will always return _virtual directories_, assuming:
  - the corresponding backend's response includes those (see above), and
  - user does not specify `apc.LsNoDirs` (CLI `--no-dirs`)
* the output is always sorted alphanumerically, directories-first

## Examples

### Show everything that has a certain prefix

```console
$ ais ls s3://speech --prefix .inventory
NAME                                                                      SIZE            CACHED
.inventory/speech/data/
.inventory/speech/2024-05-31T01-00Z/manifest.checksum                     33B             no
.inventory/speech/2024-05-31T01-00Z/manifest.json                         406B            no
.inventory/speech/data/985fc9cb-5957-4fc8-b26d-092685a747e8.csv.gz        54.14MiB        no
.inventory/speech/data/9dac8de5-cff9-432c-9663-b054ae5ce357.csv.gz        54.14MiB        no
.inventory/speech/hive/dt=2024-05-30-01-00/symlink.txt                    85B             no
.inventory/speech/hive/dt=2024-05-31-01-00/symlink.txt                    85B             no
```

### Same as above, without directories

```console
$ ais ls s3://speech --prefix .inventory --no-dirs
NAME                                                                      SIZE            CACHED
.inventory/speech/2024-05-31T01-00Z/manifest.checksum                     33B             no
.inventory/speech/2024-05-31T01-00Z/manifest.json                         406B            no
.inventory/speech/data/985fc9cb-5957-4fc8-b26d-092685a747e8.csv.gz        54.14MiB        no
.inventory/speech/data/9dac8de5-cff9-432c-9663-b054ae5ce357.csv.gz        54.14MiB        no
.inventory/speech/hive/dt=2024-05-30-01-00/symlink.txt                    85B             no
.inventory/speech/hive/dt=2024-05-31-01-00/symlink.txt                    85B             no
```

### Show dataset structure at a certain nested depth

```console
$ ais ls s3://speech --prefix .inventory/speech/ --nr
NAME                                       SIZE    CACHED
.inventory/speech/2024-05-31T01-00Z/
.inventory/speech/data/
.inventory/speech/hive/
```

### Virtual directories and data at a certain level

```console
$ ais ls s3://speech --prefix .inventory/speech/data/ --nr
NAME                                                                      SIZE            CACHED
.inventory/speech/data/
.inventory/speech/data/985fc9cb-5957-4fc8-b26d-092685a747e8.csv.gz        54.14MiB        no
.inventory/speech/data/9dac8de5-cff9-432c-9663-b054ae5ce357.csv.gz        54.14MiB        no
```
