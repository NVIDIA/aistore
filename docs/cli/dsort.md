---
layout: post
title: DSORT
permalink: /docs/cli/dsort
redirect_from:
 - /cli/dsort.md/
 - /docs/cli/dsort.md/
---

# Start, Stop, and monitor distributed parallel sorting (dSort)

For background and in-depth presentation, please see this [document](/docs/dsort.md).

- [Usage](#usage)
- [Example](#example)
- [Generate Shards](#generate-shards)
- [Start dSort job](#start-dsort-job)
- [Show dSort jobs and job status](#show-dsort-jobs-and-job-status)
- [Stop dSort job](#stop-dsort-job)
- [Remove dSort job](#remove-dsort-job)
- [Wait for dSort job](#wait-for-dsort-job)


## Usage

`ais dsort [command options] [JSON_SPECIFICATION|YAML_SPECIFICATION|-] [SRC_BUCKET] [DST_BUCKET]`

```console
$ ais dsort --help
NAME:
   ais dsort - (alias for "job start dsort") start dsort job
   Required parameters:
              - input_bck: source bucket (used as both source and destination if the latter not specified)
              - input_format: (see docs and examples below)
              - output_format: (ditto)
              - output_shard_size: (as the name implies)
   E.g. inline JSON spec:
                $ ais start dsort '{
                  "extension": ".tar",
                  "input_bck": {"name": "dsort-testing"},
                  "input_format": {"template": "shard-{0..9}"},
                  "output_shard_size": "200KB",
                  "description": "pack records into categorized shards",
                  "order_file": "http://website.web/static/order_file.txt",
                  "order_file_sep": " "
                }'
   E.g. inline YAML spec:
                $ ais start dsort -f - <<EOM
                  extension: .tar
                  input_bck:
                      name: dsort-testing
                  input_format:
                      template: shard-{0..9}
                  output_format: new-shard-{0000..1000}
                  output_shard_size: 10KB
                  description: shuffle shards from 0 to 9
                  algorithm:
                      kind: shuffle
                  EOM
   Tip: use '--dry-run' to see the results without making any changes
   Tip: use '--verbose' to print the spec (with all its parameters including applied defaults)
   See also: docs/dsort.md, docs/cli/dsort.md, and ais/test/scripts/dsort*

USAGE:
   ais dsort [command options] [JSON_SPECIFICATION|YAML_SPECIFICATION|-] [SRC_BUCKET] [DST_BUCKET]

OPTIONS:
   --file value, -f value  path to JSON or YAML job specification
   --verbose, -v           verbose
   --help, -h              show help
```

## Example

This example simply runs [ais/test/scripts/dsort-ex1-spec.json](https://github.com/NVIDIA/aistore/blob/master/ais/test/scripts/dsort-ex1-spec.json) specification. The source and destination buckets - ais://src and ais://dst, respectively - must exist.

Further, the source buckets must have at least 10 shards with names that match `input_format` (see below).

Notice the `-v` (`--verbose`) switch as well.

```console
$ ais start dsort ais://src ais://dst -f ais/test/scripts/dsort-ex1-spec.json --verbose
PROPERTY                         VALUE
algorithm.content_key_type       -
algorithm.decreasing             false
algorithm.extension              -
algorithm.kind                   alphanumeric
algorithm.seed                   -
create_concurrency_max_limit     0
description                      sort shards alphanumerically
dry_run                          false
dsorter_type                     -
extension                        .tar
extract_concurrency_max_limit    0
input_bck                        ais://src
input_format.objnames            -
input_format.template            shard-{0..9}
max_mem_usage                    -
order_file                       -
order_file_sep                   \t
output_bck                       ais://dst
output_format                    new-shard-{0000..1000}
output_shard_size                10KB

Config override:                 none

srt-M8ld-VU_i
```

## Generate Shards

`ais archive gen-shards "BUCKET/TEMPLATE.EXT"`

Put randomly generated shards into a bucket. The main use case for this command is dSort testing.
[Further reference for this command can be found here.](archive.md#generate-shards)

## Start dSort job

`ais start dsort JOB_SPEC` or `ais start dsort -f <PATH_TO_JOB_SPEC>`

Start new dSort job with the provided specification.
Specification should be provided by either argument or `-f` flag - providing both argument and flag will result in error.
Upon creation, `JOB_ID` of the job is returned - it can then be used to abort it or retrieve metrics.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--file, -f` | `string` | Path to file containing JSON or YAML job specification. Providing `-` will result in reading from STDIN | `""` |

The following table describes JSON/YAML keys which can be used in the specification.

| Key | Type | Description | Required | Default |
| --- | --- | --- | --- | --- |
| `extension` | `string` | extension of input and output shards (either `.tar`, `.tgz` or `.zip`) | yes | |
| `input_format.template` | `string` | name template for input shard | yes | |
| `output_format` | `string` | name template for output shard | yes | |
| `input_bck.name` | `string` | bucket name where shards objects are stored | yes | |
| `input_bck.provider` | `string` | bucket backend provider, see [docs](/docs/providers.md) | no | `"ais"` |
| `output_bck.name` | `string` | bucket name where new output shards will be saved | no | same as `input_bck.name` |
| `output_bck.provider` | `string` | bucket backend provider, see [docs](/docs/providers.md) | no | same as `input_bck.provider` |
| `description` | `string` | description of dSort job | no | `""` |
| `output_shard_size` | `string` | size (in bytes) of the output shard, can be in form of raw numbers `10240` or suffixed `10KB` | yes | |
| `algorithm.kind` | `string` | determines which sorting algorithm dSort job uses, available are: `"alphanumeric"`, `"shuffle"`, `"content"` | no | `"alphanumeric"` |
| `algorithm.decreasing` | `bool` | determines if the algorithm should sort the records in decreasing or increasing order, used for `kind=alphanumeric` or `kind=content` | no | `false` |
| `algorithm.seed` | `string` | seed provided to random generator, used when `kind=shuffle` | no | `""` - `time.Now()` is used |
| `algorithm.extension` | `string` | content of the file with provided extension will be used as sorting key, used when `kind=content` | yes (only when `kind=content`) |
| `algorithm.content_key_type` | `string` | content key type; may have one of the following values: "int", "float", or "string"; used exclusively with `kind=content` sorting | yes (only when `kind=content`) |
| `order_file` | `string` | URL to the file containing external key map (it should contain lines in format: `record_key[sep]shard-%d-fmt`) | yes (only when `output_format` not provided) | `""` |
| `order_file_sep` | `string` | separator used for splitting `record_key` and `shard-%d-fmt` in the lines in external key map | no | `\t` (TAB) |
| `max_mem_usage` | `string` | limits the amount of total system memory allocated by both dSort and other running processes. Once and if this threshold is crossed, dSort will continue extracting onto local drives. Can be in format 60% or 10GB | no | same as in `/deploy/dev/local/aisnode_config.sh` |
| `extract_concurrency_max_limit` | `int` | limits maximum number of concurrent shards extracted per disk | no | (calculated based on different factors) ~50 |
| `create_concurrency_max_limit` | `int` | limits maximum number of concurrent shards created per disk| no | (calculated based on different factors) ~50 |

There's also the possibility to override some of the values from global `distributed_sort` config via job specification.
All values are optional - if empty, the value from global `distributed_sort` config will be used.
For more information refer to [configuration](/docs/configuration.md).

| Key | Type | Description |
| --- | --- | --- |
| `duplicated_records` | `string` | what to do when duplicated records are found: "ignore" - ignore and continue, "warn" - notify a user and continue, "abort" - abort dSort operation |
| `missing_shards` | `string` | what to do when missing shards are detected: "ignore" - ignore and continue, "warn" - notify a user and continue, "abort" - abort dSort operation |
| `ekm_malformed_line` | `string`| what to do when extraction key map notices a malformed line: "ignore" - ignore and continue, "warn" - notify a user and continue, "abort" - abort dSort operation |
| `ekm_missing_key` | `string` | what to do when extraction key map have a missing key: "ignore" - ignore and continue, "warn" - notify a user and continue, "abort" - abort dSort operation |
| `dsorter_mem_threshold` | `string`| minimum free memory threshold which will activate specialized dsorter type which uses memory in creation phase - benchmarks shows that this type of dsorter behaves better than general type |

### Examples

#### Sort records inside the shards

Command defined below starts (alphanumeric) sorting job with extended metrics for **input** shards with names `shard-0.tar`, `shard-1.tar`, ..., `shard-9.tar`.
Each of the **output** shards will have at least `10240` bytes (`10KB`) and will be named `new-shard-0000.tar`, `new-shard-0001.tar`, ...

Assuming that `dsort_spec.json` contains:

```json
{
    "extension": ".tar",
    "input_bck": {"name": "dsort-testing"},
    "input_format": {
	template: "shard-{0..9}"
    },
    "output_format": "new-shard-{0000..1000}",
    "output_shard_size": "10KB",
    "description": "sort shards from 0 to 9",
    "algorithm": {
        "kind": "alphanumeric"
    },
}
```

You can start dSort job with:

```console
$ ais start dsort -f dsort_spec.json
JGHEoo89gg
```

#### Shuffle records

Command defined below starts basic shuffle job for **input** shards with names `shard-0.tar`, `shard-1.tar`, ..., `shard-9.tar`.
Each of the **output** shards will have at least `10240` bytes (`10KB`) and will be named `new-shard-0000.tar`, `new-shard-0001.tar`, ...

```console
$ ais start dsort -f - <<EOM
extension: .tar
input_bck:
    name: dsort-testing
input_format:
    template: shard-{0..9}
output_format: new-shard-{0000..1000}
output_shard_size: 10KB
description: shuffle shards from 0 to 9
algorithm:
    kind: shuffle
EOM
JGHEoo89gg
```

#### Pack records into shards with different categories - EKM (External Key Map)

One of the key features of the dSort is that user can specify the exact mapping from the record key to the output shard.
To use this feature `output_format` should be empty and `order_file`, as well as `order_file_sep`, must be set.
The output shards will be created with provided format which must contain mandatory `%d` which is required to enumerate the shards.

Assuming that `order_file` (URL: `http://website.web/static/order_file.txt`) has content:

```
cat_0.txt shard-cats-%d
cat_1.txt shard-cats-%d
...
dog_0.txt shard-dogs-%d
dog_1.txt shard-dogs-%d
...
car_0.txt shard-car-%d
car_1.txt shard-car-%d
...
```

or if `order_file` (URL: `http://website.web/static/order_file.json`, notice `.json` extension) and has content:

```json
{
  "shard-cats-%d": [
    "cat_0.txt",
    "cat_1.txt",
    ...
  ],
  "shard-dogs-%d": [
    "dog_0.txt",
    "dog_1.txt",
    ...
  ],
  "shard-car-%d": [
    "car_0.txt",
    "car_1.txt",
    ...
  ],
  ...
}
```

and content of the **input** shards looks more or less like this:

```
shard-0.tar:
- cat_0.txt
- dog_0.txt
- car_0.txt
...
shard-1.tar:
- cat_1.txt
- dog_1.txt
- car_1.txt
...
```

You can run:

```console
$ ais start dsort '{
    "extension": ".tar",
    "input_bck": {"name": "dsort-testing"},
    "input_format": {"template": "shard-{0..9}"},
    "output_shard_size": "200KB",
    "description": "pack records into categorized shards",
    "order_file": "http://website.web/static/order_file.txt",
    "order_file_sep": " "
}'
JGHEoo89gg
```

After the run, the **output** shards will look more or less like this (the number of records in given shard depends on provided `output_shard_size`):

```
shard-cats-0.tar:
- cat_1.txt
- cat_2.txt
shard-cats-1.tar:
- cat_3.txt
- cat_4.txt
...
shard-dogs-0.tar:
- dog_1.txt
- dog_2.txt
...
```

## Show dSort jobs and job status

`ais show job dsort [JOB_ID]`

Retrieve the status of the dSort with provided `JOB_ID` which is returned upon creation.
Lists all dSort jobs if the `JOB_ID` argument is omitted.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Regex for the description of dSort jobs | `""` |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds). E.g.:  `--refresh 2s`| ` ` |
| `--verbose, -v` | `bool` | Show detailed metrics | `false` |
| `--log` | `string` | Path to file where the metrics will be saved (does not work with progress bar) | `/tmp/dsort_run.txt` |
| `--json, -j` | `bool` | Show only json metrics | `false` |

### Examples

#### Show dSort jobs with description matching provided regex

Shows all dSort jobs with descriptions starting with `sort ` prefix.

```console
$ ais show job dsort --regex "^sort (.*)"
JOB ID		 STATUS		 START		 FINISH			 DESCRIPTION
nro_Y5h9n	 Finished	 03-16 11:39:07	 03-16 11:39:07 	 sort shards from 0 to 9
Key_Y5h9n	 Finished	 03-16 11:39:23	 03-16 11:39:23 	 sort shards from 10 to 19
enq9Y5Aqn	 Finished	 03-16 11:39:34	 03-16 11:39:34 	 sort shards from 20 to 29
```

#### Save metrics to log file

Save newly fetched metrics of the dSort job with ID `5JjIuGemR` to `/tmp/dsort_run.txt` file every `500` milliseconds

```console
$ ais show job dsort 5JjIuGemR --refresh 500ms --log "/tmp/dsort_run.txt"
DSort job has finished successfully in 21.948806ms:
  Longest extraction:	1.49907ms
  Longest sorting:	8.288299ms
  Longest creation:	4.553µs
```

#### Show only json metrics

```console
$ ais show job dsort 5JjIuGemR --json
{
  "825090t8089": {
    "local_extraction": {
      "started_time": "2020-05-28T09:53:42.466267891-04:00",
      "end_time": "2020-05-28T09:53:42.50773835-04:00",
      ....
     },
     ....
  },
  ....
}
```

#### Show only json metrics filtered by daemon id

```console
$ ais show job dsort 5JjIuGemR 766516t8087 --json
{
  "766516t8087": {
    "local_extraction": {
      "started_time": "2020-05-28T09:53:42.466267891-04:00",
      "end_time": "2020-05-28T09:53:42.50773835-04:00",
      ....
     },
     ....
  }
}
```

#### Using jq to filter out the json formatted metric output

Show running status of meta sorting phase for all targets.

```console
$ ais show job dsort 5JjIuGemR --json | jq .[].meta_sorting.running
false
false
true
false
```

Show created shards in each target along with the target ids.

```console
$ ais show job dsort 5JjIuGemR --json | jq 'to_entries[] | [.key, .value.shard_creation.created_count]'
[
  "766516t8087",
  "189"
]
[
  "710650t8086",
  "207"
]
[
  "825090t8089",
  "211"
]
[
  "743838t8088",
  "186"
]
[
  "354275t8085",
  "207"
]
```


## Stop dSort job

`ais stop dsort JOB_ID`

Stop the dSort job with given `JOB_ID`.

## Remove dSort job

`ais job rm dsort JOB_ID`

Remove the finished dSort job with given `JOB_ID` from the job list.

## Wait for dSort job

`ais wait dsort JOB_ID`

or, same:

`ais wait JOB_ID`

Wait for the dSort job with given `JOB_ID` to finish.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds) | `1s` |
| `--progress` | `bool` | Displays progress bar | `false` |
