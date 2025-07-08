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

`ais dsort [SRC_BUCKET] [DST_BUCKET]` --spec [JSON_SPECIFICATION|YAML_SPECIFICATION|-] [command options]

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
                  "ekm_file": "http://website.web/static/ekm_file.txt",
                  "ekm_file_sep": " "
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
   ais dsort [SRC_BUCKET] [DST_BUCKET] --spec [JSON_SPECIFICATION|YAML_SPECIFICATION|-] [command options]

OPTIONS:
   --spec value, -f value  path to JSON or YAML request specification
   --verbose, -v           verbose
   --help, -h              show help
```

## Example

This example simply runs [ais/test/scripts/dsort-ex1-spec.json](https://github.com/NVIDIA/aistore/blob/main/ais/test/scripts/dsort-spec1.json) specification. The source and destination buckets - ais://src and ais://dst, respectively - must exist.

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
ekm_file                       -
ekm_file_sep                   \t
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

`ais start dsort --spec JOB_SPEC` or `ais start dsort -f <PATH_TO_JOB_SPEC>`

Start new dSort job with the provided specification.
Specification should be provided by either argument or `-f` flag - providing both argument and flag will result in error.
Upon creation, `JOB_ID` of the job is returned - it can then be used to abort it or retrieve metrics.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--spec, -f` | `string` | Path to JSON or YAML specification. Providing `-` will result in reading from STDIN | `""` |

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
| `ekm_file` | `string` | URL to the file containing external key map (it should contain lines in format: `record_key[sep]shard-%d-fmt`) | yes (only when `output_format` not provided) | `""` |
| `ekm_file_sep` | `string` | separator used for splitting `record_key` and `shard-%d-fmt` in the lines in external key map | no | `\t` (TAB) |
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
      "template": "shard-{0..9}"
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
To use this feature `output_format` should be empty and `ekm_file`, as well as `ekm_file_sep`, must be set.
The output shards will be created with provided [template format](/docs/batch.md#operations-on-multiple-selected-objects).

Assuming that `ekm_file` (URL: `http://website.web/static/ekm_file.txt`) has content:

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

or if `ekm_file` (URL: `http://website.web/static/ekm_file.json`, notice `.json` extension) and has content:

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

or, you can also use regex as the record identifier. The `ekm_file` can contain regex patterns as keys to match multiple records that fit the regex pattern to provided format.

```json
{
  "shard-cats-%d": [
    "cat_[0-9]+\\.txt"
  ],
  "shard-dogs-%d": [
    "dog_[0-9]+\\.txt"
  ],
  "shard-car-%d": [
    "car_[0-9]+\\.txt"
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
$ ais start dsort --spec '{
    "extension": ".tar",
    "input_bck": {"name": "dsort-testing"},
    "input_format": {"template": "shard-{0..9}"},
    "output_shard_size": "200KB",
    "description": "pack records into categorized shards",
    "ekm_file": "http://website.web/static/ekm_file.txt",
    "ekm_file_sep": " "
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

EKM also supports [template syntax](/docs/batch.md#operations-on-multiple-selected-objects) to express output shard names.
For example, if `ekm_file` has content:

```json
{
  "shard-{0..100..3}-cats": [
    "cat_0.txt",
    "cat_1.txt",
    "cat_3.txt",
    "cat_4.txt",
    "cat_5.txt",
    "cat_6.txt",
    ...
  ],
  "shard-@00001-gap-@100-dogs": [ 
    "dog_0.txt",
    "dog_1.txt",
    ...
  ],
  "shard-%06d-cars": [
    "car_0.txt",
    "car_1.txt",
    ...
  ],
  ...
}
```

After running `dsort`, the output would be look like this:

```
shard-0-cats.tar:
- cat_0.txt
- cat_1.txt
shard-3-cats.tar:
- cat_2.txt
- cat_3.txt
shard-6-cats.tar:
- cat_4.txt
- cat_5.txt
...
shard-00001-gap-001-dogs.tar:
- dog_0.txt
- dog_1.txt
shard-00001-gap-002-dogs.tar:
- dog_2.txt
- dog_3.txt
...
shard-1-cars.tar:
- car_0.txt
- car_1.txt
shard-2-cars.tar:
- car_2.txt
- car_3.txt
...
```

## Show dSort jobs and job status

`ais show job dsort [JOB_ID]`

Retrieve the status of the dSort with provided `JOB_ID` which is returned upon creation.
Lists all dSort jobs if the `JOB_ID` argument is omitted.

### Options

```console
$ ais show job dsort --help

NAME:
   ais show job - Show running and/or finished jobs,
     e.g.:
     - show job tco-cysbohAGL              - show a given (multi-object copy/transform) job identified by its unique ID;
     - show job copy-listrange             - show all running multi-object copies;
     - show job copy-objects               - same as above (using display name);
     - show job copy                       - show all copying jobs including both bucket-to-bucket and multi-object;
     - show job copy-objects --all         - show both running and already finished (or stopped) multi-object copies;
     - show job list                       - show all running list-objects jobs;
     - show job ls                         - same as above;
     - show job ls --refresh 10            - same as above with periodic _refreshing_ every 10 seconds;
     - show job ls --refresh 10 --count 4  - same as above but only for the first four 10-seconds intervals;
     - show job prefetch-listrange         - show all running prefetch jobs;
     - show job prefetch                   - same as above;
     - show job prefetch --refresh 1m      - show all running prefetch jobs at 1 minute intervals (until Ctrl-C);
     - show job evict                      - all running bucket and/or data evicting jobs;
     - show job --all                      - show absolutely all jobs, running and finished.

USAGE:
   ais show job [NAME] [JOB_ID] [NODE_ID] [BUCKET] [command options]

OPTIONS:
   --all             Include all jobs: running, finished, and aborted
   --count value     Used together with '--refresh' to limit the number of generated reports, e.g.:
                      '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --date-time       Override the default hh:mm:ss (hours, minutes, seconds) time format - include calendar date as well
   --json, -j        JSON input/output
   --log value       Filename to log metrics (statistics)
   --no-headers, -H  Display tables without headers
   --progress        Show progress bar(s) and progress of execution in real time
   --refresh value   Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --regex value     Regular expression to select jobs by name, kind, or description, e.g.: --regex "ec|mirror|elect"
   --units value     Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --verbose, -v     Show extended statistics
   --help, -h        Show help
```

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
Dsort job has finished successfully in 21.948806ms:
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

```console
$ ais wait --help

NAME:
   ais wait - (alias for "job wait") wait for a specific batch job to complete (press <TAB-TAB> to select, '--help' for more options)

USAGE:
   ais wait [NAME] [JOB_ID] [NODE_ID] [BUCKET] [command options]

OPTIONS:
   --progress       Show progress bar(s) and progress of execution in real time
   --refresh value  Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --timeout value  Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --help, -h       Show help
```
