# Initial Sharder

Initial Sharding utility (`ishard`) is intended to create well-formed [WebDataset-formatted](https://github.com/webdataset/webdataset?tab=readme-ov-file#the-webdataset-format) shards from the original dataset. 

Note that original ML datasets will have an arbitrary structure, a massive number of small files and/or very large files, and deeply nested directories. Notwithstanding, there's almost always the need to batch associated files (that constitute computable samples) together and maybe pre-shuffle them for immediate consumption by a model.

Hence, `ishard`.

## Background

At the lowest level, a shard is any `.tar`, `.tgz` or `.tar.gz`, `.zip`, or `.tar.lz4` formatted object. AIStore equally supports all these formats, which share one common property: all 4 (four) are iterable serialized archives storing original file names and metadata. AIStore provides APIs and CLI to read, write (and append), and list existing shards.

> All sharding formats are equally supported across the entire set of AIS APIs. For instance, `list-objects` API supports "opening" shards and including contents of archived directories into generated result sets. Clients can run concurrent multi-object (source bucket => destination bucket) transactions to _en masse_ generate new archives from [selected](/docs/batch.md) subsets of files, and more.

On top of these basic capabilities, there's the already mentioned [WebDataset](https://github.com/webdataset/webdataset?tab=readme-ov-file#the-webdataset-format) convention which, in the most simple terms, requires that computable _samples_ do not cross a shard boundaries.

To give a quick example, `a/b/c/toyota.jpeg` and `a/b/c/toyota.json` from an original dataset are considered part of the same sample (a.k.a. _record_) and must be stored together in one shard.

## Terms

- **File**: Represents individual files in the source bucket. The file names are substituted into sample keys based on a configurable rule called `sample_key_pattern`.
- **Sample**: Groups multiple files with the same sample key into a single structure. After `ishard` execution, samples are indivisible and will always be included together in the same output shard.
- **Shard**: Represents the output of `ishard`, which is a collection of files archived in `.tar`, `.tgz` or `.tar.gz`, `.zip`, or `.tar.lz4` formats.

## CLI Parameters

- `-sample_key_pattern`: The pattern used to substitute source file names to sample keys. This ensures that objects with the same sample key are always merged into the same output shard.
   - `-sample_key_pattern="base_filename"`: The default option. Extracts and uses only the base filename as the sample key to merge. Removes all directory paths and extensions.
   - `-sample_key_pattern="full_name"`: Performs no substitution, using the entire file name without extension as the sample key.
   - `-sample_key_pattern="collapse_all_dir"`: Removes all '/' characters from the file name, using the resulting string as the sample key.
   - `-sample_key_pattern="custom_regex"`: Applies a custom regex pattern to substitute the file names to sample keys for your specific requirements.
- `-max_shard_size`: The desired size of each output shard in bytes. Default is `1024000`.
- `-src_bck`: The source bucket name or URI.
- `-dst_bck`: The destination bucket name or URI.
- `-shard_template`: The template used for generating output shards. Accepts Bash, Fmt, or At formats.
   - `-shard_template="prefix-{0000..4096..8}-suffix"`: generate output shards `prefix-0000-suffix`, `prefix-0008-suffix`, `prefix-00016-suffix`, and so on.
   - `-shard_template="prefix-%06d-suffix"`: generate output shards `prefix-000000-suffix`, `prefix-000001-suffix`, `prefix-000002-suffix`, and so on.
   - `-shard_template="prefix-@00001-gap-@100-suffix"`: generate output shards `prefix-00001-gap-001-suffix`, `prefix-00001-gap-002-suffix`, and so on.
- `-ext`: The extension used for generating output shards.
- `-sample_exts`: A comma-separated list of extensions that should exists in the dataset. Also see `missing_extension_action`.
- `-missing_extension_action`: Action to take when an extension is missing at any sample: `abort` | `warn` | `ignore`, if `sample_exts` is set.
- `-collapse`: If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.
- `-progress`: If true, display the progress of processing objects in the source bucket.
- `-dry_run`: If set, only shows the layout of resulting output shards without actually executing archive jobs. Use 'show_keys' to include sample keys.

## Initial Setup

**Build the Package:**

```sh
$ cd cmd/ishard
$ go mod tidy
$ go build -o ishard .
```

## Sample Usages

Let's say we have an ImageNet dataset with the following layout on the local file system:

```sh
$ tree ./ImageNet

ImageNet/
├── Annotations
│   ├── n00000333
│   │   ├── n00000333_01.xml
│   │   └── n00000333_02.xml
│   ├── n00000369
│   │   ├── n00000369_01.xml
│   │   ├── n00000369_02.xml
│   │   ├── n00000369_03.xml
│   │   └── n00000369_04.xml
│   ├── n00000565
│   └── ...
└── Data
    ├── train
    │   ├── n00000333
    │   │   ├── n00000333_01.JPEG
    │   │   ├── n00000333_02.JPEG
    │   │   ├── n00000333_02.loss
    │   │   ├── n00000333_03.JPEG
    │   │   └── n00000333_03.loss
    │   ├── n00000369
    │   │   ├── n00000369_01.JPEG
    │   │   ├── n00000369_01.loss
    │   │   ├── n00000369_02.JPEG
    │   │   ├── n00000369_02.loss
    │   │   ├── n00000369_03.JPEG
    │   │   └── n00000369_04.JPEG
    │   ├── n00000565
    │   └── ...
    └── val
        ├── n00000333
        │   ├── ILSVRC2012_val_00001851.JPEG
        │   ├── ILSVRC2012_val_00006595.JPEG
        │   ├── ILSVRC2012_val_00007175.JPEG
        │   ├── ILSVRC2012_val_00012920.JPEG
        │   └── ILSVRC2012_val_00021981.JPEG
        ├── n00000369
        │   ├── ILSVRC2012_val_00001436.JPEG
        │   ├── ILSVRC2012_val_00016182.JPEG
        |   └── ...
        └── ...
```

To put the entire directory into a bucket using CLI:

```sh
$ ais bucket create ais://ImageNet
$ ais put "./ImageNet" ais://ImageNet --recursive
$ ais bucket ls ais://ImageNet | less

NAME                                                             SIZE            
ImageNet/Annotations/n00000333/n00000333_01.xml                  100B            
ImageNet/Annotations/n00000333/n00000333_02.xml                  100B            
ImageNet/Annotations/n00000369/n00000369_01.xml                  100B            
ImageNet/Annotations/n00000369/n00000369_02.xml                  100B            
...
ImageNet/Data/train/n00000333/n00000333_01.JPEG                  30.00KiB        
ImageNet/Data/train/n00000333/n00000333_02.JPEG                  30.00KiB        
ImageNet/Data/train/n00000333/n00000333_02.loss                  8B            
ImageNet/Data/train/n00000333/n00000333_03.JPEG                  30.00KiB        
ImageNet/Data/train/n00000333/n00000333_03.loss                  8B            
ImageNet/Data/train/n00000369/n00000369_01.JPEG                  30.00KiB        
ImageNet/Data/train/n00000369/n00000369_01.loss                  8B            
ImageNet/Data/train/n00000369/n00000369_02.JPEG                  30.00KiB        
ImageNet/Data/train/n00000369/n00000369_02.loss                  8B            
...
ImageNet/Data/val/n00000333/ILSVRC2012_val_00001851.JPEG         30.00KiB        
ImageNet/Data/val/n00000333/ILSVRC2012_val_00006595.JPEG         30.00KiB        
ImageNet/Data/val/n00000333/ILSVRC2012_val_00007175.JPEG         30.00KiB        
...
```

### Correct Usages

> Sharding a large dataset can take hours to complete. Therefore, it is highly recommended to first perform a `dry-run` of your `ishard` command to ensure it performs the desired sample key substitution and produces the expected output shard composition. See the Dry Run section below for more details.

1. **Execute `ishard` with default sample key**:

   When `sample_key_pattern` is not specified, `ishard` uses `base_file_name` as sample key. This means that source files with the same base name (without extensions) will be sharded together. For example, the following three files:
   - `ImageNet/Annotations/n00000333/n00000333_02.xml`
   - `ImageNet/Data/train/n00000333/n00000333_02.JPEG`
   - `ImageNet/Data/train/n00000333/n00000333_02.loss` 
   
   They have the same base name `n00000333_02`, and therefore will always present in the same output shard, regardless of `max_shard_size` value.
   ```sh
   ./ishard -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out

   $ ais archive ls ais://ImageNet-out | less

   NAME                                                                          SIZE            
   shard-0.tar                                                                   1.00MiB       
      shard-0.tar/ImageNet/Annotations/n00000333/n00000333_01.xml                100B            
      shard-0.tar/ImageNet/Annotations/n00000333/n00000333_02.xml                100B            
      ...
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_02.JPEG                30.00KiB        
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_02.loss                100B            
      shard-0.tar/ImageNet/Data/train/n00000369/n00000369_01.JPEG                30.00KiB        
      ...
   shard-1.tar                                                                   129.00KiB       
   ...
   ```

2. **Execute `ishard` with `full_name` as sample key**:

   When `sample_key_pattern` is set to `full_name`, source files with the same full name (without extensions) will be sharded together. For example, the following two files:
   - `ImageNet/Data/train/n00005739/n00005739_01.JPEG`
   - `ImageNet/Data/train/n00005739/n00005739_01.loss`
   
   They have the same full name `ImageNet/Data/train/n00005739/n00005739_01` and therefore will always present in the same output shard. But file `ImageNet/Annotations/n00005739/n00005739_01.xml` has different full name `ImageNet/Annotations/n00005739/n00005739_01`, and therefore will be sharded separately.
   ```sh
   $ ./ishard -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out --sample_key_pattern="full_name"

   NAME                                                            SIZE            
   ...
   shard-059.tar                                                   200B
      shard-059/ImageNet/Annotations/n00005739/n00005739_01.xml    100B
      shard-059/ImageNet/Annotations/n00005739/n00005739_02.xml    100B
   ...
   shard-097.tar                                                   90.20KiB
      shard-097/ImageNet/Data/train/n00005739/n00005739_01.JPEG    30.00KiB
      shard-097/ImageNet/Data/train/n00005739/n00005739_01.loss    100B
      shard-097/ImageNet/Data/train/n00005739/n00005739_02.JPEG    30.00KiB
      shard-097/ImageNet/Data/train/n00005739/n00005739_03.JPEG    30.00KiB
      shard-097/ImageNet/Data/train/n00005739/n00005739_03.loss    100B
   shard-098.tar                                                   60.00KiB
   ...
   ```

   By default, `ishard` ensures that files with different virtual directory structure (after applying `sample_key_pattern`) won't present in the same output shard. In other words, `ishard` maintains clear boundaries between files that belong to different virtual directory, even if some output shard's size doesn't reached the `max_shard_size`. As shown in the example above, there are only two objects in the `shard-059.tar` output shard regardless of the `max_shard_size` value, since they are the only two files under their virtual directory structure.

   To disable this default setting and compact each output shard's size closer to `max_shard_size`, regardless of virtual directories, you can specify `-collapse` flag. This will to flatten samples into its parent virtual directory if their overall size doesn't reach `max_shard_size`.

   ```sh
   $ ./ishard -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out --sample_key_pattern="full_name" -collapse

   NAME                                                                    SIZE            
   shard-0.tar                                                             1.03MiB
      shard-0/ImageNet/Data/train/n00003215/n00003215_01.JPEG              30.00KiB
      shard-0/ImageNet/Data/train/n00003215/n00003215_02.JPEG              30.00KiB
      shard-0/ImageNet/Data/train/n00003215/n00003215_02.loss              100B
      shard-0/ImageNet/Data/train/n00003215/n00003215_03.JPEG              30.00KiB
   ...
   shard-6.tar                                                             1.03MiB
      shard-6/ImageNet/Data/val/n00015250/ILSVRC2012_val_00001158.JPEG     30.00KiB
      shard-6/ImageNet/Data/val/n00015250/ILSVRC2012_val_00007151.JPEG     30.00KiB
      shard-6/ImageNet/Data/val/n00015250/ILSVRC2012_val_00017846.JPEG     30.00KiB
      shard-6/ImageNet/Data/val/n00015250/ILSVRC2012_val_00020293.JPEG     30.00KiB
   ...
   shard-12.tar                                                            653.05KiB
      shard-12/ImageNet/Annotations/n00023258/n00023258_01.xml             100B
      shard-12/ImageNet/Annotations/n00023258/n00023258_02.xml             100B
      shard-12/ImageNet/Annotations/n00032644/n00032644_01.xml             100B
      shard-12/ImageNet/Annotations/n00032644/n00032644_02.xml             100B
      shard-12/ImageNet/Annotations/n00032644/n00032644_03.xml             100B
   ...
   ```

3. **Customized regex sample key:** You can also provide your own `sample_key_pattern` as regex for sample key substitution. For example, the following demonstrates how to only extract the last level of virtual directory name `n00000333` as sample key using custom regex `.*/([^/]+)/[^/]+$`.

   ```sh
   $ ./ishard -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out -progress --sample_key_pattern=".*/([^/]+)/[^/]+$"

   2024/07/11 11:34:26 `sample_key_pattern` .*/([^/]+)/[^/]+$ is not built-in (`base_file_name` | `full_name` | `collapse_all_dir`), compiled as custom regex.

   $ ais archive ls ais://ImageNet-out | less

   NAME                                                                   SIZE            
   shard-0.tar                                                            1.17MiB
      shard-0/ImageNet/Annotations/n00000333/n00000333_01.xml             100B
      shard-0/ImageNet/Annotations/n00000333/n00000333_02.xml             100B
      shard-0/ImageNet/Data/train/n00000333/n00000333_01.JPEG             30.00KiB
      shard-0/ImageNet/Data/train/n00000333/n00000333_02.JPEG             30.00KiB
      shard-0/ImageNet/Data/train/n00000333/n00000333_02.loss             100B
      shard-0/ImageNet/Data/train/n00000333/n00000333_03.JPEG             30.00KiB
      shard-0/ImageNet/Data/train/n00000333/n00000333_03.loss             100B
      shard-0/ImageNet/Data/val/n00000333/ILSVRC2012_val_00001851.JPEG    30.00KiB
      shard-0/ImageNet/Data/val/n00000333/ILSVRC2012_val_00006595.JPEG    30.00KiB
      shard-0/ImageNet/Data/val/n00000333/ILSVRC2012_val_00007175.JPEG    30.00KiB
      shard-0/ImageNet/Data/val/n00000333/ILSVRC2012_val_00012920.JPEG    30.00KiB
      shard-0/ImageNet/Data/val/n00000333/ILSVRC2012_val_00021981.JPEG    30.00KiB
      shard-0/ImageNet/Annotations/n00000369/n00000369_01.xml             100B
   ...
   ```

4. **Filter source files using prefix:** You can specify a prefix for the files to include in `ishard` using the `src_bck` parameter. For example, the following command specifies `Data` as the prefix in the source bucket, which includes only the files whose names start with `Data`.

   ```sh
   $ ./ishard -src_bck=ais://ImageNet/ImageNet/Data -dst_bck=ais://ImageNet-out

   $ ais archive ls ais://ImageNet-out | less

   NAME                                                                             SIZE            
   shard-0.tar                                                                      1.03MiB         
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_01.JPEG                  30.00KiB        
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_02.JPEG                  30.00KiB        
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_02.loss                  100B            
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_03.JPEG                  30.00KiB        
      shard-0.tar/ImageNet/Data/train/n00000333/n00000333_03.loss                  100B            
      shard-0.tar/ImageNet/Data/train/n00000369/n00000369_01.JPEG                  30.00KiB        
      shard-0.tar/ImageNet/Data/train/n00000369/n00000369_01.loss                  100B            
      shard-0.tar/ImageNet/Data/train/n00000369/n00000369_02.JPEG                  30.00KiB        
      shard-0.tar/ImageNet/Data/train/n00000369/n00000369_02.loss                  100B            
      shard-0.tar/ImageNet/Data/train/n00000369/n00000369_03.JPEG                  30.00KiB        
   ...
   ```

5. **Generate output shards name using template:** You can use various templates to generate output shards using `-shard_template`. For example: 

   ```sh
   $ ./ishard-cli -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out -shard_template="pre-{0000..8192..8}-suf"

   NAME                                                                            SIZE            
   pre-0000-suf.tar                                                                1.07MiB         
      pre-0000-suf.tar/ImageNet/Annotations/n00000333/n00000333_01.xml             100B            
      pre-0000-suf.tar/ImageNet/Annotations/n00000333/n00000333_02.xml             100B            
   ...
   pre-0008-suf.tar                                                                1.07MiB         
      pre-0008-suf.tar/ImageNet/Annotations/n00005864/n00005864_02.xml             100B            
      pre-0008-suf.tar/ImageNet/Annotations/n00007702/n00007702_01.xml             100B            
   ...
   pre-0016-suf.tar                                                                1.07MiB         
      pre-0016-suf.tar/ImageNet/Annotations/n00014536/n00014536_02.xml             100B            
      pre-0016-suf.tar/ImageNet/Annotations/n00015250/n00015250_01.xml             100B            
   ...
   ```

### Incorrect Usages

1. The number of generated output shards can't fit into specified `shard-template`.
   ```sh
   $ ./ishard -max_shard_size=256000 -src_bck=ais://sample -dst_bck=ais://sample-out -collapse --sample_key_pattern="base_filename" -shard_template="pre-{0000..50..8}-suf"

   Error: number of shards to be created exceeds expected number of shards (7)
   ```

2. Provides invalid regex `sample_key_pattern`.
   ```sh
   $ ./ishard -max_shard_size=256000 -src_bck=ais://sample -dst_bck=ais://sample-out -collapse --sample_key_pattern="(.*'" -shard_template="pre-{0000..8192..8}-suf"

   Invalid regex pattern: (.*'. Error: error parsing regexp: missing closing ): `(.*'`
   ```

## Dry Run

The `-dry_run` flag in the CLI parameters allows `ishard` to only print a preview of the output shards composition without performing the actual archiving tasks. This is especially useful when working with large datasets, where the full execution of `ishard` can take hours to complete.

```sh
$ ./ishard-cli -max_shard_size=102400 -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out --sample_key_pattern="base_file_name" -shard_template="pre-{0000..8192..8}-suf" -dry_run | less

pre-0000-suf.tar                                                        120.68KiB
    pre-0000-suf/ImageNet/Annotations/n00000333/n00000333_01.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000333/n00000333_01.JPEG        30.00KiB
    pre-0000-suf/ImageNet/Annotations/n00000333/n00000333_02.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000333/n00000333_02.JPEG        30.00KiB
    pre-0000-suf/ImageNet/Data/train/n00000333/n00000333_02.loss        100B
    pre-0000-suf/ImageNet/Annotations/n00000369/n00000369_01.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000369/n00000369_01.JPEG        30.00KiB
    pre-0000-suf/ImageNet/Data/train/n00000369/n00000369_01.loss        100B
    pre-0000-suf/ImageNet/Annotations/n00000369/n00000369_02.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000369/n00000369_02.JPEG        30.00KiB
    pre-0000-suf/ImageNet/Data/train/n00000369/n00000369_02.loss        100B
pre-0008-suf.tar                                                        120.59KiB
    pre-0008-suf/ImageNet/Annotations/n00000369/n00000369_03.xml        100B
...
```

You can also apply `-dry_run="show_keys"` to display the key of each group of samples after `sample_key_pattern` substitution. The string inside `[]` in the output represents the sample key of the sample to which the following files belong.

```sh
$ ./ishard-cli -max_shard_size=102400 -src_bck=ais://ImageNet -dst_bck=ais://ImageNet-out --sample_key_pattern="base_file_name" -shard_template="pre-{0000..8192..8}-suf" -dry_run="show_keys" | less

pre-0000-suf.tar                                                        120.68KiB
  [n00000333_01]                                                        
    pre-0000-suf/ImageNet/Annotations/n00000333/n00000333_01.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000333/n00000333_01.JPEG        30.00KiB
  [n00000333_02]                                                        
    pre-0000-suf/ImageNet/Annotations/n00000333/n00000333_02.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000333/n00000333_02.JPEG        30.00KiB
    pre-0000-suf/ImageNet/Data/train/n00000333/n00000333_02.loss        100B
  [n00000369_01]                                                        
    pre-0000-suf/ImageNet/Annotations/n00000369/n00000369_01.xml        100B
    pre-0000-suf/ImageNet/Data/train/n00000369/n00000369_01.JPEG        30.00KiB
    pre-0000-suf/ImageNet/Data/train/n00000369/n00000369_01.loss        100B
  [n00000369_02]                                                        
...
```

## Running the Tests

Test in Short Mode

```sh
go test -v -short
```

Test in Complete Mode

```sh
go test -v
```

Test in Debug Mode

```sh
go test -v -short -tags=debug
```

Test for a Specific Case

```sh
go test -v -short -tags=debug -run=TestIshardMaxShardSize
```

## TODO List

### MUST HAVE/DESIRABLE
- [X] Shard name patterns
   - [X] Utilize existing name template tools
- [X] goroutine
- [X] configurable record key, extensions
   - [X] upon missing extension in a record: (abort | warn | ignore)
- [X] dry run
- [ ] version 0.9 (github checksum, git cmd)
- [ ] go install
- [ ] debug build
- [X] allow user to specify source directories to include/exclude (achieved by prefix option)
- [ ] logging (timestamp, nlog)
- [ ] Large list of objects, need to swap MEM temporary
- [X] Long stress tests

### GOOD TO HAVE
- [X] progress bar (later)
- [ ] polling for completion of archive xactions
- [ ] integration into aistore (later)
- [ ] E2E testing from CLI
