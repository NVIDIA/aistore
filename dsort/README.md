# DSort

## Introduction

DSort is extension for DFC. It was designed to perform map-reduce like
operations on terabytes and petabytes of AI datasets. As a part of the whole
system, DSort is capable of taking advantage of objects stored on DFC without
much overhead.

AI datasets are usually stored in tarballs, zip objects, msgpacks or tf-records.
Focusing only on these types of files and specific workload allows us to tweak
performance without too much tradeoffs.

## Capabilities

Example of map-reduce like operation which can be performed on dSort is
shuffling (in particular sorting) all objects across all shards by a given
algorithm.

![dsort](../images/dsort_mapreduce.png)

We allow for output shards to be different size than input shards, thus a user
is also able to reshard the objects. This means that output shards can contain
more or less objects inside the shard than in the input shard, depending on
requested sizes of the shards.

The result of such an operation would mean that we could get output shards with
different sizes with objects that are shuffled across all the shards, which
would then be ready to be processed by a machine learning script/model.

## Terms

**Object** - single piece of data. In tarballs and zip files, an *object* is
single file contained in this type of archives. In msgpack (assuming that
msgpack file is stream of dictionaries) *object* is single dictionary.

**Shard** - collection of objects. In tarballs and zip files, a *shard* is whole
archive. In msgpack is the whole msgpack file.

We distinguish two kinds of shards: input and output. Input shards, as the name
says, it is given as an input for the dSort operation. Output on the other hand
is something that is the result of the operation. Output shards can differ from
input shards in many ways: size, number of objects, names etc.

Shards are assumed to be already on DFC cluster or somewhere in the cloud bucket
so that DFC can access them. Output shards will always be placed in the same
bucket and directory as the input shards - accessing them after completed dSort,
is the same as input shards but of course with different names.

**Record** - abstracts multiple objects with same key name into single
structure. Records are inseparable which means if they come from single shard
they will also be in output shard together.

Eg. if we have a tarball which contains files named: `file1.txt`, `file1.png`,
`file2.png`, then we would have 2 *records*: one for `file1` and one for
`file2`.

**Extraction phase** - dSort has multiple phases in which it does the whole
operation. The first of them is **extraction**. In this phase, dSort is reading
input shards and looks inside them to get to the objects and metadata. Objects
and their metadata are then extracted to either disk or memory so that dSort
won't need another pass of the whole data set again. This way DSort can create
**Records** which are then used for the whole operation as the main source of
information (like location of the objects, sizes, names etc.). Extraction phase
is very critical because it does I/O operations. To make the following phases
faster, we added support for extraction to memory so that requests for the given
objects will be served from RAM instead of disk. The user can specify how much
memory can be used for the extraction phase, either in raw numbers like `1GB` or
percentages `60%`.

As mentioned this operation does a lot of I/O operations. To allow the user to
have better control over the disk usage, we have provided a concurrency
parameter which limits the number of shards that can be read at the same time.

**Sorting phase** - in this phase, the metadata is processed and aggregated on a
single machine. It can be processed in various ways: sorting, shuffling,
resizing etc. This is usually the fastest phase but still uses a lot of CPU
processing power, to process the metadata.

The merging of metadata is performed in multiple steps to distribute the load
across machines.

**Creation phase** - it is last phase of dSort where output shards are created.
Like the extraction phase, the creation phase is bottlenecked by disk and I/O.
Additionally, this phase may use a lot of bandwidth because objects may have
been extracted on different machine.

Similarly to the extraction phase we expose a concurrency parameter for the
user, to limit number of shards created simultaneously.

Shards are created from local records or remote records. Local records are
records which were extracted on the machine where the shard is being created,
and similarly, remote records are records which were extracted on different
machines. This means that a single machine will typically have a lot of
read/write operations on the disk coming from either local or remote requests.
This is why tweaking the concurrency parameter is really important and can have
great impact on performance. We strongly advise to make couple of tests on small
load to see what value of this parameter will result in the best performance.
Eg. tests shown that on setup: 10x targets, 10x disks on each target, the best
concurrency value is 60.

The other thing that user needs to remember is that when running multiple dSort
operations at once, it might be better to set the concurrency parameter to
something lower since both of the operation may use disk at the same time. A
higher concurrency parameter can result in performance degradation.

**Metrics** - user can monitor whole operation thanks to metrics. Metrics
provide an overview of what is happening in the cluster, for example: which
phase is currently running, how much time has been spent on each phase, etc.
There are many metrics (numbers and stats) recorded for each of the phases.

## Playground

To easily use the dSort capabilities, we have created a bunch of scripts which
will help user to understand dSort a little bit better and to showcase some uses
dSort.

All scripts can be found [here](playground/README.md). Have fun :)
