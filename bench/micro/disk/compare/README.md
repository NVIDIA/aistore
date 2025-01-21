This script provides a way to benchmark polling diskstats from [/proc/diskstats](https://www.kernel.org/doc/Documentation/iostats.txt) against using linux application `iostat`.

For a detailed analysis of the experiment results, please refer to this [PDF](experiments.pdf).

## How to run benchmark?

First, build the script: `go build`

Next, run the `lsblk` command to get a list of all disks, the value under the `NAME` column is used in the next step.

Finally, run the script using the disks you want use in the benchmark.

```console
$ ./compare <disk1> ... <diskN> > /path/to/results.csv
```

This continuously writes the benchmark data to `/path/to/results.csv`.

During this time, load can be generated on the disks by using [`loadgen.sh`](/bench/disk/loadgen) in a different terminal. Note that the corresponding mount points corresponding to the disks need to be passed. This can be obtained with the `lsblk` command, the value under the `MOUNTPOINT` column is the corresponding mount point.

The benchmark can be stopped at any time by pressing `ctrl+c` in the controlling terminal.