---
layout: post
title: SOAKTEST
permalink: bench/soaktest
redirect_from:
 - bench/soaktest/README.md/
---

[Soak Testing](https://en.wikipedia.org/wiki/Soak_testing) (also referred to as endurance testing) is a practice where a system is evaluated to see whether it could perform well under a significant load for an extended period, thereby measuring its reaction and analyzing its behavior under sustained use.

`soaktest.sh` is the script that builds and runs a Soak Test executable (SK for short) on a currently running AIS cluster.

## Dependencies

SK assumes that you have [Go 1.10 or later](https://golang.org/dl/) installed, and that `$PATH` is set to include the go installation (can be checked by running `go version`). This is used to build the executable.

SK also assumes that the working code for `aisloader` is in the location `${GOPATH}/src/github.com/NVIDIA/aistore/bench/aisloader`, since the executable works by building and running a copy of `aisloader` from there.

## Design

SK simulates a wide variety of scenarios on the ais cluster using recipes. They are constantly run in random order.

These are the terms used in describing the design of recipes:

- Recipe: A file that describes a possible scenario in our system. Recipes are written in go and can be found [here](recipes). All recipes are also registered [here](recipes/register.go). Recipes are comprised of primitives arranged in phases, with minimal go code in between.
- Recipe Cycle: SK runs recipes by constantly running a random permutation of all available recipes. The run of a particular permutation is considered a Recipe Cycle.
- Primitive: A function call within a recipe that communicates with the AIS cluster. Examples: `GET(...)`, `PUT(...)`, `DELETE(...)` etc. Some of these are measured to track performance. The file that defines all primitives can be found [here](soakprim/primitives.go).
- Phase: A set of primitives surrounded by a call to `Pre(...)` at the start and ending with `Post(...)` is considered a phase within a recipe. Phases are run sequentially, while the primitives within a phase are run asynchronously. The call to `Pre(...)` ensures that the recipe meets all the prerequisites before proceeding with the phase, while the call to `Post(..)` checks if the phase was successful and saves summary information about the phase to the report.
- Regression: A continuous workload that continuously GETs from a bucket. Designed to simulate a neural network using training data hosted by AIS. Regression can either run alongside another recipe or run by itself (called a regression phase). SK alternates between running a recipe and running regression phase. Multiple instances of this can be run in parallel and will point to the same bucket.

## Usage

SK can be run by running `soaktest.sh` the usual way bash scripts are run, and accepts command line args. Once running, it continues to run forever until the user specifies to stop it by pressing `ctrl+c` on the controlling console. Then it gracefully exits and prints reports to the specified directory. The paths to the reports are printed to the controlling console when SK exits.

SK accepts a number of command-line arguments, all of which can also be passed by passing the arguments into `soaktest.sh`:
 - `-ip` -  IP address for proxy server (assumes running locally if not set).
 - `-port` - Port number for proxy server (assumes running locally if not set).

 - `-short` - Skips the longer recipes, makes the default  `-reg-phaseduration` shorter.

 - `-keeptargets` - Skips the recipes that remove targets.
 - `-nocleanup` - Disables cleaning up buckets from other SK.

 - `-rec-disable` - Skips running recipes, if true will just continuously run regression phases.
 - `-rec-list` - Comma-delimited list of RecipeIDs to run (use `./soaktest.sh ls` to get RecipeIDs), if set `-short` will be ignored.
 - `-rec-cycles` - Stops after running this many recipe cycles, 0=infinite.
 - `-rec-regdisable` - Disables running regression while recipe is running.
 - `-rec-pctcap` - Max Pct (0-100) of total storage capacity allocated to recipes (Default 0.9).
 - `-rec-minsize` - Min filesize in recipes (default 300MiB), can specify with [multiplicative suffix](/bench/aisloader/README.md#bytes-multiplicative-suffix).
 - `-rec-maxsize` - Max filesize in recipes (default 1GiB), can specify with [multiplicative suffix](/bench/aisloader/README.md#bytes-multiplicative-suffix).
 - `-rec-primworkers` - Number of workers that are run by a primitive within a recipe (default 1).

 - `-reg-phasedisable` - Skips running regression phases, if true will just continuously run recipes.
 - `-reg-phaseduration` - Duration of regression phases (default: 10s short, 1m long).
 - `-reg-pctcap` - Max Pct (0-100) of total storage capacity allocated to regression (default 0.4).
 - `-reg-setupduration` - The maximum amount of time to spend setting up the bucket for regression (default 12s), 0=fill until `-reg-pctcap`.
 - `-reg-setupworkers` - Number of workers that is used to set up the bucket for regression (default 4).
 - `-reg-minsize` - Min filesize in regression (default 700MiB), can specify with [multiplicative suffix](/bench/aisloader/README.md#bytes-multiplicative-suffix).
 - `-reg-maxsize` - Max filesize in regression (default 2GiB), can specify with [multiplicative suffix](/bench/aisloader/README.md#bytes-multiplicative-suffix).
 - `-reg-instances` - Number of regression instances (default 1).
 - `-reg-workers` - Number of workers that regression uses (default 1).

 - `-reportdir` - The directory to write reports to, creates and uses `/tmp/ais-soak/reports` if not specified.

SK supports additional commands:
 - `./soaktest.sh ls` displays a list of all recipes with descriptions.
 - `./soaktest.sh usage` displays an extended help menu with examples.

 ## Usage Examples

 - `./soaktest.sh`
    - Run soaktest with all default parameters
 - `./soaktest.sh --short`
    - Run short soaktest with all default parameters
 - `./soaktest.sh --short --rec-cycles=1 --reg-phasedisable --rec-regdisable`
    - Run all short recipes and then exit with no regression
 - `./soaktest.sh --rec-disable --reg-phaseduration=1s`
    - Run regression phases in intervals of one second
 - `./soaktest.sh --ip=my-k8-cluster --port=8081`
    - Run soaktest with default parameters on the proxy at http://my-k8-cluster:8081
 - `./soaktest.sh --rec-list=1,3 --rec-pctcap=0.2 --reg-pctcap=0.1`
    - Run soaktest using RecipeID 1 and 3, allocating 0.2% of capacity to recipes, and 0.1% to regression
 - `./soaktest.sh --rec-disable --reg-phasedisable`
    - Don't run anything and just cleanup

Note that running multiple SK in parallel is supported, the requirement is that `-localcleanup` is set on all SK, and `-keeptargets` is set on all but one SK that runs recipes.

For example, the following script runs 5 separate instances of SK

```console
$ ./soaktest.sh --short --nocleanup --rec-primworkers=4 --reg-phasedisable
$ ./soaktest.sh --nocleanup --keeptargets
$ ./soaktest.sh --rec-disable --reg-workers=4 --nocleanup
$ ./soaktest.sh --short --nocleanup --keeptargets
$ ./soaktest.sh --short --reg-phasedisable --nocleanup --keeptargets
```

 ## Output

SK prints information about what's happening within the soak test to the controlling console. Examples include the current running recipe and which phase is running within the recipe.

In addition to the console, SK also creates a folder in the directory specified by the `reportdir` argument. This folder has a name that's unique to the running instance of SK. Files in this folder are also suffixed with the folder name.

The report folder contains a number of files. `*` is used to indicate suffix of the folder name:
 - `detail-*.log` -  A detailed log of everything that's happening within the soak test. Note that this is more detailed than what is displayed to the console.
 - `summary-*.log` - A log of all the errors encountered during the run of the soak test. Every entry present in this log should also be present in `detail-*.log`.

 - `detail-*.csv` - Records the metrics from AISLoader that are returned when called by a primitive.
 - `summary-*.csv` - Records the same data as `detail-*.csv`, except aggregated by recipe.
 - `regression-*.csv` - Periodically records the metrics from AISLoader that are returned to the regression process.
 - `sysinfo-*.csv` - Periodically records the CPU and RAM usage of nodes within the AIS Cluster.

 ## Formatting Excel Timestamp

Some csv files in the SK output folder have columns that report timestamps in `excel timestamp`. They can be formatted to a user-friendly format in LibreOffice Calc and Excel.

### LibreOffice Calc

First, right-click on the heading above the column and click `Format Cells ...`.

<img src="/aistore/docs/images/soak-time-libreoffice-step1.png" width="50%" >

Next, in the displayed dialog window, select the `Date` option in the `Category` box, and change the text in the `Format code` textbox to `MM/DD/YY HH:MM:SS.000`. Then, click the `ok` button.

<img src="/aistore/docs/images/soak-time-libreoffice-step2.png" width="50%" >

### Excel

First, right-click on the heading above the column and click `Format Cells ...`.

<img src="/aistore/docs/images/soak-time-excel-step1.png" width="50%" >

Next, in the displayed dialog window, select the `Custom` option in the `Category` box, and change the text in the `Type` textbox to `MM/DD/YY HH:MM:SS.000`. Then, click the `ok` button.

> If the column is used in a graph, `HH:MM:SS` should be used instead of `MM/DD/YY HH:MM:SS.000` to reduce clutter in the resulting graph.

<img src="/aistore/docs/images/soak-time-excel-step2.png" width="50%" >
