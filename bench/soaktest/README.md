# Soak Test

Soak Testing (also referred to as endurance testing) is defined [here](https://www.katalon.com/resources-center/blog/soak-testing/) as a test where the system is "evaluated to see whether it could perform well under a significant load for an extended period, thereby measuring its reaction and analyzing its behavior under sustained use".

`soaktest.sh` is the script that builds and runs a Soak Test executable (SK for short) on a currently running AIS custer.

## Dependencies

SK assumes that you have [Go 1.10 or later](https://golang.org/dl/) installed, and that `$PATH` is set to include the go installation (can be checked by running `go version`). This is used to build the executable.

SK also assumes that working code for `aisloader` is in the location `${GOPATH}/src/github.com/NVIDIA/aistore/bench/aisloader`, since the executable works by building and running a copy of `aisloader` from there.

## Design

SK simulates a wide variety of scenarios on the ais cluster using recipes. They are constantly run in a random order.

These are the terms used in describing the design of recipes:

- Recipe: A file that describe a possible scenario in our system. Recipes are written in go and can be found [here](recipes). All recipes are also registered [here](recipes/register.go). Recipes are comprised of primitives arranged in phases, with minimal go code in between.
- Primitive: A function call within a recipe that communicates with the AIS cluster. Examples: `GET(...)`, `PUT(...)`, `DELETE(...)` etc. Some of these are measured to track performance. The file that defines all primitives can be found [here](soakprim/primitives.go).
- Phase: A set of primitives surrounded by a call to `Pre(...)` at the start and ending with `Post(...)` is considered a phase within a recipe. Phases are run sequentially, while the primitives within a phase are run asynchronously. The call to `Pre(...)` ensures that the recipe meets all the prerequisites before proceeding with the phase, while the call to `Post(..)` checks if the phase was successful and saves summary information about the phase to the report.
- Regression: A constant process of repeated GETs that is run on the AIS cluster during the execution of SK, independent of recipes. The latency of the GETs is used to benchmark cluster performance during the soak test.

## Usage

SK can be run by running `soaktest.sh` the usual way bash scripts are run, and accepts command line args. Once running, it continues to run forever until the user specifies to stop it by pressing `ctrl+c` on the controlling console. Then it gracefully exits and prints reports to the specified directory. The paths to the reports are printed to the controlling console when SK exits.

The SK accepts a number of command line arguments, all of which can also be passed by passing the arguments into `soaktest.sh`:
 - `-ip` -  IP address for proxy server (assumes running locally if not set).
 - `-port` - Port number for proxy server (assumes running locally if not set).
 - `-short` - Skips longer recipes, set this to true if soak test is expected to run for under an hour.
 - `-numcycles` - Stops after running all recipes this amount of times.
 - `-reportdir` - The directory to write reports to, creates and uses `/tmp/ais-soak/reports` if not specified.

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
