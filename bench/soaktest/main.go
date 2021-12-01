// Package soaktest provides tools to perform soak tests on AIS cluster
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */

// 'soaktest' preforms a 'soak test' (see: https://www.tutorialspoint.com/software_testing_dictionary/soak_testing.htm) on AIStore.
// It sends requests using 'aisloader' an extended period of time to identify performance degradation, while also running configurable functional tests
// Run with -help for usage information.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/recipes"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/scheduler"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools"
)

const (
	regPhaseDurationShortDefault = time.Second * 10
	regPhaseDurationLongDefault  = time.Minute * 10

	recMinFilesizeDefault = 300 * cos.MiB
	recMaxFilesizeDefault = 1 * cos.GiB

	regMinFilesizeDefault = 700 * cos.MiB
	regMaxFilesizeDefault = 2 * cos.GiB
)

var (
	flagUsage bool
	flagLs    bool

	recipeListStr string

	recMinFilesizeStr string
	recMaxFilesizeStr string

	regMinFilesizeStr string
	regMaxFilesizeStr string
)

func getExecutable() string {
	val, ok := os.LookupEnv("START_CMD")
	if ok {
		return val
	}
	return filepath.Base(os.Args[0])
}

func printUsage(f *flag.FlagSet) {
	fmt.Println("Flags: ")
	f.PrintDefaults()
	fmt.Println()

	w := new(tabwriter.Writer)

	executable := getExecutable()

	fmt.Println("Examples: ")
	fmt.Printf("  %v --short --rec-cycles=3 --reg-phasedisable\n\t\tRun all short recipes 3 times with no regression phases in between\n", executable)
	fmt.Printf("  %v --rec-list=1,3 --rec-pctcap=0.1 \n\t\tRun soaktest using RecipeID 1 and 3, using 0.1%% of capacity\n", executable)
	fmt.Printf("  %v --reg-phasedisable --rec-regdisable \n\t\tRun soaktest without regression\n", executable)
	fmt.Printf("  %v --rec-disable --reg-phaseduration=1s \n\t\tRun only regression phases that last 1 second\n", executable)
	fmt.Printf("  %v --rec-disable --reg-phasedisable \n\t\tRun cleanup\n", executable)
	fmt.Println()

	fmt.Println("Additional Commands: ")
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(w, "%v usage\tShows this menu\n", executable)
	fmt.Fprintf(w, "%v ls\tLists all recipes with descriptions\n", executable)
	w.Flush()
	fmt.Println()
}

func parseCmdLine() {
	// Command line options
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Discard flags of imported packages

	f.StringVar(&soakcmn.Params.IP, "ip", "", "IP address for proxy server")
	f.StringVar(&soakcmn.Params.Port, "port", "", "Port number for proxy server")

	f.BoolVar(&flagUsage, "usage", false, "Show extensive help menu with examples and exit")
	f.BoolVar(&flagLs, "ls", false, "Lists all recipes with descriptions and exit")

	f.BoolVar(&soakcmn.Params.Short, "short", false, "Skips the longer recipes, makes the default reg-phaseduration shorter")

	f.BoolVar(&soakcmn.Params.KeepTargets, "keeptargets", false, "skips the recipes that remove targets")
	f.BoolVar(&soakcmn.Params.LocalCleanup, "localcleanup", false, "disables cleaning up buckets from other soaktests")

	f.BoolVar(&soakcmn.Params.RecDisable, "rec-disable", false, "Skips running recipes, and just continuously run regression phases")
	f.StringVar(&recipeListStr, "rec-list", "", "Comma-delimited list of RecipeIDs to run, if set '--short' will be ignored")
	f.IntVar(&soakcmn.Params.NumCycles, "rec-cycles", 0, "Stop after cycling through all the recipes this many times, 0=infinite")
	f.BoolVar(&soakcmn.Params.RecRegDisable, "rec-regdisable", false, "Disables running regression while recipe is running")
	f.Float64Var(&soakcmn.Params.RecPctCapacity, "rec-pctcap", 0.9, "Pct (0-100) of total storage capacity allocated to recipes")
	f.StringVar(&recMinFilesizeStr, "rec-minsize", "", fmt.Sprintf("Min filesize in recipes (default %s), can specify units with suffix", cos.B2S(recMinFilesizeDefault, 2)))
	f.StringVar(&recMaxFilesizeStr, "rec-maxsize", "", fmt.Sprintf("Max filesize in recipes (default %s), can specify units with suffix", cos.B2S(recMaxFilesizeDefault, 2)))
	f.IntVar(&soakcmn.Params.RecPrimWorkers, "rec-primworkers", 1, "Number of workers that are run by a primitive within a recipe")

	f.BoolVar(&soakcmn.Params.RegPhaseDisable, "reg-phasedisable", false, "Skips running regression phases, and just continuously run recipes")
	f.DurationVar(&soakcmn.Params.RegPhaseDuration, "reg-phaseduration", 0, fmt.Sprintf("Duration of regression phases (default: %v short, %v long)", regPhaseDurationShortDefault, regPhaseDurationLongDefault))
	f.Float64Var(&soakcmn.Params.RegPctCapacity, "reg-pctcap", 2.0/5.0, "Pct (0-100) of total storage capacity allocated to regression")
	f.DurationVar(&soakcmn.Params.RegSetupDuration, "reg-setupduration", time.Second*12, "The maximum amount of time to spend setting up the bucket for regression")
	f.IntVar(&soakcmn.Params.RegSetupWorkers, "reg-setupworkers", 4, "Number of workers that is used to set up the bucket for regression")
	f.StringVar(&regMinFilesizeStr, "reg-minsize", "", fmt.Sprintf("Min filesize in regression (default %s), can specify units with suffix", cos.B2S(regMinFilesizeDefault, 2)))
	f.StringVar(&regMaxFilesizeStr, "reg-maxsize", "", fmt.Sprintf("Max filesize in regression (default %s), can specify units with suffix", cos.B2S(regMaxFilesizeDefault, 2)))
	f.IntVar(&soakcmn.Params.RegInstances, "reg-instances", 1, "Number of instances of regression per bucket")
	f.IntVar(&soakcmn.Params.RegWorkers, "reg-workers", 1, "Number of workers that regression uses")

	f.StringVar(&soakcmn.Params.ReportDir, "reportdir", "", fmt.Sprintf("Folder to place report, places in %q if empty", report.DefaultDir))

	f.Parse(os.Args[1:])

	if len(os.Args[1:]) == 0 {
		fmt.Printf("No flags passed. Type '%v usage' for list of flags and examples.\n", getExecutable())
		fmt.Println("Proceeding with default flags ...")
	}

	os.Args = []string{os.Args[0]}
	flag.Parse() // Called so that imported packages don't compain

	// Check commands (not args)
	if f.NArg() != 0 {
		switch f.Arg(0) {
		case "usage":
			printUsage(f)
		case "ls":
			recipes.PrintRecipes()
		default:
			fmt.Printf("Invalid command %v\nType '%v usage' for help\n", f.Arg(0), flag.Arg(0))
		}
		os.Exit(0)
	}

	if flagUsage {
		printUsage(f)
		os.Exit(0)
	}
	if flagLs {
		recipes.PrintRecipes()
		os.Exit(0)
	}
}

func writeParams() {
	b := cos.MustMarshal(soakcmn.Params)
	report.Writef(report.SummaryLevel, "----- Started Soak With Params -----\n")
	report.Writef(report.SummaryLevel, "%s\n", string(b))
	report.Writef(report.SummaryLevel, "----- Started Soak With Params -----\n")
	report.Flush()
}

func main() {
	parseCmdLine()

	// Sanity check for recipe list
	recipeList := make([]int, 0)
	if recipeListStr != "" {
		valid := recipes.GetValidRecipeIDs()
		for _, x := range strings.Split(recipeListStr, ",") {
			if x != "" {
				newID, err := strconv.Atoi(x)
				if err != nil {
					cos.Exitf("Cannot parse RecipeID list: %v", recipeListStr)
				}
				if _, ok := valid[newID]; !ok {
					cos.Exitf("Invalid RecipeID: %v", newID)
				}
				recipeList = append(recipeList, newID)
			}
		}

		if len(recipeList) == 0 {
			cos.Exitf("RecipeID list empty")
		}

		// de-dup
		soakcmn.Params.RecSet = make(map[int]struct{})
		for _, x := range recipeList {
			soakcmn.Params.RecSet[x] = struct{}{}
		}
	}

	// Sanity check for filesizes
	checkFilesize := func(inp string, def int64, outp *int64) {
		if inp == "" {
			*outp = def
		} else {
			var err error
			if *outp, err = cos.S2B(inp); err != nil {
				cos.Exitf("%v is not a size", inp)
			}
		}
		if *outp <= 0 {
			cos.Exitf("%v must be positive number", inp)
		}
	}

	// Sanity check filesizes for recipe
	checkFilesize(recMinFilesizeStr, recMinFilesizeDefault, &soakcmn.Params.RecMinFilesize)
	checkFilesize(recMaxFilesizeStr, recMaxFilesizeDefault, &soakcmn.Params.RecMaxFilesize)
	if soakcmn.Params.RecMaxFilesize < soakcmn.Params.RecMinFilesize {
		cos.Exitf("Recipe filesize: max %v must be at least min %v", recMaxFilesizeStr, recMinFilesizeStr)
	}

	// Sanity check filesizes for regression
	checkFilesize(regMinFilesizeStr, regMinFilesizeDefault, &soakcmn.Params.RegMinFilesize)
	checkFilesize(regMaxFilesizeStr, regMaxFilesizeDefault, &soakcmn.Params.RegMaxFilesize)
	if soakcmn.Params.RegMaxFilesize < soakcmn.Params.RegMinFilesize {
		cos.Exitf("Regression filesize: max %v must be at least min %v", regMaxFilesizeStr, regMinFilesizeStr)
	}

	// Sanity check for number of workers
	if soakcmn.Params.RecPrimWorkers < 1 {
		cos.Exitf("rec-primworkers must be at least 1")
	}
	if soakcmn.Params.RegSetupWorkers < 1 {
		cos.Exitf("reg-setupworkers must be at least 1")
	}
	if soakcmn.Params.RegWorkers < 1 {
		cos.Exitf("reg-workers must be at least 1")
	}

	// Sanity check for ip and port
	if soakcmn.Params.IP == "" && soakcmn.Params.Port == "" {
		if containers.DockerRunning() {
			dockerEnvFile := "/tmp/docker_ais/deploy.env"
			envVars := cos.ParseEnvVariables(dockerEnvFile)
			soakcmn.Params.IP = envVars["PRIMARY_HOST_IP"]
			soakcmn.Params.Port = envVars["PORT"]
		} else {
			soakcmn.Params.IP = "localhost"
			soakcmn.Params.Port = "8080"
		}
	} else if soakcmn.Params.IP == "" {
		cos.Exitf("Port specified without IP")
	} else if soakcmn.Params.Port == "" {
		cos.Exitf("IP specified without port")
	}
	if err := devtools.PingURL(soakcmn.Params.IP + ":" + soakcmn.Params.Port); err != nil {
		cos.Exitf("Cannot connect to %s:%s, reason: %s", soakcmn.Params.IP, soakcmn.Params.Port, err)
	}
	soakprim.SetPrimaryURL()

	// Check if everything disabled
	if soakcmn.Params.RecDisable && soakcmn.Params.RegPhaseDisable {
		fmt.Println("Both recipes and regression phase are disabled, performing cleanup...")
		soakprim.CleanupSoak()
		return
	}

	// Set the default regression phase duration based on short
	if soakcmn.Params.RegPhaseDuration == 0 {
		if soakcmn.Params.Short {
			soakcmn.Params.RegPhaseDuration = regPhaseDurationShortDefault
		} else {
			soakcmn.Params.RegPhaseDuration = regPhaseDurationLongDefault
		}
	}

	// Params are all good at this point

	// do cleanup before starting to avoid going over capacity
	soakprim.CleanupSoak()

	report.InitReportFiles()

	writeParams()

	scheduler.RunRandom()

	report.Close()

	soakprim.CleanupSoak()
}
