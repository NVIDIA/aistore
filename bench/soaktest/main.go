/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// 'soaktest' preforms a 'soak test' (see: https://www.tutorialspoint.com/software_testing_dictionary/soak_testing.htm) on AIStore.
// It sends requests using 'aisloader' an extended period of time to identify performance degradation, while also running configurable functional tests
// Run with -help for usage information.

package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"

	"github.com/NVIDIA/aistore/bench/soaktest/recipes"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/scheduler"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	regPhaseDurationShortDefault = time.Second * 10
	regPhaseDurationLongDefault  = time.Minute * 10

	recMinFilesizeDefault = 300 * cmn.MiB
	recMaxFilesizeDefault = 1 * cmn.GiB

	regMinFilesizeDefault = 700 * cmn.MiB
	regMaxFilesizeDefault = 2 * cmn.GiB
)

var (
	recipeListStr string

	recMinFilesizeStr string
	recMaxFilesizeStr string

	regMinFilesizeStr string
	regMaxFilesizeStr string
)

func printUsage(f *flag.FlagSet) {
	fmt.Println("Flags: ")
	f.PrintDefaults()
	fmt.Println()

	w := new(tabwriter.Writer)

	fmt.Println("Examples: ")
	fmt.Printf("  %v --short --rec-cycles=3 --reg-phasedisable\n\t\tRun all short recipes 3 times with no regression phases in between\n", os.Args[0])
	fmt.Printf("  %v --rec-list=1,3 --rec-pctcap=0.1 \n\t\tRun soaktest using RecipeID 1 and 3, using 0.1%% of capacity\n", os.Args[0])
	fmt.Printf("  %v --reg-phasedisable --rec-regdisable \n\t\tRun soaktest without regression\n", os.Args[0])
	fmt.Printf("  %v --rec-disable --reg-phaseduration=1s \n\t\tRun only regression phases that last 1 second\n", os.Args[0])
	fmt.Printf("  %v --rec-disable --reg-phasedisable \n\t\tRun cleanup\n", os.Args[0])
	fmt.Println()

	fmt.Println("Additional Commands: ")
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(w, "%v usage\tShows this menu\n", os.Args[0])
	fmt.Fprintf(w, "%v ls\tLists all recipes with descriptions\n", os.Args[0])
	w.Flush()
	fmt.Println()
}

func parseCmdLine() {

	// Command line options
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError) //Discard flags of imported packages

	f.StringVar(&soakcmn.Params.IP, "ip", "", "IP address for proxy server")
	f.StringVar(&soakcmn.Params.Port, "port", "", "Port number for proxy server")

	f.BoolVar(&soakcmn.Params.Short, "short", false, "Skips the longer recipes, makes the default reg-phaseduration shorter")

	f.BoolVar(&soakcmn.Params.RecDisable, "rec-disable", false, "Skips running recipes, if true will if true will just continuously run regression phases")
	f.StringVar(&recipeListStr, "rec-list", "", "Comma-delimited list of RecipeIDs to run, if set '--short' will be ignored")
	f.IntVar(&soakcmn.Params.NumCycles, "rec-cycles", 0, "Stop after cycling through all the recipes this many times, 0=infinite")
	f.BoolVar(&soakcmn.Params.RecRegDisable, "rec-regdisable", false, "Disables running regression while recipe is running")
	f.Float64Var(&soakcmn.Params.RecPctCapacity, "rec-pctcap", 0.9, "Pct (0-100) of total storage capacity allocated to recipes")
	f.StringVar(&recMinFilesizeStr, "rec-minsize", "", fmt.Sprintf("Min filesize in recipes (default %s), can specify units with suffix", cmn.B2S(recMinFilesizeDefault, 2)))
	f.StringVar(&recMaxFilesizeStr, "rec-maxsize", "", fmt.Sprintf("Max filesize in recipes (default %s), can specify units with suffix", cmn.B2S(recMaxFilesizeDefault, 2)))
	f.IntVar(&soakcmn.Params.RecPrimWorkers, "rec-primworkers", 1, "Number of workers that are run by a primitive within a recipe")

	f.BoolVar(&soakcmn.Params.RegPhaseDisable, "reg-phasedisable", false, "Skips running regression phases, if true will just continuously run recipes")
	f.DurationVar(&soakcmn.Params.RegPhaseDuration, "reg-phaseduration", 0, fmt.Sprintf("Duration of regression phases (default: %v short, %v long)", regPhaseDurationShortDefault, regPhaseDurationLongDefault))
	f.Float64Var(&soakcmn.Params.RegPctCapacity, "reg-pctcap", 2.0/5.0, "Pct (0-100) of total storage capacity allocated to regression")
	f.DurationVar(&soakcmn.Params.RegSetupDuration, "reg-setupduration", time.Second*12, "The maximum amount of time to spend setting up the bucket for regression")
	f.IntVar(&soakcmn.Params.RegSetupWorkers, "reg-setupworkers", 4, "Number of workers that is used to set up the bucket for regression")
	f.StringVar(&regMinFilesizeStr, "reg-minsize", "", fmt.Sprintf("Min filesize in regression (default %s), can specify units with suffix", cmn.B2S(regMinFilesizeDefault, 2)))
	f.StringVar(&regMaxFilesizeStr, "reg-maxsize", "", fmt.Sprintf("Max filesize in regression (default %s), can specify units with suffix", cmn.B2S(regMaxFilesizeDefault, 2)))
	f.IntVar(&soakcmn.Params.RegWorkers, "reg-workers", 1, "Number of workers that regression uses")

	f.StringVar(&soakcmn.Params.ReportDir, "reportdir", "", fmt.Sprintf("Folder to place report, places in '%s' if empty", report.DefaultDir))

	f.Parse(os.Args[1:])

	os.Args = []string{os.Args[0]}
	flag.Parse() //Called so that imported packages don't compain

	//Check commands (not args)
	if f.NArg() != 0 {
		switch f.Arg(0) {
		case "usage":
			printUsage(f)
			os.Exit(0)
		case "ls":
			recipes.PrintRecipes()
		default:
			fmt.Printf("Invalid command %v\nType '%v usage' for help\n", f.Arg(0), flag.Arg(0))
		}
		os.Exit(0)
	}
}

func writeParams() {
	b, _ := jsoniter.Marshal(soakcmn.Params)
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
					fmt.Printf("can't parse RecipeID list: %v\n", recipeListStr)
					return
				}
				if _, ok := valid[newID]; !ok {
					fmt.Printf("invalid RecipeID: %v\n", newID)
					return
				}
				recipeList = append(recipeList, newID)
			}
		}

		if len(recipeList) == 0 {
			fmt.Println("RecipeID list empty")
			return
		}

		//de-dup
		soakcmn.Params.RecSet = make(map[int]struct{})
		for _, x := range recipeList {
			soakcmn.Params.RecSet[x] = struct{}{}
		}
	}

	// Sanity check for filesizes
	checkFilesize := func(inp string, def int64, outp *int64) bool {
		if inp == "" {
			*outp = def
		} else {
			var err error
			if *outp, err = cmn.S2B(inp); err != nil {
				fmt.Printf("%v is not a size\n", inp)
				return false
			}
		}
		if *outp <= 0 {
			fmt.Printf("%v must be positive number\n", inp)
			return false
		}
		return true
	}

	// Sanity check filesizes for recipe
	if !checkFilesize(recMinFilesizeStr, recMinFilesizeDefault, &soakcmn.Params.RecMinFilesize) {
		return
	}
	if !checkFilesize(recMaxFilesizeStr, recMaxFilesizeDefault, &soakcmn.Params.RecMaxFilesize) {
		return
	}
	if soakcmn.Params.RecMaxFilesize < soakcmn.Params.RecMinFilesize {
		fmt.Printf("recipe filesize: max %v must be at least min %v\n", recMaxFilesizeStr, recMinFilesizeStr)
	}

	// Sanity check filesizes for regression
	if !checkFilesize(regMinFilesizeStr, regMinFilesizeDefault, &soakcmn.Params.RegMinFilesize) {
		return
	}
	if !checkFilesize(regMaxFilesizeStr, regMaxFilesizeDefault, &soakcmn.Params.RegMaxFilesize) {
		return
	}
	if soakcmn.Params.RegMaxFilesize < soakcmn.Params.RegMinFilesize {
		fmt.Printf("regression filesize: max %v must be at least min %v\n", regMaxFilesizeStr, regMinFilesizeStr)
	}

	// Sanity check for ip and port
	if soakcmn.Params.IP == "" && soakcmn.Params.Port == "" {
		if tutils.DockerRunning() {
			dockerEnvFile := "/tmp/docker_ais/deploy.env"
			envVars := tutils.ParseEnvVariables(dockerEnvFile)
			soakcmn.Params.IP = envVars["PRIMARY_HOST_IP"]
			soakcmn.Params.Port = envVars["PORT"]
		} else {
			soakcmn.Params.IP = "localhost"
			soakcmn.Params.Port = "8080"
		}
	} else if soakcmn.Params.IP == "" {
		fmt.Println("port specified without ip")
		return
	} else if soakcmn.Params.Port == "" {
		fmt.Println("ip specified without port")
		return
	}
	if err := tutils.Tcping(soakcmn.Params.IP + ":" + soakcmn.Params.Port); err != nil {
		fmt.Printf("Cannot connect to %s:%s, reason: %v\n", soakcmn.Params.IP, soakcmn.Params.Port, err)
		return
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

	report.InitReportFiles()

	writeParams()

	scheduler.RunRandom()

	report.Close()

	soakprim.CleanupSoak()
}
