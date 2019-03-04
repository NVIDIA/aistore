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

	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/scheduler"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/tutils"
)

var (
	short     bool //if true, skips the longer recipes
	numCycles int  //number of cycles to run, 0=infinite
	reportDir string

	ip   string
	port string
)

func parseCmdLine() {
	// Command line options
	flag.BoolVar(&short, "short", false, "If true, skips the longer recipes")
	flag.IntVar(&numCycles, "numcycles", 0, "Number of cycles to run, 0=infinite")
	flag.StringVar(&reportDir, "reportdir", "", fmt.Sprintf("Folder to place report, places in '%s' if empty", report.DefaultDir))

	flag.StringVar(&ip, "ip", "", "IP address for proxy server")
	flag.StringVar(&port, "port", "", "Port number for proxy server")

	flag.Parse()
}

func main() {
	parseCmdLine()

	if ip == "" && port == "" {
		if tutils.DockerRunning() {
			dockerEnvFile := "/tmp/docker_ais/deploy.env"
			envVars := tutils.ParseEnvVariables(dockerEnvFile)
			ip = envVars["PRIMARY_HOST_IP"]
			port = envVars["PORT"]
		} else {
			ip = "localhost"
			port = "8080"
		}
	} else if ip == "" {
		fmt.Println("port specified without ip")
		return
	} else if port == "" {
		fmt.Println("ip specified without port")
		return
	}

	if err := tutils.Tcping(ip + ":" + port); err != nil {
		fmt.Printf("Cannot connect to %s:%s, reason: %v\n", ip, port, err)
		return
	}

	soakprim.SetPrimaryURL(ip, port)

	scheduler.RunShort = short
	scheduler.NumCycles = numCycles

	report.InitReportFiles(reportDir)

	scheduler.RunRandom()

	report.Close()
}
