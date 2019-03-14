// The commands package provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import "github.com/urfave/cli"

type AIScli struct {
	*cli.App
}

var (
	JSONFlag    = cli.BoolFlag{Name: "json,j", Usage: "json output"}
	VerboseFlag = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
)

func New() AIScli {
	aisCLI := AIScli{cli.NewApp()}
	aisCLI.Init()
	return aisCLI
}

func (aisCLI AIScli) Init() {
	aisCLI.Name = "aiscli"
	aisCLI.Usage = "CLI tool for AIS"
	aisCLI.Version = "0.1"
	aisCLI.EnableBashCompletion = true
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}
}
