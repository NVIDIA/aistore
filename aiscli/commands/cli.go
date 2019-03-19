// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

type AIScli struct {
	*cli.App
}

var (
	JSONFlag    = cli.BoolFlag{Name: "json,j", Usage: "json output"}
	VerboseFlag = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}

	// Downloader
	BucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the downloaded object is saved to, eg. 'imagenet'", Value: ""}
	BckProviderFlag = cli.StringFlag{Name: cmn.URLParamBckProvider, Usage: "determines which bucket (`local` or `cloud`) should be used. By default, locality is determined automatically", Value: ""}
	TimeoutFlag     = cli.StringFlag{Name: cmn.URLParamTimeout, Usage: "timeout for request to external resource, eg. '30m'", Value: ""}
	ObjNameFlag     = cli.StringFlag{Name: cmn.URLParamObjName, Usage: "name of the object the download is saved as, eg. 'train-images-mnist.tgz'", Value: ""}
	LinkFlag        = cli.StringFlag{Name: cmn.URLParamLink, Usage: "URL of where the object is downloaded from, eg. 'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz'", Value: ""}
	BaseFlag        = cli.StringFlag{Name: cmn.URLParamBase, Usage: "base URL of the object used to formulate the download URL, eg. 'http://yann.lecun.com/exdb/mnist'", Value: ""}
	TemplateFlag    = cli.StringFlag{Name: cmn.URLParamTemplate, Usage: "bash template describing names of the objects in the URL, eg: 'object{200..300}log.txt'", Value: ""}
	IDFlag          = cli.StringFlag{Name: cmn.URLParamID, Usage: "id of the download job, eg: '76794751-b81f-4ec6-839d-a512a7ce5612'"}
	ProgressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
)

// Returns the value of flag (either parent or local scope)
func parseFlag(c *cli.Context, flag string) string {
	if c.GlobalIsSet(flag) {
		return c.GlobalString(flag)
	}
	return c.String(flag)
}

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
