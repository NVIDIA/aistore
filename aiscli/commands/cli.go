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
	JSONFlag     = cli.BoolFlag{Name: "json,j", Usage: "json output"}
	VerboseFlag  = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	ChecksumFlag = cli.BoolFlag{Name: cmn.GetPropsChecksum, Usage: "validate checksum"}
	WaitFlag     = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}

	BucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the objects are saved to, eg. 'imagenet'", Value: ""}
	BckProviderFlag = cli.StringFlag{Name: cmn.URLParamBckProvider, Usage: "determines which bucket (`local` or `cloud`) should be used. By default, locality is determined automatically", Value: ""}

	// Downloader
	TimeoutFlag     = cli.StringFlag{Name: cmn.URLParamTimeout, Usage: "timeout for request to external resource, eg. '30m'", Value: ""}
	ObjNameFlag     = cli.StringFlag{Name: cmn.URLParamObjName, Usage: "name of the object the download is saved as, eg. 'train-images-mnist.tgz'", Value: ""}
	LinkFlag        = cli.StringFlag{Name: cmn.URLParamLink, Usage: "URL of where the object is downloaded from, eg. 'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz'", Value: ""}
	BaseFlag        = cli.StringFlag{Name: cmn.URLParamBase, Usage: "base URL of the object used to formulate the download URL, eg. 'http://yann.lecun.com/exdb/mnist'", Value: ""}
	TemplateFlag    = cli.StringFlag{Name: cmn.URLParamTemplate, Usage: "bash template describing names of the objects in the URL, eg: 'object{200..300}log.txt'", Value: ""}
	DlPrefixFlag    = cli.StringFlag{Name: cmn.URLParamPrefix, Usage: "prefix of the object name, eg. 'imagenet/imagenet-'"}
	DlSuffixFlag    = cli.StringFlag{Name: cmn.URLParamSuffix, Usage: "suffix of the object name, eg. '.tgz'"}
	IDFlag          = cli.StringFlag{Name: cmn.URLParamID, Usage: "id of the download job, eg: '76794751-b81f-4ec6-839d-a512a7ce5612'"}
	ProgressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}

	// Object
	KeyFlag      = cli.StringFlag{Name: "key", Usage: "name of object", Value: ""}
	OldKeyFlag   = cli.StringFlag{Name: "oldkey", Usage: "old name of object", Value: ""}
	OffsetFlag   = cli.StringFlag{Name: cmn.URLParamOffset, Usage: "object read offset", Value: ""}
	LengthFlag   = cli.StringFlag{Name: cmn.URLParamLength, Usage: "object read length", Value: ""}
	PrefixFlag   = cli.StringFlag{Name: cmn.URLParamPrefix, Usage: "prefix for object matching"}
	RegexFlag    = cli.StringFlag{Name: "regex", Usage: "regex pattern for object matching", Value: "\\d+1\\d"}
	ListFlag     = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'", Value: ""}
	RangeFlag    = cli.StringFlag{Name: "range", Usage: "colon separated interval of object indices, eg. <START>:<STOP>", Value: ""}
	DeadlineFlag = cli.StringFlag{Name: "deadline", Usage: "amount of time (Go Duration string) before the request expires", Value: "0s"}
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
