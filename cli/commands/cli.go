// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

type AIScli struct {
	*cli.App
}

const (
	cliName         = "aiscli"
	CommandList     = "list"
	CommandRename   = "rename"
	CommandSetProps = "setprops"
)

var (
	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	verboseFlag  = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	checksumFlag = cli.BoolFlag{Name: cmn.GetPropsChecksum, Usage: "validate checksum"}
	propsFlag    = cli.BoolFlag{Name: "props", Usage: "properties of resource (object, bucket)"}
	waitFlag     = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}

	bucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the objects are saved to, eg. 'imagenet'"}
	bckProviderFlag = cli.StringFlag{Name: cmn.URLParamBckProvider,
		Usage: "determines which bucket ('local' or 'cloud') should be used. By default, locality is determined automatically"}
	regexFlag = cli.StringFlag{Name: cmn.URLParamRegex, Usage: "regex pattern for matching"}

	// Downloader
	timeoutFlag     = cli.StringFlag{Name: cmn.URLParamTimeout, Usage: "timeout for request to external resource, eg. '30m'"}
	descriptionFlag = cli.StringFlag{Name: cmn.URLParamDescription, Usage: "description of the job - can be useful when listing all downloads"}
	objNameFlag     = cli.StringFlag{Name: cmn.URLParamObjName, Usage: "name of the object the download is saved as, eg. 'train-images-mnist.tgz'"}
	linkFlag        = cli.StringFlag{Name: cmn.URLParamLink,
		Usage: "URL of where the object is downloaded from, eg. 'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz'"}
	baseFlag = cli.StringFlag{Name: cmn.URLParamBase,
		Usage: "base URL of the object used to formulate the download URL, eg. 'http://yann.lecun.com/exdb/mnist'"}
	templateFlag = cli.StringFlag{Name: cmn.URLParamTemplate,
		Usage: "bash template describing names of the objects in the URL, eg: 'object{200..300}log.txt'"}
	dlPrefixFlag    = cli.StringFlag{Name: cmn.URLParamPrefix, Usage: "prefix of the object name, eg. 'imagenet/imagenet-'"}
	dlSuffixFlag    = cli.StringFlag{Name: cmn.URLParamSuffix, Usage: "suffix of the object name, eg. '.tgz'"}
	idFlag          = cli.StringFlag{Name: cmn.URLParamID, Usage: "id of the download job, eg: '76794751-b81f-4ec6-839d-a512a7ce5612'"}
	progressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
	refreshRateFlag = cli.IntFlag{Name: "refresh", Usage: "refresh rate for progress bar (in milliseconds)"}

	// Object
	keyFlag      = cli.StringFlag{Name: "key", Usage: "name of object"}
	outFileFlag  = cli.StringFlag{Name: "outfile", Usage: "name of the file where the contents will be saved"}
	bodyFlag     = cli.StringFlag{Name: "body", Usage: "filename for content of the object"}
	newKeyFlag   = cli.StringFlag{Name: "newkey", Usage: "new name of object"}
	offsetFlag   = cli.StringFlag{Name: cmn.URLParamOffset, Usage: "object read offset"}
	lengthFlag   = cli.StringFlag{Name: cmn.URLParamLength, Usage: "object read length"}
	prefixFlag   = cli.StringFlag{Name: cmn.URLParamPrefix, Usage: "prefix for string matching"}
	listFlag     = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'"}
	rangeFlag    = cli.StringFlag{Name: "range", Usage: "colon separated interval of object indices, eg. <START>:<STOP>"}
	deadlineFlag = cli.StringFlag{Name: "deadline", Usage: "amount of time (Go Duration string) before the request expires", Value: "0s"}

	// Bucket
	newBucketFlag = cli.StringFlag{Name: "newbucket", Usage: "new name of bucket"}
	pageSizeFlag  = cli.StringFlag{Name: "pagesize", Usage: "maximum number of entries by list bucket call", Value: "1000"}
	objPropsFlag  = cli.StringFlag{Name: "props", Usage: "properties to return with object names, comma separated", Value: "size,version"}
	objLimitFlag  = cli.StringFlag{Name: "limit", Usage: "limit object count", Value: "0"}
)

func flagIsSet(c *cli.Context, flag string) bool {
	// If the flag name has multiple values, take first one
	flag = cleanFlag(flag)
	return c.GlobalIsSet(flag) || c.IsSet(flag)
}

// Returns the value of flag (either parent or local scope)
func parseFlag(c *cli.Context, flag string) string {
	flag = cleanFlag(flag)
	if c.GlobalIsSet(flag) {
		return c.GlobalString(flag)
	}
	return c.String(flag)
}

func checkFlags(c *cli.Context, flag ...string) error {
	for _, f := range flag {
		if !flagIsSet(c, f) {
			return fmt.Errorf("%q is not set", f)
		}
	}
	return nil
}

func New() AIScli {
	aisCLI := AIScli{cli.NewApp()}
	aisCLI.Init()
	return aisCLI
}

func (aisCLI AIScli) Init() {
	aisCLI.Name = cliName
	aisCLI.Usage = "CLI tool for AIS"
	aisCLI.Version = "0.1"
	aisCLI.EnableBashCompletion = true
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}
}
