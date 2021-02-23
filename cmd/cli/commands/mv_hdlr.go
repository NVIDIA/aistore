// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands to move buckets and objects.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	mvCmdsFlags = map[string][]cli.Flag{
		subcmdMvBucket: {waitFlag},
		subcmdMvObject: {},
	}

	mvCmds = []cli.Command{
		{
			Name:  commandMv,
			Usage: "move (rename) buckets and objects",
			Subcommands: []cli.Command{
				{
					Name:         subcmdMvBucket,
					Usage:        "move an ais bucket",
					ArgsUsage:    "BUCKET_NAME NEW_BUCKET_NAME",
					Flags:        mvCmdsFlags[subcmdMvBucket],
					Action:       mvBucketHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, false /* separator */, cmn.ProviderAIS),
				},
				{
					Name:         subcmdMvObject,
					Usage:        "move an object in an ais bucket",
					ArgsUsage:    "BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME",
					Flags:        mvCmdsFlags[subcmdMvObject],
					Action:       mvObjectHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, true /* separator */, cmn.ProviderAIS),
				},
			},
		},
	}
)

func mvBucketHandler(c *cli.Context) error {
	bucketName, newBucketName, err := getOldNewBucketName(c)
	if err != nil {
		return err
	}
	bck, err := parseBckURI(c, bucketName)
	if err != nil {
		return err
	}
	newBck, err := parseBckURI(c, newBucketName)
	if err != nil {
		return err
	}

	if bck.Equal(newBck) {
		return incorrectUsageMsg(c, "cannot mv %q as %q", bck, newBck)
	}

	return mvBucket(c, bck, newBck)
}

func mvObjectHandler(c *cli.Context) (err error) {
	if c.NArg() != 2 {
		return incorrectUsageMsg(c, "invalid number of arguments")
	}
	var (
		oldObjFull = c.Args().Get(0)
		newObj     = c.Args().Get(1)

		oldObj string
		bck    cmn.Bck
	)

	if bck, oldObj, err = parseBckObjectURI(c, oldObjFull); err != nil {
		return
	}
	if oldObj == "" {
		return incorrectUsageMsg(c, "no object specified in %q", oldObjFull)
	}
	if bck.Name == "" {
		return incorrectUsageMsg(c, "no bucket specified for object %q", oldObj)
	}
	if bck.Provider != "" && !bck.IsAIS() {
		return incorrectUsageMsg(c, "provider %q not supported", bck.Provider)
	}

	if bckDst, objDst, err := parseBckObjectURI(c, newObj); err == nil && bckDst.Name != "" && bckDst.Provider != "" {
		if !bckDst.Equal(bck) {
			return incorrectUsageMsg(c, "moving an object to another bucket(%s) is not supported", bckDst)
		}
		if oldObj == "" {
			return missingArgumentsError(c, "no object specified in %q", newObj)
		}
		newObj = objDst
	}

	if newObj == oldObj {
		return incorrectUsageMsg(c, "source and destination are the same object")
	}

	bck.Provider = cmn.ProviderAIS
	if err = api.RenameObject(defaultAPIParams, bck, oldObj, newObj); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%q moved to %q\n", oldObj, newObj)
	return
}
