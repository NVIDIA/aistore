// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands to rename buckets and objects.
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
	renameCmdsFlags = map[string][]cli.Flag{
		subcmdRenameBucket: {},
		subcmdRenameObject: {},
	}

	renameCmds = []cli.Command{
		{
			Name:  commandRename,
			Usage: "rename buckets and objects",
			Subcommands: []cli.Command{
				{
					Name:         subcmdRenameBucket,
					Usage:        "rename ais bucket",
					ArgsUsage:    bucketOldNewArgument,
					Flags:        renameCmdsFlags[subcmdRenameBucket],
					Action:       renameBucketHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, false /* separator */, cmn.ProviderAIS),
				},
				{
					Name:         subcmdRenameObject,
					Usage:        "rename object in ais bucket",
					ArgsUsage:    objectOldNewArgument,
					Flags:        renameCmdsFlags[subcmdRenameObject],
					Action:       renameObjectHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, true /* separator */, cmn.ProviderAIS),
				},
			},
		},
	}
)

func renameBucketHandler(c *cli.Context) error {
	bucketName, newBucketName, err := getOldNewBucketName(c)
	if err != nil {
		return err
	}
	bck, objName, err := parseBckObjectURI(bucketName)
	if err != nil {
		return err
	}
	newBck, newObjName, err := parseBckObjectURI(newBucketName)
	if err != nil {
		return err
	}

	if err := validateLocalBuckets([]cmn.Bck{bck, newBck}, "renaming"); err != nil {
		return err
	}
	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}
	if newObjName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}

	bck.Provider, newBck.Provider = cmn.ProviderAIS, cmn.ProviderAIS

	return renameBucket(c, bck, newBck)
}

func renameObjectHandler(c *cli.Context) (err error) {
	if c.NArg() != 2 {
		return incorrectUsageMsg(c, "invalid number of arguments")
	}
	var (
		oldObjFull = c.Args().Get(0)
		newObj     = c.Args().Get(1)

		oldObj string
		bck    cmn.Bck
	)

	if bck, oldObj, err = parseBckObjectURI(oldObjFull); err != nil {
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

	bck.Provider = cmn.ProviderAIS
	if err = api.RenameObject(defaultAPIParams, bck, oldObj, newObj); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%q renamed to %q\n", oldObj, newObj)
	return
}
