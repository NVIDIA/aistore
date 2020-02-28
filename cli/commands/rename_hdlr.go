// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that rename entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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
			Usage: "rename entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdRenameBucket,
					Usage:        "rename an ais bucket",
					ArgsUsage:    bucketOldNewArgument,
					Flags:        renameCmdsFlags[subcmdRenameBucket],
					Action:       renameBucketHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, false /* separator */, cmn.ProviderAIS),
				},
				{
					Name:         subcmdRenameObject,
					Usage:        "rename an object of the ais bucket",
					ArgsUsage:    objectOldNewArgument,
					Flags:        renameCmdsFlags[subcmdRenameObject],
					Action:       renameObjectHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, true /* separator */, cmn.ProviderAIS),
				},
			},
		},
	}
)

func renameBucketHandler(c *cli.Context) (err error) {
	bucketName, newBucketName, err := getOldNewBucketName(c)
	if err != nil {
		return
	}
	bck, objName := parseBckObjectURI(bucketName)
	newBck, newObjName := parseBckObjectURI(newBucketName)

	if cmn.IsProviderCloud(bck, true) || cmn.IsProviderCloud(newBck, true) {
		return fmt.Errorf("renaming of cloud buckets not supported")
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

	bck, oldObj = parseBckObjectURI(oldObjFull)
	if oldObj == "" {
		return incorrectUsageMsg(c, "no object specified in '%s'", oldObjFull)
	}
	if bck.Name == "" {
		return incorrectUsageMsg(c, "no bucket specified for object '%s'", oldObj)
	}
	if bck.Provider != "" && !bck.IsAIS() {
		return incorrectUsageMsg(c, "provider '%s' not supported", bck.Provider)
	}

	bck.Provider = cmn.ProviderAIS
	if err = api.RenameObject(defaultAPIParams, bck, oldObj, newObj); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s renamed to %s\n", oldObj, newObj)
	return
}
