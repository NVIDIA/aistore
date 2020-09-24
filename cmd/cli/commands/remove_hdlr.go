// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that remove various entities from the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/urfave/cli"
)

var (
	removeCmdsFlags = map[string][]cli.Flag{
		subcmdRemoveBucket: {
			ignoreErrorFlag,
		},
		subcmdRemoveObject: baseLstRngFlags,
		subcmdRemoveNode: {
			suspendModeFlag,
			noRebalanceFlag,
		},
		subcmdRemoveDownload: {
			allJobsFlag,
		},
		subcmdRemoveDsort: {},
	}

	removeCmds = []cli.Command{
		{
			Name:  commandRemove,
			Usage: "remove buckets, objects, and other entities",
			Subcommands: []cli.Command{
				{
					Name:      subcmdRemoveBucket,
					Usage:     "remove ais buckets",
					ArgsUsage: bucketsArgument,
					Flags:     removeCmdsFlags[subcmdRemoveBucket],
					Action:    removeBucketHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{
						multiple: true, provider: cmn.ProviderAIS,
					}),
				},
				{
					Name:      subcmdRemoveObject,
					Usage:     "remove object from bucket",
					ArgsUsage: optionalObjectsArgument,
					Flags:     removeCmdsFlags[subcmdRemoveObject],
					Action:    removeObjectHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{
						multiple: true, separator: true,
					}),
				},
				{
					Name:         subcmdRemoveNode,
					Usage:        "remove node from cluster",
					ArgsUsage:    daemonIDArgument,
					Flags:        removeCmdsFlags[subcmdRemoveNode],
					Action:       removeNodeHandler,
					BashComplete: daemonCompletions(completeAllDaemons),
				},
				{
					Name:         subcmdRemoveDownload,
					Usage:        "remove finished download job(s) identified by job's ID or regular expression",
					ArgsUsage:    jobIDArgument,
					Flags:        removeCmdsFlags[subcmdRemoveDownload],
					Action:       removeDownloadHandler,
					BashComplete: downloadIDFinishedCompletions,
				},
				{
					Name:         subcmdRemoveDsort,
					Usage:        fmt.Sprintf("remove finished %s job identified by the job's ID", cmn.DSortName),
					ArgsUsage:    jobIDArgument,
					Flags:        removeCmdsFlags[subcmdRemoveDsort],
					Action:       removeDsortHandler,
					BashComplete: dsortIDFinishedCompletions,
				},
			},
		},
	}
)

func removeBucketHandler(c *cli.Context) (err error) {
	var buckets []cmn.Bck
	if buckets, err = bucketsFromArgsOrEnv(c); err != nil {
		return
	}
	if err := validateLocalBuckets(buckets, "removing"); err != nil {
		return err
	}

	return destroyBuckets(c, buckets)
}

func removeObjectHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing bucket name")
	}

	// single fullObjName provided. Either remove one object or listFlag/templateFlag provided
	if c.NArg() == 1 {
		bck, objName, err := cmn.ParseBckObjectURI(c.Args().First())
		if err != nil {
			return err
		}
		if bck, _, err = validateBucket(c, bck, "", false); err != nil {
			return err
		}

		if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
			// list or range operation on a given bucket
			return listOrRangeOp(c, commandRemove, bck)
		}

		if objName == "" {
			return incorrectUsageMsg(c, "%q or %q flag not set with a single bucket argument",
				listFlag.Name, templateFlag.Name)
		}

		// ais rm BUCKET/OBJECT_NAME - pass, multiObjOp will handle it
	}

	// list and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q are cannot be used together with object name arguments",
			strings.Join([]string{listFlag.Name, templateFlag.Name}, ", "))
	}

	// object argument(s) given by the user; operation on given object(s)
	return multiObjOp(c, commandRemove)
}

func removeNodeHandler(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "daemon ID")
	}
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return err
	}
	sid := c.Args().First()
	node := smap.GetNode(sid)
	if node == nil {
		return fmt.Errorf("node %q does not exist", sid)
	}
	mode := parseStrFlag(c, suspendModeFlag)
	action := ""
	if mode != "" {
		if action, err = suspendModeToAction(c, mode); err != nil {
			return err
		}
	}
	doRebalance := !flagIsSet(c, noRebalanceFlag)
	switch mode {
	case "":
		return clusterRemoveNode(c, sid)
	default:
		var id string
		actValue := &cmn.ActValDecommision{DaemonID: sid, Rebalance: doRebalance}
		id, err = api.Maintenance(defaultAPIParams, action, actValue)
		if err != nil {
			return err
		}

		if action == cmn.ActUnsuspend {
			fmt.Fprintf(c.App.Writer, "Node %q maintenance stopped\n", sid)
		} else if action == cmn.ActDecommission && (node.IsProxy() || !doRebalance) {
			fmt.Fprintf(c.App.Writer, "Node %q removed from the cluster\n", sid)
		} else if action == cmn.ActSuspend {
			fmt.Fprintf(c.App.Writer, "Node %q is under maintenance\n", sid)
		} else {
			fmt.Fprintf(c.App.Writer,
				"Node %q is under maintenance\nStarted rebalance %q, use 'ais show xaction %s' to monitor progress\n",
				sid, id, id)
		}
	}
	return nil
}

func removeDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()
	if flagIsSet(c, allJobsFlag) {
		return removeDownloadRegex(c)
	}
	if c.NArg() < 1 {
		return missingArgumentsError(c, "download job ID")
	}
	if err = api.DownloadRemove(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed download job %q\n", id)
	return
}

func removeDownloadRegex(c *cli.Context) (err error) {
	var (
		dlList downloader.DlJobInfos
		regex  = ".*"
		cnt    int
		failed bool
	)
	dlList, err = api.DownloadGetList(defaultAPIParams, regex)
	if err != nil {
		return err
	}
	for _, dl := range dlList {
		if !dl.JobFinished() {
			continue
		}
		if err = api.DownloadRemove(defaultAPIParams, dl.ID); err == nil {
			fmt.Fprintf(c.App.Writer, "removed download job %q\n", dl.ID)
			cnt++
		} else {
			fmt.Fprintf(c.App.Writer, "failed to remove download job %q, err: %v\n", dl.ID, err)
			failed = true
		}
	}
	if cnt == 0 && !failed {
		fmt.Fprintf(c.App.Writer, "no finished download jobs, nothing to do\n")
	}
	return
}

func removeDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 {
		return missingArgumentsError(c, cmn.DSortName+" job ID")
	}

	if err = api.RemoveDSort(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed %s job %q\n", cmn.DSortName, id)
	return
}
