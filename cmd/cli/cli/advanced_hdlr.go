// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides advanced commands that are useful for testing or development but not everyday use.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

var (
	advancedCmd = cli.Command{
		Name:  commandAdvanced,
		Usage: "Special commands intended for development and advanced usage",
		Subcommands: []cli.Command{
			jobStartResilver,
			{
				Name:         cmdPreload,
				Usage:        "Preload object metadata into in-memory cache",
				ArgsUsage:    bucketArgument,
				Action:       loadLomCacheHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:         cmdRmSmap,
				Usage:        "Immediately remove node from cluster map (beware: potential data loss!)",
				ArgsUsage:    nodeIDArgument,
				Action:       removeNodeFromSmap,
				BashComplete: suggestAllNodes,
			},
			{
				Name:   cmdRandNode,
				Usage:  "Print random node ID (by default, ID of a randomly selected target)",
				Action: randNode,
				BashComplete: func(c *cli.Context) {
					if c.NArg() == 0 {
						fmt.Println(apc.Proxy, apc.Target)
					}
				},
			},
			{
				Name:         cmdRandMountpath,
				Usage:        "Print a random mountpath from a given target",
				Action:       randMountpath,
				BashComplete: suggestTargets,
			},
			{
				Name:         cmdRotateLogs,
				Usage:        "Rotate aistore logs",
				ArgsUsage:    optionalNodeIDArgument,
				Action:       rotateLogs,
				BashComplete: suggestAllNodes,
			},
			{
				Name:         cmdBackendEnable,
				Usage:        "(Re)enable cloud backend (see also: 'ais config cluster backend')",
				ArgsUsage:    cloudProviderArg,
				Action:       backendEnableHandler,
				BashComplete: suggestCloudProvider,
			},
			{
				Name:         cmdBackendDisable,
				Usage:        "Disable cloud backend (see also: 'ais config cluster backend')",
				ArgsUsage:    cloudProviderArg,
				Action:       backendDisableHandler,
				BashComplete: suggestCloudProvider,
			},
			{
				Name:         cmdCheckLock,
				Usage:        "Check object lock status (read/write/unlocked)",
				ArgsUsage:    optionalPrefixArgument,
				Flags:        []cli.Flag{verbObjPrefixFlag, maxPagesFlag, objLimitFlag, pageSizeFlag, encodeObjnameFlag},
				Action:       checkObjectLockHandler,
				BashComplete: bucketCompletions(bcmplop{separator: true}),
			},
		},
	}
)

func loadLomCacheHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	xargs := xact.ArgsMsg{Kind: apc.ActLoadLomCache, Bck: bck}
	return startXaction(c, &xargs, "")
}

func removeNodeFromSmap(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	if node.IsProxy() {
		smap, err := getClusterMap(c)
		if err != nil {
			return err // (unlikely)
		}
		if smap.IsPrimary(node) {
			return fmt.Errorf("%s is primary (cannot remove the primary node)", sname)
		}
	}
	return api.RemoveNodeUnsafe(apiBP, node.ID())
}

func randNode(c *cli.Context) error {
	var (
		si        *meta.Snode
		smap, err = getClusterMap(c)
	)
	if err != nil {
		return err
	}
	if c.NArg() > 0 && c.Args().Get(0) == apc.Proxy {
		si, err = smap.GetRandProxy(false) // _not_ excluding primary
	} else {
		si, err = smap.GetRandTarget()
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, si.ID())
	return nil
}

func randMountpath(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	tsi, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	if tsi.IsProxy() {
		return fmt.Errorf("%s is a 'proxy' (expecting 'target')", sname)
	}
	daeStatus, err := _status(tsi)
	if err != nil {
		return V(err)
	}
	cdf := daeStatus.Node.Tcdf
	for mpath := range cdf.Mountpaths {
		fmt.Fprintln(c.App.Writer, mpath)
		break
	}
	return nil
}

func rotateLogs(c *cli.Context) error {
	node, sname, err := arg0Node(c)
	if err != nil {
		return err
	}
	// 1. node
	if node != nil {
		if err := api.RotateLogs(apiBP, node.ID()); err != nil {
			return V(err)
		}
		actionDone(c, sname+": rotated logs")
		return nil
	}
	// 2. or cluster
	if err := api.RotateClusterLogs(apiBP); err != nil {
		return V(err)
	}
	actionDone(c, "cluster: rotated all logs")
	return nil
}

func backendEnableHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	cloudProvider := c.Args().Get(0)
	if err := api.EnableBackend(apiBP, cloudProvider); err != nil {
		return err
	}
	actionDone(c, "cluster: enabled "+cloudProvider+" backend")
	return nil
}

func backendDisableHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	cloudProvider := c.Args().Get(0)
	if err := api.DisableBackend(apiBP, cloudProvider); err != nil {
		return err
	}
	actionDone(c, "cluster: disabled "+cloudProvider+" backend")
	return nil
}

func checkObjectLockHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}

	// parse
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, true)
	if err != nil {
		return err
	}
	var warned bool
	objName = warnEscapeObjName(c, objName, &warned)

	// prefix
	var prefix string
	if flagIsSet(c, verbObjPrefixFlag) {
		prefix = parseStrFlag(c, verbObjPrefixFlag)
	}
	if strings.HasSuffix(objName, "/") {
		if prefix != "" && prefix != objName {
			return fmt.Errorf("prefix %q vs prefix %q ambiguity", prefix, objName)
		}
		prefix = objName
	}

	// many
	if prefix != "" || objName == "" {
		return _checkLockMany(c, bck, prefix, uri)
	}

	// one
	lockState, err := _checkLockOne(bck, objName)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s: %s\n", uri, lockState)
	return nil
}

func _checkLockMany(c *cli.Context, bck cmn.Bck, prefix, uri string) error {
	type res struct {
		Name   string
		Status string
	}
	var (
		pageIdx int
		msg     = &apc.LsoMsg{Prefix: prefix, Props: apc.GetPropsName}
	)
	pageSize, maxPages, limit, errN := setLsoPage(c, bck)
	if errN != nil {
		return errN
	}
	msg.PageSize = pageSize
	toShow := int(limit) // countdown

	for {
		// next page
		lst, err := api.ListObjectsPage(apiBP, bck, msg, api.ListArgs{})
		if err != nil {
			return lsoErr(msg, err)
		}

		if len(lst.Entries) == 0 {
			if pageIdx == 0 {
				if prefix == "" || prefix == "/" {
					fmt.Fprintln(c.App.Writer, "No objects")
				} else {
					fmt.Fprintf(c.App.Writer, "No objects with prefix %q\n", uri)
				}
			}
			return nil
		}

		// handle page
		var (
			out     = lst.Entries
			results []res
		)
		if limit > 0 && toShow < len(lst.Entries) {
			out = lst.Entries[:toShow]
		}
		for _, entry := range out {
			lockState, err := _checkLockOne(bck, entry.Name)
			if err != nil {
				warn := fmt.Sprintf("%s: %v", bck.Cname(entry.Name), err)
				actionWarn(c, warn)
				continue
			}
			results = append(results, res{Name: bck.Cname(entry.Name), Status: lockState})
		}

		// print page
		if pageIdx > 0 {
			fmt.Fprintln(c.App.Writer, "\n"+fblue("Page "+strconv.Itoa(pageIdx+1)), "=========")
		}

		if err := teb.Print(results, teb.ObjLockTmpl); err != nil {
			return err
		}

		// loop
		if msg.ContinuationToken == "" {
			return nil
		}
		pageIdx++
		if maxPages > 0 && pageIdx >= int(maxPages) {
			return nil
		}
		if limit > 0 {
			toShow -= len(lst.Entries)
			if toShow <= 0 {
				return nil
			}
		}
	}
}

func _checkLockOne(bck cmn.Bck, objName string) (string, error) {
	lockState, err := api.CheckObjectLock(apiBP, bck, objName)
	if err != nil {
		return "", V(err)
	}
	switch lockState {
	case apc.LockNone:
		return "unlocked", nil
	case apc.LockRead:
		return "read-locked", nil
	case apc.LockWrite:
		return "write-locked", nil
	default:
		return "", fmt.Errorf("unexpected return from 'api.CheckObjectLock': %d", lockState)
	}
}
