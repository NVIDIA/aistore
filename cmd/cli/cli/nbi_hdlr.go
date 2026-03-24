// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

const (
	createNBIUsage = "Create bucket inventory (snapshot) for subsequent fast listing,\n" +
		indent1 + "e.g.:\n" +
		indent1 + "\t* ais nbi create s3://abc\t- create inventory with default (name, size) properties;\n" +
		indent1 + "\t* ais nbi create s3://abc --inv-name my-first-inventory\t- same, with a custom inventory name;\n" +
		indent1 + "\t* ais nbi create s3://abc --prefix images/\t- inventory only objects under 'images/';\n" +
		indent1 + "\t* ais nbi create s3://abc --all\t- inventory with all object properties;\n" +
		indent1 + "\t* ais nbi create s3://abc --name-only\t- lightweight: object names only;\n" +
		indent1 + "\t* ais nbi create ais://@remais/xyz --inv-pages 2\t- remote AIS, with 2 pages per chunk."

	removeNBIUsage = "Remove bucket inventory,\n" +
		indent1 + "e.g.:\n" +
		indent1 + "\t* ais nbi rm s3://abc\t- remove inventory for the bucket;\n" +
		indent1 + "\t* ais nbi rm s3://abc --inv-name my-first-inventory\t- remove specific named inventory."

	showNBIUsage = "Show bucket inventory or all matching inventories,\n" +
		indent1 + "e.g.:\n" +
		indent1 + "\t* ais nbi show\t- show all inventories in the cluster;\n" +
		indent1 + "\t* ais nbi show s3:\t- show inventories for all s3:// buckets;\n" +
		indent1 + "\t* ais nbi show s3://abc\t- show inventory details for the bucket;\n" +
		indent1 + "\t* ais nbi show s3://abc --inv-name my-first-inventory\t- show specific named inventory."
)

// flags
var (
	nbiCmdFlags = map[string][]cli.Flag{
		commandCreate: {
			nbiNameFlag,
			invPrefixFlag,
			nameOnlyFlag,
			allPropsFlag,
			nbiNamesPerChunkFlag,
			nbiForceFlag,
		},
		commandRemove: {
			nbiNameFlag,
		},
		commandShow: {
			nbiNameFlag,
			verboseFlag,
		},
	}
)

// commands
var (
	cmdCreateNBI = cli.Command{
		Name:         commandCreate,
		Usage:        createNBIUsage,
		ArgsUsage:    bucketArgument,
		Flags:        sortFlags(nbiCmdFlags[commandCreate]),
		Action:       createNBIHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}
	cmdRemoveNBI = cli.Command{
		Name:         commandRemove,
		Usage:        removeNBIUsage,
		ArgsUsage:    bucketArgument,
		Flags:        sortFlags(nbiCmdFlags[commandRemove]),
		Action:       removeNBIHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}
	cmdShowNBI = cli.Command{
		Name:         commandShow,
		Usage:        showNBIUsage,
		ArgsUsage:    optionalBucketArgument,
		Flags:        sortFlags(nbiCmdFlags[commandShow]),
		Action:       showNBIHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}

	// top-level
	nbiCmd = cli.Command{
		Name:  commandNBI,
		Usage: "Manage native bucket inventory (NBI) - create (to facilitate faster list-objects), show, or remove bucket inventory snapshots",
		Subcommands: []cli.Command{
			cmdCreateNBI,
			cmdRemoveNBI,
			cmdShowNBI,
		},
	}
)

//
// createNBIHandler
//

func createNBIHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}

	// msg
	msg := &apc.CreateNBIMsg{}
	msg.SetFlag(apc.LsNoDirs)

	if flagIsSet(c, invPrefixFlag) {
		msg.Prefix = parseStrFlag(c, invPrefixFlag)
	}

	switch {
	case flagIsSet(c, nameOnlyFlag):
		msg.SetFlag(apc.LsNameOnly)
		msg.Props = apc.GetPropsName // abs. minimum
	case flagIsSet(c, allPropsFlag):
		msg.AddProps(apc.GetPropsAll...) // all supported props
	default:
		msg.AddProps(apc.GetPropsName, apc.GetPropsSize, apc.GetPropsCached) // NOTE default
	}

	// inv name
	if flagIsSet(c, nbiNameFlag) {
		msg.Name = parseStrFlag(c, nbiNameFlag)
		if err := cos.CheckAlphaPlus(msg.Name, "inventory name"); err != nil {
			return err
		}
	}

	// advanced
	if flagIsSet(c, nbiNamesPerChunkFlag) {
		a := parseIntFlag(c, nbiNamesPerChunkFlag)
		msg.NamesPerChunk = int64(a)
	}
	msg.Force = flagIsSet(c, nbiForceFlag)

	// do
	xid, err := api.CreateNBI(apiBP, bck, msg)
	if err != nil {
		return V(err)
	}

	actionDone(c, fmt.Sprintf("Creating inventory %s. %s", bck.Cname(""), toMonitorMsg(c, xid, "")))
	return nil
}

//
// removeNBIHandler
//

func removeNBIHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}

	// bucket
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}

	// inv name
	var (
		s       string
		invName = parseStrFlag(c, nbiNameFlag)
	)
	if invName != "" {
		if err := cos.CheckAlphaPlus(invName, "inventory name"); err != nil {
			return err
		}
		s = " '" + invName + "'"
	}
	if err := api.DestroyNBI(apiBP, bck, invName); err != nil {
		return V(err)
	}

	actionDone(c, fmt.Sprintf("Removed inventory%s for bucket %s", s, bck.String()))
	return nil
}

//
// showNBIHandler
//

func showNBIHandler(c *cli.Context) error {
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}

	invName := parseStrFlag(c, nbiNameFlag)
	if invName != "" {
		if err := cos.CheckAlphaPlus(invName, "inventory name"); err != nil {
			return err
		}
	}

	// optional bucket
	var (
		bck cmn.Bck
		err error
	)
	if c.NArg() == 1 {
		var (
			objName string
			opts    = cmn.ParseURIOpts{IsQuery: true}
			uri     = c.Args().Get(0)
		)
		uri = preparseBckObjURI(uri)
		bck, objName, err = cmn.ParseBckObjectURI(uri, opts)
		if err != nil {
			return err
		}
		if objName != "" {
			return objectNameArgNotExpected(c, objName)
		}
	}

	// do
	infos, err := api.GetNBI(apiBP, bck, invName)
	if err != nil {
		return V(err)
	}

	if len(infos) == 0 {
		switch {
		case bck.IsEmpty() && invName == "":
			fmt.Fprintln(c.App.Writer, "No bucket inventories in the cluster")
		case bck.IsEmpty() && invName != "":
			fmt.Fprintf(c.App.Writer, "No inventory named %q in the cluster\n", invName)
		case invName == "":
			fmt.Fprintln(c.App.Writer, "No inventories for", bck.Cname(""))
		default:
			fmt.Fprintf(c.App.Writer, "No inventory named %q for %s\n", invName, bck.Cname(""))
		}
		return nil
	}

	lst := make([]*apc.NBIInfo, 0, len(infos))
	for _, v := range infos {
		lst = append(lst, v)
	}
	sort.Slice(lst, func(i, j int) bool {
		if lst[i].Bucket == lst[j].Bucket {
			return lst[i].Name < lst[j].Name
		}
		return lst[i].Bucket < lst[j].Bucket
	})

	if flagIsSet(c, verboseFlag) {
		return teb.Print(lst, teb.NBITmplVerbose)
	}
	return teb.Print(lst, teb.NBITmpl)
}
