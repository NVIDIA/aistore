// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
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

	showNBIUsage = "Show bucket inventory,\n" +
		indent1 + "e.g.:\n" +
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
			nbiPagesPerChunkFlag,
			nbiMaxEntriesPerChunkFlag,
			nbiForceFlag,
		},
		commandRemove: {
			nbiNameFlag,
		},
		commandShow: {
			nbiNameFlag,
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
		ArgsUsage:    bucketArgument,
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
			cmdShowNBI, // TODO -- FIXME: add `ais show nbi` alias
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
	if flagIsSet(c, nbiPagesPerChunkFlag) {
		a := parseIntFlag(c, nbiPagesPerChunkFlag)
		msg.PagesPerChunk = int64(a)
	}
	if flagIsSet(c, nbiMaxEntriesPerChunkFlag) {
		a := parseIntFlag(c, nbiMaxEntriesPerChunkFlag)
		msg.MaxEntriesPerChunk = int64(a)
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
	_ = c
	return errors.New(NIY) // TODO: not implemented yet
}
