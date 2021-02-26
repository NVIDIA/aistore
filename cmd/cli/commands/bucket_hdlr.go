// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles CLI commands that pertain to AIS buckets.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const errFmtSameBucket = "cannot %s bucket %q onto itself"

var (
	bucketCmdsFlags = map[string][]cli.Flag{
		commandRemove: {ignoreErrorFlag},
		commandMv:     {waitFlag},
		commandCreate: {
			ignoreErrorFlag,
			bucketPropsFlag,
			forceFlag,
		},
		commandCopy: {
			cpBckDryRunFlag,
			cpBckPrefixFlag,
			waitFlag,
		},
		commandEvict: append(
			baseLstRngFlags,
			dryRunFlag,
			keepMDFlag,
		),
		subcmdSetProps: {
			resetFlag,
			forceFlag,
		},
		commandList: {
			regexFlag,
			templateFlag,
			prefixFlag,
			pageSizeFlag,
			objPropsFlag,
			objLimitFlag,
			showUnmatchedFlag,
			allItemsFlag,
			noHeaderFlag,
			pagedFlag,
			maxPagesFlag,
			startAfterFlag,
			cachedFlag,
		},
		subcmdSummary: {
			cachedFlag,
			allFlag,
			verboseFlag,
		},
	}

	// define separately to allow for aliasing (see alias_hdlr.go)
	bucketCmdList = cli.Command{
		Name:         commandList,
		Usage:        "list buckets and their objects",
		Action:       defaultListHandler,
		ArgsUsage:    listCommandArgument,
		Flags:        bucketCmdsFlags[commandList],
		BashComplete: bucketCompletions(bckCompletionsOpts{withProviders: true}),
	}

	bucketCmdSummary = cli.Command{
		Name:         subcmdSummary,
		Usage:        "fetch and display bucket summary",
		ArgsUsage:    optionalBucketArgument,
		Flags:        bucketCmdsFlags[subcmdSummary],
		Action:       showBucketHandler,
		BashComplete: bucketCompletions(),
	}

	bucketCmds = []cli.Command{
		{
			Name:  commandBucket,
			Usage: "create/destroy buckets, list bucket's content, show existing buckets and their properties",
			Subcommands: []cli.Command{
				bucketCmdList,
				bucketCmdSummary,
				{
					Name:      commandCreate,
					Usage:     "create ais buckets",
					ArgsUsage: bucketsArgument,
					Flags:     bucketCmdsFlags[commandCreate],
					Action:    createBucketHandler,
				},
				{
					Name:         commandCopy,
					Usage:        "copy ais buckets in the cluster",
					ArgsUsage:    "SRC_BUCKET_NAME DST_BUCKET_NAME",
					Flags:        bucketCmdsFlags[commandCopy],
					Action:       copyBucketHandler,
					BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{}, 0, 2),
				},
				{
					Name:         commandMv,
					Usage:        "move an ais bucket",
					ArgsUsage:    "BUCKET_NAME NEW_BUCKET_NAME",
					Flags:        bucketCmdsFlags[commandMv],
					Action:       mvBucketHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, false /* separator */, cmn.ProviderAIS),
				},
				{
					Name:      commandRemove,
					Usage:     "remove ais buckets",
					ArgsUsage: bucketsArgument,
					Flags:     bucketCmdsFlags[commandRemove],
					Action:    removeBucketHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{
						multiple: true, provider: cmn.ProviderAIS,
					}),
				},
				{
					Name:         commandEvict,
					Usage:        "evict buckets or objects prefetched from cloud buckets",
					ArgsUsage:    optionalObjectsArgument,
					Flags:        bucketCmdsFlags[commandEvict],
					Action:       evictHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{multiple: true}),
				},
				{
					Name:         subcmdSetProps,
					Usage:        "update or reset bucket properties",
					ArgsUsage:    bucketPropsArgument,
					Flags:        bucketCmdsFlags[subcmdSetProps],
					Action:       setPropsHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{additionalCompletions: []cli.BashCompleteFunc{propCompletions}}),
				},
			},
		},
	}
)

func createBucketHandler(c *cli.Context) (err error) {
	var props *cmn.BucketPropsToUpdate
	if flagIsSet(c, bucketPropsFlag) {
		propSingleBck, err := parseBckPropsFromContext(c)
		if err != nil {
			return err
		}
		props = propSingleBck
		props.Force = flagIsSet(c, forceFlag)
	}
	buckets, err := bucketsFromArgsOrEnv(c)
	if err != nil {
		return err
	}
	for _, bck := range buckets {
		err = createBucket(c, bck, props)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyBucketHandler(c *cli.Context) (err error) {
	bucketName, newBucketName, err := getOldNewBucketName(c)
	if err != nil {
		return err
	}
	fromBck, err := parseBckURI(c, bucketName)
	if err != nil {
		return err
	}
	if fromBck.Provider != "" && !fromBck.IsAIS() {
		return incorrectUsageMsg(c, "only AIS bucket can be a source")
	}
	toBck, err := parseBckURI(c, newBucketName)
	if err != nil {
		return err
	}

	if fromBck.Equal(toBck) {
		return incorrectUsageMsg(c, errFmtSameBucket, commandCopy, fromBck)
	}

	msg := &cmn.CopyBckMsg{
		Prefix: parseStrFlag(c, cpBckPrefixFlag),
		DryRun: flagIsSet(c, cpBckDryRunFlag),
	}

	if msg.DryRun {
		// TODO: once IC is integrated with copy-bck stats, show something more relevant, like stream of object names
		// with destination which they would have been copied to. Then additionally, make output consistent with etl
		// dry-run output.
		fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
	}

	return copyBucket(c, fromBck, toBck, msg)
}

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
		return incorrectUsageMsg(c, errFmtSameBucket, commandMv, bck)
	}

	return mvBucket(c, bck, newBck)
}

func removeBucketHandler(c *cli.Context) (err error) {
	var buckets []cmn.Bck
	if buckets, err = bucketsFromArgsOrEnv(c); err != nil {
		return
	}
	return destroyBuckets(c, buckets)
}

func evictHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		objName string
	)
	printDryRunHeader(c)

	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing bucket name")
	}

	// bucket argument given by the user
	if c.NArg() == 1 {
		objPath := c.Args().First()
		if isWebURL(objPath) {
			bck = parseURLtoBck(objPath)
		} else if bck, objName, err = parseBckObjectURI(c, objPath); err != nil {
			return
		}

		if bck.IsAIS() {
			return fmt.Errorf("cannot evict ais buckets (the operation applies to Cloud buckets only)")
		}

		if bck, _, err = validateBucket(c, bck, "", false); err != nil {
			return err
		}

		if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
			if objName != "" {
				return incorrectUsageMsg(c, "object name (%q) not supported when list or template flag provided", objName)
			}
			// list or range operation on a given bucket
			return listOrRangeOp(c, commandEvict, bck)
		}

		if objName == "" {
			// operation on a given bucket
			return evictBucket(c, bck)
		}

		// evict single object from cloud bucket - multiObjOp will handle
	}

	// list and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q are invalid when object names provided", strings.Join([]string{listFlag.Name, templateFlag.Name}, ", "))
	}

	// object argument(s) given by the user; operation on given object(s)
	return multiObjOp(c, commandEvict)
}

func setPropsHandler(c *cli.Context) (err error) {
	var origProps *cmn.BucketProps
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return
	}

	if bck, origProps, err = validateBucket(c, bck, "", false); err != nil {
		return
	}

	if flagIsSet(c, resetFlag) { // ignores all arguments, just resets bucket origProps
		return resetBucketProps(c, bck)
	}

	updateProps, err := parseBckPropsFromContext(c)
	if err != nil {
		return
	}
	updateProps.Force = flagIsSet(c, forceFlag)

	newProps := origProps.Clone()
	newProps.Apply(updateProps)
	if newProps.Equal(origProps) { // Apply props and check for change
		displayPropsEqMsg(c, bck)
		return
	}

	if err = setBucketProps(c, bck, updateProps); err != nil {
		helpMsg := fmt.Sprintf("To show bucket properties, run \"%s %s %s BUCKET_NAME -v\"",
			cliName, commandShow, subcmdShowBckProps)
		return newAdditionalInfoError(err, helpMsg)
	}

	showDiff(c, origProps, newProps)
	return
}

func displayPropsEqMsg(c *cli.Context, bck cmn.Bck) {
	args := c.Args().Tail()
	if len(args) == 1 && !isJSON(args[0]) {
		fmt.Fprintf(c.App.Writer, "Bucket %q: property %q, nothing to do\n", bck, args[0])
		return
	}
	fmt.Fprintf(c.App.Writer, "Bucket %q already has the set props, nothing to do\n", bck)
}

func showDiff(c *cli.Context, origProps, newProps *cmn.BucketProps) {
	origKV, _ := bckPropList(origProps, true)
	newKV, _ := bckPropList(newProps, true)
	for idx, prop := range newKV {
		if origKV[idx].Value != prop.Value {
			fmt.Fprintf(c.App.Writer, "%q set to:%q (was:%q)\n", prop.Name, prop.Value, origKV[idx].Value)
		}
	}
}

func defaultListHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		objPath = c.Args().First()
	)
	if isWebURL(objPath) {
		bck = parseURLtoBck(objPath)
	} else if bck, err = parseBckURI(c, objPath, true /*query*/); err != nil {
		return
	}

	if bck, _, err = validateBucket(c, bck, "ls", true); err != nil {
		return
	}

	if bck.Name == "" {
		return listBucketNames(c, cmn.QueryBcks(bck))
	}

	bck.Name = strings.TrimSuffix(bck.Name, "/")
	return listObjects(c, bck)
}
