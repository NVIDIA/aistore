// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles CLI commands that pertain to AIS buckets.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
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
			forceFlag,
		},
		subcmdResetProps: {},
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
			fastFlag,
			validateFlag,
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
		Action:       summaryBucketHandler,
		BashComplete: bucketCompletions(),
	}

	bucketCmd = cli.Command{
		Name:  commandBucket,
		Usage: "create/destroy buckets, list bucket's content, show existing buckets and their properties",
		Subcommands: []cli.Command{
			bucketCmdList,
			bucketCmdSummary,
			makeAlias(showCmdBucket, "", true, commandShow), // alias for `ais show`
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
				ArgsUsage:    "SRC_BUCKET DST_BUCKET",
				Flags:        bucketCmdsFlags[commandCopy],
				Action:       copyBucketHandler,
				BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{}, 0, 2),
			},
			{
				Name:         commandMv,
				Usage:        "move an ais bucket",
				ArgsUsage:    "BUCKET NEW_BUCKET",
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
				Usage:        "evict buckets or objects prefetched from remote buckets",
				ArgsUsage:    optionalObjectsArgument,
				Flags:        bucketCmdsFlags[commandEvict],
				Action:       evictHandler,
				BashComplete: bucketCompletions(bckCompletionsOpts{multiple: true}),
			},
			{
				Name:  subcmdProps,
				Usage: "show, update or reset bucket properties",
				Subcommands: []cli.Command{
					{
						Name:         subcmdSetProps,
						Usage:        "update bucket properties",
						ArgsUsage:    bucketPropsArgument,
						Flags:        bucketCmdsFlags[subcmdSetProps],
						Action:       setPropsHandler,
						BashComplete: bucketCompletions(bckCompletionsOpts{additionalCompletions: []cli.BashCompleteFunc{propCompletions}}),
					},
					{
						Name:         subcmdResetProps,
						Usage:        "reset bucket properties",
						ArgsUsage:    bucketPropsArgument,
						Flags:        bucketCmdsFlags[subcmdResetProps],
						Action:       resetPropsHandler,
						BashComplete: bucketCompletions(bckCompletionsOpts{additionalCompletions: []cli.BashCompleteFunc{propCompletions}}),
					},
					makeAlias(showCmdBucket, "", true, commandShow),
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
		if err := createBucket(c, bck, props); err != nil {
			return err
		}
	}
	return nil
}

func checkObjectHealth(c *cli.Context, queryBcks cmn.QueryBcks) (err error) {
	type bucketHealth struct {
		Name          string
		ObjectCnt     uint64
		Misplaced     uint64
		MissingCopies uint64
	}
	bckList, err := api.ListBuckets(defaultAPIParams, queryBcks)
	if err != nil {
		return
	}
	bckSums := make([]*bucketHealth, 0)
	msg := &cmn.SelectMsg{Flags: cmn.SelectMisplaced}
	msg.AddProps(cmn.GetPropsCopies, cmn.GetPropsCached)
	for _, bck := range bckList {
		if queryBcks.Name != "" && !queryBcks.Equal(bck) {
			continue
		}
		var (
			p       *cmn.BucketProps
			objList *cmn.BucketList
			obj     *cmn.BucketEntry
		)
		if p, err = headBucket(bck); err != nil {
			return
		}
		copies := int16(p.Mirror.Copies)
		stats := &bucketHealth{Name: bck.String()}
		objList, err = api.ListObjects(defaultAPIParams, bck, msg, 0)
		if err != nil {
			return err
		}

		updateStats := func(obj *cmn.BucketEntry) {
			if obj == nil {
				return
			}
			stats.ObjectCnt++
			if !obj.IsStatusOK() {
				stats.Misplaced++
			} else if obj.CheckExists() && p.Mirror.Enabled && obj.Copies < copies {
				stats.MissingCopies++
			}
		}

		for _, entry := range objList.Entries {
			if obj == nil {
				obj = entry
				continue
			}
			if obj.Name == entry.Name {
				if entry.IsStatusOK() {
					obj = entry
				}
				continue
			}
			updateStats(obj)
			obj = entry
		}
		updateStats(obj)

		bckSums = append(bckSums, stats)
	}
	return templates.DisplayOutput(bckSums, c.App.Writer, templates.BucketSummaryValidateTmpl)
}

func showObjectHealth(c *cli.Context) (err error) {
	queryBcks, err := parseQueryBckURI(c, c.Args().First())
	if err != nil {
		return err
	}

	fValidate := func() error {
		return checkObjectHealth(c, queryBcks)
	}
	return cmn.WaitForFunc(fValidate, longCommandTime)
}

func showBucketSizes(c *cli.Context) error {
	queryBcks, err := parseQueryBckURI(c, c.Args().First())
	if err != nil {
		return err
	}

	if err := updateLongRunParams(c); err != nil {
		return err
	}

	summaries, err := fetchSummaries(queryBcks, flagIsSet(c, fastFlag), flagIsSet(c, cachedFlag))
	if err != nil {
		return err
	}
	tmpl := templates.BucketsSummariesTmpl
	if flagIsSet(c, fastFlag) {
		tmpl = templates.BucketsSummariesFastTmpl
	}
	return templates.DisplayOutput(summaries, c.App.Writer, tmpl)
}

func summaryBucketHandler(c *cli.Context) (err error) {
	if flagIsSet(c, validateFlag) {
		return showObjectHealth(c)
	}

	return showBucketSizes(c)
}

func copyBucketHandler(c *cli.Context) (err error) {
	bckFrom, bckTo, err := parseBcks(c)
	if err != nil {
		return err
	}
	if !bckFrom.IsAIS() {
		return incorrectUsageMsg(c, "only AIS bucket can be a source")
	}
	if bckFrom.Equal(bckTo) {
		return incorrectUsageMsg(c, errFmtSameBucket, commandCopy, bckTo)
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

	return copyBucket(c, bckFrom, bckTo, msg)
}

func mvBucketHandler(c *cli.Context) error {
	bckFrom, bckTo, err := parseBcks(c)
	if err != nil {
		return err
	}
	if bckFrom.Equal(bckTo) {
		return incorrectUsageMsg(c, errFmtSameBucket, commandMv, bckTo)
	}

	return mvBucket(c, bckFrom, bckTo)
}

func removeBucketHandler(c *cli.Context) (err error) {
	var buckets []cmn.Bck
	if buckets, err = bucketsFromArgsOrEnv(c); err != nil {
		return
	}
	return destroyBuckets(c, buckets)
}

func evictHandler(c *cli.Context) (err error) {
	printDryRunHeader(c)

	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing bucket name")
	}

	// Bucket argument provided by the user.
	if c.NArg() == 1 {
		uri := c.Args().First()
		bck, objName, err := parseBckObjectURI(c, uri, true)
		if err != nil {
			return err
		}
		if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
			if objName != "" {
				return incorrectUsageMsg(c, "object name (%q) not supported when list or template flag provided", objName)
			}
			// List or range operation on a given bucket.
			return listOrRangeOp(c, commandEvict, bck)
		}
		if objName == "" {
			// Operation on a given bucket.
			return evictBucket(c, bck)
		}

		// Evict a single object from remote bucket - multiObjOp will handle.
	}

	// List and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q are invalid when object names provided",
			strings.Join([]string{listFlag.Name, templateFlag.Name}, ", "))
	}

	// Object argument(s) given by the user; operation on given object(s).
	return multiObjOp(c, commandEvict)
}

func resetPropsHandler(c *cli.Context) (err error) {
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return err
	}
	return resetBucketProps(c, bck)
}

func setPropsHandler(c *cli.Context) (err error) {
	var origProps *cmn.BucketProps
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return err
	}
	if origProps, err = headBucket(bck); err != nil {
		return err
	}

	updateProps, err := parseBckPropsFromContext(c)
	if err != nil {
		return err
	}
	updateProps.Force = flagIsSet(c, forceFlag)

	newProps := origProps.Clone()
	newProps.Apply(updateProps)
	if newProps.Equal(origProps) { // Apply props and check for change
		displayPropsEqMsg(c, bck)
		return nil
	}

	if err = setBucketProps(c, bck, updateProps); err != nil {
		helpMsg := fmt.Sprintf("To show bucket properties, run \"%s %s %s BUCKET -v\"",
			cliName, commandShow, subcmdShowBucket)
		return newAdditionalInfoError(err, helpMsg)
	}

	showDiff(c, origProps, newProps)
	return nil
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
	var (
		origKV = bckPropList(origProps, true)
		newKV  = bckPropList(newProps, true)
	)
	for idx, prop := range newKV {
		if origKV[idx].Value != prop.Value {
			fmt.Fprintf(c.App.Writer, "%q set to: %q (was: %q)\n", prop.Name, prop.Value, origKV[idx].Value)
		}
	}
}

func defaultListHandler(c *cli.Context) (err error) {
	queryBcks, err := parseQueryBckURI(c, c.Args().First())
	if err != nil {
		return err
	}

	if queryBcks.Name == "" {
		return listBuckets(c, queryBcks)
	}
	return listObjects(c, cmn.Bck(queryBcks))
}
