// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS buckets.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

const (
	errFmtSameBucket = "cannot %s bucket %q onto itself"
	errFmtExclusive  = "flags %q and %q are mutually exclusive"
)

var (
	// flags
	bucketCmdsFlags = map[string][]cli.Flag{
		commandRemove: {
			ignoreErrorFlag,
			yesFlag,
		},
		commandRename: {waitFlag},
		commandCreate: {
			ignoreErrorFlag,
			bucketPropsFlag,
			forceFlag,
		},
		commandCopy: {
			cpBckDryRunFlag,
			cpBckPrefixFlag,
			templateFlag,
			listFlag,
			waitFlag,
			continueOnErrorFlag,
			forceFlag,
		},
		commandEvict: append(
			baseLstRngFlags,
			dryRunFlag,
			keepMDFlag,
			verboseFlag,
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
			objPropsLsFlag,
			objLimitFlag,
			showUnmatchedFlag,
			allObjsOrBcksFlag,
			noHeaderFlag,
			noFooterFlag,
			pagedFlag,
			maxPagesFlag,
			startAfterFlag,
			listObjCachedFlag,
			listAnonymousFlag,
			listArchFlag,
			nameOnlyFlag,
			sizeInBytesFlag,
			bckSummaryFlag,
		},

		subcmdSummary: {
			listObjCachedFlag,
			allObjsOrBcksFlag,
			sizeInBytesFlag,
			validateSummaryFlag,
			verboseFlag,
		},
		subcmdLRU: {
			enableFlag,
			disableFlag,
		},
	}

	// commands
	bucketsObjectsCmdList = cli.Command{
		Name:         commandList,
		Usage:        "list buckets, objects in buckets, and files in objects formatted as archives",
		Action:       listAnyHandler,
		ArgsUsage:    listAnyCommandArgument,
		Flags:        bucketCmdsFlags[commandList],
		BashComplete: bucketCompletions(bckCompletionsOpts{withProviders: true}),
	}

	bucketCmdSummary = cli.Command{
		Name:         subcmdSummary,
		Usage:        "generate and display bucket summary",
		ArgsUsage:    optionalBucketArgument,
		Flags:        bucketCmdsFlags[subcmdSummary],
		Action:       summaryBucketHandler,
		BashComplete: bucketCompletions(),
	}

	bucketCmdLRU = cli.Command{
		Name:         subcmdLRU,
		Usage:        "show LRU settings for a bucket; enable or disable LRU eviction",
		ArgsUsage:    optionalBucketArgument,
		Flags:        bucketCmdsFlags[subcmdLRU],
		Action:       lruBucketHandler,
		BashComplete: bucketCompletions(),
	}
	bucketObjCmdEvict = cli.Command{
		Name:         commandEvict,
		Usage:        "evict objects from buckets and entire buckets",
		ArgsUsage:    optionalObjectsArgument,
		Flags:        bucketCmdsFlags[commandEvict],
		Action:       evictHandler,
		BashComplete: bucketCompletions(bckCompletionsOpts{multiple: true}),
	}

	bucketCmd = cli.Command{
		Name:  commandBucket,
		Usage: "create/destroy buckets, list bucket's content, show existing buckets and their properties",
		Subcommands: []cli.Command{
			bucketsObjectsCmdList,
			bucketCmdSummary,
			bucketCmdLRU,
			bucketObjCmdEvict,
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
				Usage:        "copy bucket",
				ArgsUsage:    "SRC_BUCKET DST_BUCKET",
				Flags:        bucketCmdsFlags[commandCopy],
				Action:       copyBucketHandler,
				BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{}, 0, 2),
			},
			{
				Name:      commandRename,
				Usage:     "rename/move ais bucket",
				ArgsUsage: "BUCKET NEW_BUCKET",
				Flags:     bucketCmdsFlags[commandRename],
				Action:    mvBucketHandler,
				BashComplete: oldAndNewBucketCompletions(
					[]cli.BashCompleteFunc{},
					false, /* separator */
					apc.AIS,
				),
			},
			{
				Name:      commandRemove,
				Usage:     "remove ais buckets",
				ArgsUsage: bucketsArgument,
				Flags:     bucketCmdsFlags[commandRemove],
				Action:    removeBucketHandler,
				BashComplete: bucketCompletions(bckCompletionsOpts{
					multiple: true, provider: apc.AIS,
				}),
			},
			{
				Name:   subcmdProps,
				Usage:  "show, update or reset bucket properties",
				Action: showBckPropsHandler,
				Subcommands: []cli.Command{
					{
						Name:      subcmdSetProps,
						Usage:     "update bucket properties",
						ArgsUsage: bucketPropsArgument,
						Flags:     bucketCmdsFlags[subcmdSetProps],
						Action:    setPropsHandler,
						BashComplete: bucketCompletions(
							bckCompletionsOpts{additionalCompletions: []cli.BashCompleteFunc{bpropCompletions}},
						),
					},
					{
						Name:      subcmdResetProps,
						Usage:     "reset bucket properties",
						ArgsUsage: bucketPropsArgument,
						Flags:     bucketCmdsFlags[subcmdResetProps],
						Action:    resetPropsHandler,
						BashComplete: bucketCompletions(
							bckCompletionsOpts{additionalCompletions: []cli.BashCompleteFunc{bpropCompletions}},
						),
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
		Bck           cmn.Bck
		ObjectCnt     uint64
		Misplaced     uint64
		MissingCopies uint64
	}
	bckList, err := api.ListBuckets(apiBP, queryBcks, apc.FltPresent)
	if err != nil {
		return
	}
	bckSums := make([]*bucketHealth, 0)
	msg := &apc.ListObjsMsg{Flags: apc.LsAll}
	msg.AddProps(apc.GetPropsCopies, apc.GetPropsCached)
	for _, bck := range bckList {
		if queryBcks.Name != "" && !queryBcks.Equal(&bck) {
			continue
		}
		var (
			p       *cmn.BucketProps
			objList *cmn.ListObjects
			obj     *cmn.ObjEntry
		)
		if p, err = headBucket(bck, true /* don't add */); err != nil {
			return
		}
		copies := int16(p.Mirror.Copies)
		stats := &bucketHealth{Bck: bck}
		objList, err = api.ListObjects(apiBP, bck, msg, 0)
		if err != nil {
			return err
		}

		updateStats := func(obj *cmn.ObjEntry) {
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
	return tmpls.DisplayOutput(bckSums, c.App.Writer, tmpls.BucketSummaryValidateTmpl, nil, false)
}

func summaryBucketHandler(c *cli.Context) (err error) {
	if flagIsSet(c, validateSummaryFlag) {
		return showMisplacedAndMore(c)
	}

	return showBucketSummary(c)
}

func showBucketSummary(c *cli.Context) error {
	queryBcks, err := parseQueryBckURI(c, c.Args().First())
	if err != nil {
		return err
	}
	if err := updateLongRunParams(c); err != nil {
		return err
	}

	summaries, err := getSummaries(queryBcks, flagIsSet(c, listObjCachedFlag), flagIsSet(c, allObjsOrBcksFlag))
	if err != nil {
		return err
	}

	var altMap template.FuncMap
	if flagIsSet(c, sizeInBytesFlag) {
		altMap = tmpls.AltFuncMapSizeBytes()
	}
	return tmpls.DisplayOutput(summaries, c.App.Writer, tmpls.BucketsSummariesTmpl, altMap, false)
}

// NOTE: always execute the "slow" version of the bucket-summary (compare with `listBuckets`)
func getSummaries(qbck cmn.QueryBcks, cachedObjs, allBuckets bool) (summaries cmn.BckSummaries, err error) {
	fDetails := func() (err error) {
		msg := &apc.BckSummMsg{ObjCached: cachedObjs, BckPresent: !allBuckets, Fast: false}
		summaries, err = api.GetBucketSummary(apiBP, qbck, msg)
		return
	}
	err = cmn.WaitForFunc(fDetails, longCommandTime)
	return
}

func showMisplacedAndMore(c *cli.Context) (err error) {
	queryBcks, err := parseQueryBckURI(c, c.Args().First())
	if err != nil {
		return err
	}

	fValidate := func() error {
		return checkObjectHealth(c, queryBcks)
	}
	return cmn.WaitForFunc(fValidate, longCommandTime)
}

func fullBckCopy(c *cli.Context, bckFrom, bckTo cmn.Bck) (err error) {
	msg := &apc.CopyBckMsg{
		Prefix: parseStrFlag(c, cpBckPrefixFlag),
		DryRun: flagIsSet(c, cpBckDryRunFlag),
		Force:  flagIsSet(c, forceFlag),
	}

	return copyBucket(c, bckFrom, bckTo, msg)
}

func multiObjBckCopy(c *cli.Context, fromBck, toBck cmn.Bck, listObjs, tmplObjs string, etlID ...string) (err error) {
	operation := "Copying objects"
	if listObjs != "" && tmplObjs != "" {
		return incorrectUsageMsg(c, errFmtExclusive, listFlag.Name, templateFlag.Name)
	}
	var lrMsg cmn.SelectObjsMsg
	if listObjs != "" {
		lrMsg.ObjNames = strings.Split(listObjs, ",")
	} else {
		if _, err = cos.NewParsedTemplate(tmplObjs); err != nil {
			return err
		}
		lrMsg.Template = tmplObjs
	}
	msg := cmn.TCObjsMsg{
		SelectObjsMsg: lrMsg,
		ToBck:         toBck,
	}
	msg.DryRun = flagIsSet(c, cpBckDryRunFlag)
	if flagIsSet(c, etlBucketRequestTimeout) {
		msg.RequestTimeout = cos.Duration(etlBucketRequestTimeout.Value)
	}
	msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)
	var xactID string
	if len(etlID) != 0 {
		msg.ID = etlID[0]
		operation = "ETL objects"
		xactID, err = api.ETLMultiObj(apiBP, fromBck, msg)
	} else {
		xactID, err = api.CopyMultiObj(apiBP, fromBck, msg)
	}
	if err != nil {
		return err
	}
	if !flagIsSet(c, waitFlag) {
		fmt.Fprintf(c.App.Writer, fmtXactStatusCheck, operation, fromBck, toBck, apc.ActCopyBck, toBck)
		return nil
	}
	fmt.Fprintf(c.App.Writer, fmtXactStarted, operation, fromBck, toBck)
	wargs := api.XactReqArgs{ID: xactID, Kind: apc.ActCopyObjects}
	if err = api.WaitForXactionIdle(apiBP, wargs); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, operation, fromBck, toBck)
	} else {
		fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	}
	return err
}

func copyBucketHandler(c *cli.Context) (err error) {
	dryRun := flagIsSet(c, cpBckDryRunFlag)
	bckFrom, bckTo, err := parseBcks(c)
	if err != nil {
		return err
	}
	if bckFrom.Equal(&bckTo) {
		return incorrectUsageMsg(c, errFmtSameBucket, commandCopy, bckTo)
	}

	if dryRun {
		// TODO: once IC is integrated with copy-bck stats, show something more relevant, like stream of object names
		// with destination which they would have been copied to. Then additionally, make output consistent with etl
		// dry-run output.
		fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
	}

	listObjs := parseStrFlag(c, listFlag)
	tmplObjs := parseStrFlag(c, templateFlag)

	// Full bucket copy
	if listObjs == "" && tmplObjs == "" {
		if dryRun {
			fmt.Fprintln(c.App.Writer, "Copying the whole bucket.")
		}
		return fullBckCopy(c, bckFrom, bckTo)
	}

	// Copy matching objects
	if listObjs != "" && tmplObjs != "" {
		return incorrectUsageMsg(c, errFmtExclusive, listFlag.Name, templateFlag.Name)
	}
	if dryRun {
		if listObjs != "" {
			fmt.Fprintf(c.App.Writer, "Copying only objects %q.\n", listObjs)
		} else {
			fmt.Fprintf(c.App.Writer, "Copying the objects that match the pattern %q.\n", tmplObjs)
		}
	}
	return multiObjBckCopy(c, bckFrom, bckTo, listObjs, tmplObjs)
}

func mvBucketHandler(c *cli.Context) error {
	bckFrom, bckTo, err := parseBcks(c)
	if err != nil {
		return err
	}
	if bckFrom.Equal(&bckTo) {
		return incorrectUsageMsg(c, errFmtSameBucket, commandRename, bckTo)
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
				return incorrectUsageMsg(c,
					"object name (%q) cannot be used together with --list and/or --template flags",
					objName)
			}
			// List or range operation on a given bucket.
			return listOrRangeOp(c, commandEvict, bck)
		}
		if objName == "" {
			// Evict entire bucket.
			return evictBucket(c, bck)
		}

		// Evict a single object from remote bucket - multiObjOp will handle.
	}

	// List and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q, %q cannot be used together with object name arguments",
			listFlag.Name, templateFlag.Name)
	}

	// operation on a given object or objects.
	return multiObjOp(c, commandEvict)
}

func resetPropsHandler(c *cli.Context) (err error) {
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return err
	}
	return resetBucketProps(c, bck)
}

func lruBucketHandler(c *cli.Context) (err error) {
	var p *cmn.BucketProps
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return err
	}
	if p, err = headBucket(bck, true /* don't add */); err != nil {
		return err
	}
	defProps, err := defaultBckProps(bck)
	if err != nil {
		return err
	}
	if flagIsSet(c, enableFlag) {
		return toggleLRU(c, bck, p, true)
	}
	if flagIsSet(c, disableFlag) {
		return toggleLRU(c, bck, p, false)
	}
	return HeadBckTable(c, p, defProps, "lru")
}

func toggleLRU(c *cli.Context, bck cmn.Bck, p *cmn.BucketProps, toggle bool) (err error) {
	const fmts = "Bucket %q: LRU is already %s, nothing to do\n"
	if toggle && p.LRU.Enabled {
		fmt.Fprintf(c.App.Writer, fmts, bck.DisplayName(), "enabled")
		return
	}
	if !toggle && !p.LRU.Enabled {
		fmt.Fprintf(c.App.Writer, fmts, bck.DisplayName(), "disabled")
		return
	}
	toggledProps, err := cmn.NewBucketPropsToUpdate(cos.SimpleKVs{"lru.enabled": strconv.FormatBool(toggle)})
	if err != nil {
		return
	}
	return updateBckProps(c, bck, p, toggledProps)
}

func setPropsHandler(c *cli.Context) (err error) {
	var currProps *cmn.BucketProps
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return err
	}
	if currProps, err = headBucket(bck, false /* don't add */); err != nil {
		return err
	}
	newProps, err := parseBckPropsFromContext(c)
	if err != nil {
		return err
	}
	newProps.Force = flagIsSet(c, forceFlag)

	return updateBckProps(c, bck, currProps, newProps)
}

func updateBckProps(c *cli.Context, bck cmn.Bck, currProps *cmn.BucketProps, updateProps *cmn.BucketPropsToUpdate) (err error) {
	// Apply updated props and check for change
	allNewProps := currProps.Clone()
	allNewProps.Apply(updateProps)
	if allNewProps.Equal(currProps) {
		displayPropsEqMsg(c, bck)
		return nil
	}

	if err = setBucketProps(c, bck, updateProps); err != nil {
		helpMsg := fmt.Sprintf("To show bucket properties, run \"%s %s %s BUCKET -v\"",
			cliName, commandShow, subcmdShowBucket)
		return newAdditionalInfoError(err, helpMsg)
	}

	showDiff(c, currProps, allNewProps)
	return nil
}

func displayPropsEqMsg(c *cli.Context, bck cmn.Bck) {
	args := c.Args().Tail()
	if len(args) == 1 && !isJSON(args[0]) {
		fmt.Fprintf(c.App.Writer, "Bucket %q: property %q, nothing to do\n", bck.DisplayName(), args[0])
		return
	}
	fmt.Fprintf(c.App.Writer, "Bucket %q already has the same values of props, nothing to do\n", bck.DisplayName())
}

func showDiff(c *cli.Context, currProps, newProps *cmn.BucketProps) {
	var (
		origKV = bckPropList(currProps, true)
		newKV  = bckPropList(newProps, true)
	)
	for idx, prop := range newKV {
		if origKV[idx].Value != prop.Value {
			fmt.Fprintf(c.App.Writer, "%q set to: %q (was: %q)\n", prop.Name, prop.Value, origKV[idx].Value)
		}
	}
}

func listAnyHandler(c *cli.Context) error {
	var (
		opts = cmn.ParseURIOpts{IsQuery: true}
		uri  = c.Args().First()
	)
	uri = preparseBckObjURI(uri)
	bck, objName, err := cmn.ParseBckObjectURI(uri, opts)
	if err != nil {
		return err
	}

	switch {
	case objName != "": // list archive OR show specific obj (HEAD(obj))
		if flagIsSet(c, listArchFlag) {
			return listArchHandler(c)
		}
		if _, err := headBucket(bck, true /* don't add */); err != nil {
			return err
		}
		return showObjProps(c, bck, objName)
	case bck.Name == "": // list buckets
		fltPresence := apc.FltPresent
		if flagIsSet(c, allObjsOrBcksFlag) {
			fltPresence = apc.FltExists
		}
		return listBuckets(c, cmn.QueryBcks(bck), fltPresence)
	default: // list objects
		prefix := parseStrFlag(c, prefixFlag)
		listArch := flagIsSet(c, listArchFlag) // include archived content, if requested
		return listObjects(c, bck, prefix, listArch)
	}
}
