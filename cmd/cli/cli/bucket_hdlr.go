// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS buckets.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

const examplesBckSetProps = `
Usage examples:
- ais bucket props set BUCKET checksum.type=xxhash
- ais bucket props set BUCKET checksum.type=md5 checksum.validate_warm_get=true
- ais bucket props BUCKET checksum		# to show
- ais bucket props set BUCKET backend_bck=s3://abc
- ais bucket props set BUCKET backend_bck=none	# to reset
  (see docs/cli for details)
`

var listAnyUsage = "list buckets, objects in buckets, and files in " + archExts + "-formatted objects,\n" +
	indent1 + "e.g.:\n" +
	indent1 + "\t* ais ls \t- list all buckets in a cluster (all providers);\n" +
	indent1 + "\t* ais ls ais://abc -props name,size,copies,location \t- list all objects from a given bucket, include only the (4) specified properties;\n" +
	indent1 + "\t* ais ls ais://abc -props all \t- same as above but include all properties;\n" +
	indent1 + "\t* ais ls ais://abc --page-size 20 --refresh 3s \t- list a very large bucket (20 items in each page), report progress every 3s;\n" +
	indent1 + "\t* ais ls ais \t- list all ais buckets;\n" +
	indent1 + "\t* ais ls s3 \t- list all s3 buckets that are present in the cluster;\n" +
	indent1 + "\t* ais ls s3 --all \t- list all s3 buckets, both in-cluster and remote;\n" +
	indent1 + "with template, regex, and/or prefix:\n" +
	indent1 + "\t* ais ls gs: --regex \"^abc\" --all \t- list all accessible GCP buckets with names starting with \"abc\";\n" +
	indent1 + "\t* ais ls ais://abc --regex \".md\" --props size,checksum \t- list *.md objects with their respective sizes and checksums;\n" +
	indent1 + "\t* ais ls gs://abc --template images/\t- list all objects from the virtual subdirectory called \"images\";\n" +
	indent1 + "\t* ais ls gs://abc --prefix images/\t- same as above (for more examples, see '--template' below);\n" +
	indent1 + "with in-cluster vs remote content comparison (diff):\n" +
	indent1 + "\t* ais ls s3://abc --check-versions         \t- for each remote object in s3://abc: check whether it has identical in-cluster copy\n" +
	indent1 + "\t                                           \t  and show missing objects\n" +
	indent1 + "\t* ais ls s3://abc --check-versions --cached\t- for each in-cluster object in s3://abc: check whether it has identical remote copy\n" +
	indent1 + "\t                                           \t  and show deleted objects\n" +
	indent1 + "with summary (stats):\n" +
	indent1 + "\t* ais ls s3 --summary \t- for each s3 bucket in the cluster: print object numbers and total size(s);\n" +
	indent1 + "\t* ais ls s3 --summary --all \t- generate summary report for all s3 buckets; include remote objects and buckets that are _not present_\n" +
	indent1 + "\t* ais ls s3 --summary --all --dont-add\t- same as above but without adding _non-present_ remote buckets to cluster's BMD"

var copyBucketUsage = "copy entire bucket or selected objects (to select, use '--list', '--template', or '--prefix'), e.g.:\n" +
	indent1 + "\t- 'ais cp gs://webdaset-coco ais://dst'\t- copy entire Cloud bucket;\n" +
	indent1 + "\t- 'ais cp s3://abc ais://nnn --all'\t- copy entire Cloud bucket that may not be _present_ in the cluster;\n" +
	indent1 + "\t- 'ais cp s3://abc gs://xyz --all'\t- copy Cloud bucket to another Cloud;\n" +
	indent1 + "\t- 'ais cp s3://abc ais://nnn --latest'\t- copy Cloud bucket, and make sure that already present in-cluster copies are updated to the latest (remote) versions;\n" +
	indent1 + "\t- 'ais cp s3://abc ais://nnn --sync'\t- same as above, but in addition delete in-cluster copies that do not exist (any longer) in the remote source\n" +
	indent1 + "with template, prefix, and/or progress bar:\n" +
	indent1 + "\t- 'ais cp ais://nnn/111 ais://mmm'\t- copy a single object (assuming, prefix '111' corresponds to a single object);\n" +
	indent1 + "\t- 'ais cp gs://webdataset-coco ais:/dst --template d-tokens/shard-{000000..000999}.tar.lz4'\t- copy up to 1000 objects that share the specified prefix;\n" +
	indent1 + "\t- 'ais cp gs://webdataset-coco ais:/dst --prefix d-tokens/ --progress --all'\t- show progress while copying virtual subdirectory 'd-tokens'"

var (
	// flags
	bucketCmdsFlags = map[string][]cli.Flag{
		commandCreate: {
			ignoreErrorFlag,
			bucketPropsFlag,
			forceFlag,
			dontHeadRemoteFlag,
		},
		commandRemove: {
			ignoreErrorFlag,
			yesFlag,
		},
		commandCopy: {
			listFlag,
			templateFlag,
			verbObjPrefixFlag,
			copyAllObjsFlag,
			continueOnErrorFlag,
			forceFlag,
			copyDryRunFlag,
			copyPrependFlag,
			progressFlag,
			refreshFlag,
			waitFlag,
			waitJobXactFinishedFlag,
			latestVerFlag,
			syncFlag,
		},
		commandRename: {
			waitFlag,
			waitJobXactFinishedFlag,
		},
		commandEvict: append(
			listRangeProgressWaitFlags,
			keepMDFlag,
			verbObjPrefixFlag, // to disambiguate bucket/prefix vs bucket/objName
			dryRunFlag,
			verboseFlag, // not yet used
			nonverboseFlag,
		),
		cmdSetBprops: {
			forceFlag,
		},
		cmdResetBprops: {},

		commandList: {
			allObjsOrBcksFlag,
			listObjCachedFlag,
			nameOnlyFlag,
			objPropsFlag,
			regexLsAnyFlag,
			templateFlag,
			listObjPrefixFlag,
			pageSizeFlag,
			pagedFlag,
			objLimitFlag,
			refreshFlag,
			showUnmatchedFlag,
			noHeaderFlag,
			noFooterFlag,
			maxPagesFlag,
			startAfterFlag,
			bckSummaryFlag,
			dontHeadRemoteFlag,
			dontAddRemoteFlag,
			listArchFlag,
			unitsFlag,
			silentFlag,
			dontWaitFlag,
			verChangedFlag,
		},

		cmdLRU: {
			enableFlag,
			disableFlag,
		},
	}

	bckSummaryFlags = append(storageSummFlags, validateSummaryFlag)

	// commands
	bucketsObjectsCmdList = cli.Command{
		Name:         commandList,
		Usage:        listAnyUsage,
		ArgsUsage:    listAnyCommandArgument,
		Flags:        bucketCmdsFlags[commandList],
		Action:       listAnyHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}

	bucketCmdSummary = cli.Command{
		Name:         cmdSummary,
		Usage:        "generate and display bucket summary",
		ArgsUsage:    optionalBucketArgument,
		Flags:        bckSummaryFlags,
		Action:       summaryBucketHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}

	bucketCmdLRU = cli.Command{
		Name:         cmdLRU,
		Usage:        "show bucket's LRU configuration; enable or disable LRU eviction",
		ArgsUsage:    optionalBucketArgument,
		Flags:        bucketCmdsFlags[cmdLRU],
		Action:       lruBucketHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}
	bucketObjCmdEvict = cli.Command{
		Name: commandEvict,
		Usage: "evict one remote bucket, multiple remote buckets, or\n" +
			indent1 + "selected objects in a given remote bucket or buckets, e.g.:\n" +
			indent1 + "\t- 'evict gs://abc'\t- evict entire bucket (all gs://abc objects in aistore);\n" +
			indent1 + "\t- 'evict gs:'\t- evict all GCP buckets from the cluster;\n" +
			indent1 + "\t- 'evict gs://abc --template images/'\t- evict all objects from the virtual subdirectory \"images\";\n" +
			indent1 + "\t- 'evict gs://abc/images/'\t- same as above;\n" +
			indent1 + "\t- 'evict gs://abc --template \"shard-{0000..9999}.tar.lz4\"'\t- evict the matching range (prefix + brace expansion);\n" +
			indent1 + "\t- 'evict \"gs://abc/shard-{0000..9999}.tar.lz4\"'\t- same as above (notice double quotes)",
		ArgsUsage:    bucketObjectOrTemplateMultiArg,
		Flags:        bucketCmdsFlags[commandEvict],
		Action:       evictHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true}),
	}
	bucketCmdCopy = cli.Command{
		Name:         commandCopy,
		Usage:        copyBucketUsage,
		ArgsUsage:    bucketObjectSrcArgument + " " + bucketDstArgument,
		Flags:        bucketCmdsFlags[commandCopy],
		Action:       copyBucketHandler,
		BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{}, 0, 2),
	}
	bucketCmdRename = cli.Command{
		Name:         commandRename,
		Usage:        "rename/move ais bucket",
		ArgsUsage:    bucketArgument + " " + bucketNewArgument,
		Flags:        bucketCmdsFlags[commandRename],
		Action:       mvBucketHandler,
		BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{}, 0, 2),
	}
	bucketCmdSetProps = cli.Command{
		Name: cmdSetBprops,
		Usage: "update bucket properties; the command accepts both JSON-formatted input and plain Name=Value pairs, e.g.:\n" +
			indent1 + "\t* ais bucket props set ais://nnn backend_bck=s3://mmm\n" +
			indent1 + "\t* ais bucket props set ais://nnn backend_bck=none\n" +
			indent1 + "\t* ais bucket props set gs://vvv versioning.validate_warm_get=false versioning.synchronize=true\n" +
			indent1 + "\t* ais bucket props set gs://vvv mirror.enabled=true mirror.copies=4 checksum.type=md5\n" +
			indent1 + "\t* ais bucket props set s3://mmm ec.enabled true ec.data_slices 6 ec.parity_slices 4 --force\n" +
			indent1 + "\tReferences:\n" +
			indent1 + "\t* for details and many more examples, see docs/cli/bucket.md\n" +
			indent1 + "\t* to show bucket properties (names and current values), use 'ais bucket show'",
		ArgsUsage: bucketPropsArgument,
		Flags:     bucketCmdsFlags[cmdSetBprops],
		Action:    setPropsHandler,
		BashComplete: bucketCompletions(
			bcmplop{additionalCompletions: []cli.BashCompleteFunc{bpropCompletions}},
		),
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
			bucketCmdCopy,
			bucketCmdRename,
			{
				Name:      commandRemove,
				Usage:     "remove ais buckets",
				ArgsUsage: bucketsArgument,
				Flags:     bucketCmdsFlags[commandRemove],
				Action:    removeBucketHandler,
				BashComplete: bucketCompletions(bcmplop{
					multiple: true, provider: apc.AIS,
				}),
			},
			{
				Name:   cmdProps,
				Usage:  "show, update or reset bucket properties",
				Action: showBckPropsHandler,
				Subcommands: []cli.Command{
					bucketCmdSetProps,
					{
						Name:      cmdResetBprops,
						Usage:     "reset bucket properties",
						ArgsUsage: bucketPropsArgument,
						Flags:     bucketCmdsFlags[cmdResetBprops],
						Action:    resetPropsHandler,
						BashComplete: bucketCompletions(
							bcmplop{additionalCompletions: []cli.BashCompleteFunc{bpropCompletions}},
						),
					},
					makeAlias(showCmdBucket, "", true, commandShow),
				},
			},
		},
	}
)

func createBucketHandler(c *cli.Context) (err error) {
	var props *cmn.BpropsToSet
	if flagIsSet(c, bucketPropsFlag) {
		propSingleBck, err := parseBpropsFromContext(c)
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
	dontHeadRemote := flagIsSet(c, dontHeadRemoteFlag)
	for _, bck := range buckets {
		if err := createBucket(c, bck, props, dontHeadRemote); err != nil {
			return err
		}
	}
	return nil
}

func checkObjectHealth(queryBcks cmn.QueryBcks) error {
	type bucketHealth struct {
		Bck           cmn.Bck
		ObjectCnt     uint64
		Misplaced     uint64
		MissingCopies uint64
	}
	bcks, err := api.ListBuckets(apiBP, queryBcks, apc.FltPresent)
	if err != nil {
		return V(err)
	}
	bckSums := make([]*bucketHealth, 0)
	msg := &apc.LsoMsg{Flags: apc.LsMissing}
	msg.AddProps(apc.GetPropsCopies, apc.GetPropsCached)

	for i := range bcks {
		bck := bcks[i]
		if queryBcks.Name != "" && !queryBcks.Equal(&bck) {
			continue
		}
		var (
			objList *cmn.LsoResult
			obj     *cmn.LsoEntry
		)
		p, err := headBucket(bck, true /* don't add */)
		if err != nil {
			return err
		}
		copies := int16(p.Mirror.Copies)
		stats := &bucketHealth{Bck: bck}
		objList, err = api.ListObjects(apiBP, bck, msg, api.ListArgs{})
		if err != nil {
			return err
		}

		updateStats := func(obj *cmn.LsoEntry) {
			if obj == nil {
				return
			}
			stats.ObjectCnt++
			if !obj.IsStatusOK() {
				stats.Misplaced++
			} else if obj.IsPresent() && p.Mirror.Enabled && obj.Copies < copies {
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
	return teb.Print(bckSums, teb.BucketSummaryValidateTmpl)
}

func summaryBucketHandler(c *cli.Context) error {
	if flagIsSet(c, validateSummaryFlag) {
		return showMisplacedAndMore(c)
	}
	return summaryStorageHandler(c)
}

func showMisplacedAndMore(c *cli.Context) error {
	queryBcks, err := parseQueryBckURI(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	f := func() error {
		return checkObjectHealth(queryBcks)
	}
	return waitForFunc(f, longClientTimeout)
}

func mvBucketHandler(c *cli.Context) error {
	bckFrom, bckTo, _, err := parseBcks(c, bucketArgument, bucketNewArgument, 0 /*shift*/, false /*optionalSrcObjname*/)
	if err != nil {
		return err
	}
	if bckFrom.Equal(&bckTo) {
		return incorrectUsageMsg(c, errFmtSameBucket, commandRename, bckTo)
	}
	return mvBucket(c, bckFrom, bckTo)
}

func removeBucketHandler(c *cli.Context) error {
	buckets, err := bucketsFromArgsOrEnv(c)
	if err != nil {
		return err
	}
	return destroyBuckets(c, buckets)
}

func resetPropsHandler(c *cli.Context) error {
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	if _, err := api.ResetBucketProps(apiBP, bck); err != nil {
		return V(err)
	}
	actionDone(c, "Bucket props successfully reset to cluster defaults")
	return nil
}

func lruBucketHandler(c *cli.Context) error {
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	var p *cmn.Bprops
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
	return headBckTable(c, p, defProps, "lru")
}

func toggleLRU(c *cli.Context, bck cmn.Bck, p *cmn.Bprops, toggle bool) (err error) {
	const fmts = "Bucket %q: LRU is already %s, nothing to do\n"
	if toggle && p.LRU.Enabled {
		fmt.Fprintf(c.App.Writer, fmts, bck.Cname(""), "enabled")
		return
	}
	if !toggle && !p.LRU.Enabled {
		fmt.Fprintf(c.App.Writer, fmts, bck.Cname(""), "disabled")
		return
	}
	toggledProps, err := cmn.NewBpropsToSet(cos.StrKVs{"lru.enabled": strconv.FormatBool(toggle)})
	if err != nil {
		return
	}
	return updateBckProps(c, bck, p, toggledProps)
}

func setPropsHandler(c *cli.Context) (err error) {
	var currProps *cmn.Bprops
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	if currProps, err = headBucket(bck, false /* don't add */); err != nil {
		return err
	}
	newProps, err := parseBpropsFromContext(c)
	if err != nil {
		if strings.Contains(err.Error(), "missing property") {
			if errV := showBucketProps(c); errV == nil {
				msg := err.Error() + examplesBckSetProps
				fmt.Fprintln(c.App.ErrWriter)
				actionWarn(c, msg)
				return nil
			}
		}
		return fmt.Errorf("%v%s", err, examplesBckSetProps)
	}
	newProps.Force = flagIsSet(c, forceFlag)

	return updateBckProps(c, bck, currProps, newProps)
}

// TODO: more validation; e.g. `validate_warm_get = true` is only supported for buckets with Cloud and remais backends
func updateBckProps(c *cli.Context, bck cmn.Bck, currProps *cmn.Bprops, updateProps *cmn.BpropsToSet) (err error) {
	// apply updated props
	allNewProps := currProps.Clone()
	allNewProps.Apply(updateProps)

	// check for changes
	if allNewProps.Equal(currProps) {
		displayPropsEqMsg(c, bck)
		return nil
	}

	// do
	if _, err = api.SetBucketProps(apiBP, bck, updateProps); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
			return herr
		}
		helpMsg := fmt.Sprintf("To show bucket properties, run '%s %s %s %s'",
			cliName, commandShow, cmdBucket, bck.Cname(""))
		return newAdditionalInfoError(err, helpMsg)
	}
	showDiff(c, currProps, allNewProps)
	actionDone(c, "\nBucket props successfully updated.")
	return nil
}

func displayPropsEqMsg(c *cli.Context, bck cmn.Bck) {
	args := c.Args().Tail()
	if len(args) == 1 && !isJSON(args[0]) {
		fmt.Fprintf(c.App.Writer, "Bucket %q: property %q, nothing to do\n", bck.Cname(""), args[0])
		return
	}
	fmt.Fprintf(c.App.Writer, "Bucket %q already has the same values of props, nothing to do\n", bck.Cname(""))
}

func showDiff(c *cli.Context, currProps, newProps *cmn.Bprops) {
	var (
		origKV = bckPropList(currProps, true)
		newKV  = bckPropList(newProps, true)
	)
	for _, np := range newKV {
		var found bool
		for _, op := range origKV {
			if np.Name != op.Name {
				continue
			}
			found = true
			if np.Value != op.Value {
				fmt.Fprintf(c.App.Writer, "%q set to: %q (was: %q)\n", np.Name, np.Value, op.Value)
			}
		}
		if !found && np.Value != "" {
			fmt.Fprintf(c.App.Writer, "%q set to: %q (was: n/a)\n", np.Name, np.Value)
		}
	}
}

type lsbCtx struct {
	regexStr        string
	regex           *regexp.Regexp
	fltPresence     int
	countRemoteObjs bool
	all             bool
}

func listAnyHandler(c *cli.Context) error {
	var (
		opts = cmn.ParseURIOpts{IsQuery: true}
		uri  = c.Args().Get(0)
	)
	uri = preparseBckObjURI(uri)
	bck, objName, err := cmn.ParseBckObjectURI(uri, opts) // `ais ls` with no args - is Ok

	if err != nil {
		if errV := errBucketNameInvalid(c, uri, err); errV != nil {
			return errV
		}
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
		err := showObjProps(c, bck, objName)
		if err == nil {
			if _, errV := archive.Mime("", objName); errV == nil {
				fmt.Fprintf(c.App.Writer, "\n('ais ls %s %s' to list archived contents, %s for details)\n",
					bck.Cname(objName), flprn(listArchFlag), qflprn(cli.HelpFlag))
			}
		}
		return err
	case bck.Name == "" || flagIsSet(c, bckSummaryFlag): // list or summarize bucket(s)
		var lsb lsbCtx
		if lsb.regexStr = parseStrFlag(c, regexLsAnyFlag); lsb.regexStr != "" {
			regex, err := regexp.Compile(lsb.regexStr)
			if err != nil {
				return err
			}
			lsb.regex = regex
		}
		lsb.all = flagIsSet(c, allObjsOrBcksFlag)
		lsb.fltPresence = apc.FltPresent
		if lsb.all {
			lsb.fltPresence = apc.FltExists
		}
		if flagIsSet(c, bckSummaryFlag) {
			if lsb.all && (bck.Provider != apc.AIS || !bck.Ns.IsGlobal()) {
				lsb.countRemoteObjs = true
				const (
					warn = "counting and sizing remote objects may take considerable time\n"
					tip1 = "(tip: run 'ais storage summary' or use '--regex' to refine the selection)\n"
					tip2 = "(tip: use '--refresh DURATION' to show progress, '--help' for details)\n"
				)
				switch {
				case !flagIsSet(c, refreshFlag):
					actionWarn(c, warn+tip2)
				case lsb.regex == nil:
					actionWarn(c, warn+tip1)
				default:
					actionWarn(c, warn)
				}
			}
			if bck.Name != "" {
				_ = listBckTable(c, cmn.QueryBcks(bck), cmn.Bcks{bck}, lsb)
				return nil
			}
		}

		return listOrSummBuckets(c, cmn.QueryBcks(bck), lsb)
	default: // list objects
		prefix := parseStrFlag(c, listObjPrefixFlag)
		listArch := flagIsSet(c, listArchFlag) // include archived content, if requested
		return listObjects(c, bck, prefix, listArch)
	}
}
