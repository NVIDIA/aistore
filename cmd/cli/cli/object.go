// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

const (
	dryRunExamplesCnt = 10
	dryRunHeader      = "[DRY RUN]"
	dryRunExplanation = "No modifications on the cluster"
)

// Promote AIS-colocated files and directories to objects.
func promote(c *cli.Context, bck cmn.Bck, objName, fqn string) error {
	var (
		target = parseStrFlag(c, targetIDFlag)
		recurs = flagIsSet(c, recursFlag)
	)
	promoteArgs := &api.PromoteArgs{
		BaseParams: apiBP,
		Bck:        bck,
		PromoteArgs: cluster.PromoteArgs{
			DaemonID:       target,
			ObjName:        objName,
			SrcFQN:         fqn,
			Recursive:      recurs,
			SrcIsNotFshare: flagIsSet(c, notFshareFlag),
			OverwriteDst:   flagIsSet(c, overwriteFlag),
			DeleteSrc:      flagIsSet(c, deleteSrcFlag),
		},
	}
	xid, err := api.Promote(promoteArgs)
	if err != nil {
		return err
	}
	var s1, s2 string
	if recurs {
		s1 = "recursively "
	}
	if xid != "" {
		s2 = fmt.Sprintf(", xaction ID %q", xid)
	}
	// alternatively, print(fmtXactStatusCheck, apc.ActPromote, ...)
	msg := fmt.Sprintf("%spromoted %q => %s%s\n", s1, fqn, bck.DisplayName(), s2)
	actionDone(c, msg)
	return nil
}

func setCustomProps(c *cli.Context, bck cmn.Bck, objName string) (err error) {
	props := make(cos.StrKVs)
	propArgs := c.Args().Tail()

	if len(propArgs) == 1 && isJSON(propArgs[0]) {
		if err = jsoniter.Unmarshal([]byte(propArgs[0]), &props); err != nil {
			return
		}
	} else {
		if len(propArgs) == 0 {
			err = missingArgumentsError(c, "property key-value pairs")
			return
		}
		for _, pair := range propArgs {
			nv := strings.Split(pair, "=")
			if len(nv) != 2 {
				return fmt.Errorf("invalid custom property %q (Hint: use syntax key1=value1 key2=value2 ...)", nv)
			}
			nv[0] = strings.TrimSpace(nv[0])
			nv[1] = strings.TrimSpace(nv[1])
			props[nv[0]] = nv[1]
		}
	}
	setNewCustom := flagIsSet(c, setNewCustomMDFlag)
	if err = api.SetObjectCustomProps(apiBP, bck, objName, props, setNewCustom); err != nil {
		return
	}
	msg := fmt.Sprintf("Custom props successfully updated (to show updates, run 'ais show object %s/%s --props=all').",
		bck, objName)
	actionDone(c, msg)
	return nil
}

// replace common abbreviations (such as `~/`) and return an absolute path
func absPath(fileName string) (path string, err error) {
	path = cos.ExpandPath(fileName)
	if path, err = filepath.Abs(path); err != nil {
		return "", err
	}
	return
}

// Returns longest common prefix ending with '/' (exclusive) for objects in the template
// /path/to/dir/test{0..10}/dir/another{0..10} => /path/to/dir
// /path/to/prefix-@00001-gap-@100-suffix => /path/to
func rangeTrimPrefix(pt cos.ParsedTemplate) string {
	sepaIndex := strings.LastIndex(pt.Prefix, string(os.PathSeparator))
	debug.Assert(sepaIndex >= 0)
	return pt.Prefix[:sepaIndex+1]
}

func putDryRun(c *cli.Context, bck cmn.Bck, objName, fileName string) error {
	actionCptn(c, dryRunHeader, " "+dryRunExplanation)
	path, err := absPath(fileName)
	if err != nil {
		return err
	}
	if objName == "" {
		objName = filepath.Base(path)
	}
	archPath := parseStrFlag(c, archpathOptionalFlag)
	if archPath == "" {
		actionDone(c, fmt.Sprintf("PUT %q => %s/%s\n", fileName, bck.DisplayName(), objName))
	} else {
		actionDone(c, fmt.Sprintf("APPEND %q to %s/%s as %s\n", fileName, bck.DisplayName(), objName, archPath))
	}
	return nil
}

func putAny(c *cli.Context, bck cmn.Bck, objName, fileName string) error {
	bname := bck.DisplayName()

	// 1. STDIN
	if fileName == "-" {
		if objName == "" {
			return fmt.Errorf("when writing directly from standard input destination object name (in %s) is required",
				c.Command.ArgsUsage)
		}
		chunkSize, err := parseSizeFlag(c, chunkSizeFlag)
		if err != nil {
			return err
		}
		if flagIsSet(c, chunkSizeFlag) && chunkSize == 0 {
			return fmt.Errorf("chunk size (in %s) cannot be zero (%s recommended)",
				qflprn(chunkSizeFlag), teb.FmtSize(defaultChunkSize, cos.UnitsIEC, 0))
		}
		if chunkSize == 0 {
			chunkSize = defaultChunkSize
		}
		if flagIsSet(c, verboseFlag) {
			actionWarn(c, "To terminate input, press Ctrl-D two or more times")
		}
		cksum, err := cksumToCompute(c, bck)
		if err != nil {
			return err
		}
		cksumType := cksum.Type() // can be none
		if err := putAppendChunks(c, bck, objName, os.Stdin, cksumType, chunkSize); err != nil {
			return err
		}
		actionDone(c, fmt.Sprintf("PUT (stdin) => %s/%s\n", bname, objName))
		return nil
	}

	path, err := absPath(fileName)
	if err != nil {
		return err
	}

	// 2. inline "range" w/ no flag, e.g.: "/tmp/www/test{0..2}{0..2}.txt" ais://nnn/www
	if pt, err := cos.ParseBashTemplate(path); err == nil {
		return putRange(c, pt, bck, rangeTrimPrefix(pt), objName /* subdir name */)
	}

	// 3. inline "list" w/ no flag: "FILE[,FILE...]" BUCKET/[OBJECT_NAME]
	if _, err := os.Stat(fileName); err != nil {
		fnames := splitCsv(fileName)
		return putList(c, fnames, bck, objName /* subdir name */)
	}

	// 4. put single file _or_ append to arch
	if finfo, err := os.Stat(path); err == nil && !finfo.IsDir() {
		if objName == "" {
			// [CONVENTION]: if objName is not provided
			// we use the filename as the destination object name
			objName = filepath.Base(path)
		}

		archPath := parseStrFlag(c, archpathOptionalFlag)
		// APPEND to an existing archive
		if archPath != "" {
			if err := appendToArch(c, bck, objName, path, archPath, finfo); err != nil {
				return err
			}
			actionDone(c, fmt.Sprintf("APPEND %q to %s/%s as %s\n", fileName, bname, objName, archPath))
			return nil
		}

		// single-file PUT
		if err := putRegular(c, bck, objName, path, finfo); err != nil {
			return err
		}
		actionDone(c, fmt.Sprintf("PUT %q => %s/%s\n", fileName, bname, objName))
		return nil
	}

	// 5. directory
	recurs := flagIsSet(c, recursFlag)
	files, err := lsFobj(c, path, "", objName, recurs)
	if err != nil {
		return err
	}
	tag := _putFrom(fmt.Sprintf(" from %q", fileName), recurs)
	return putFobjs(c, files, bck, tag)
}

func _putFrom(s string, recurs bool) (tag string) {
	if recurs {
		tag = s + "(recursive)"
	} else {
		tag = s + "(non-recursive)"
	}
	return
}

func putList(c *cli.Context, fnames []string, bck cmn.Bck, appendPrefixSubdir string) error {
	var (
		allFiles = make([]fobj, 0, len(fnames))
		recurs   = flagIsSet(c, recursFlag)
	)
	for _, n := range fnames {
		files, err := lsFobj(c, n, "", appendPrefixSubdir, recurs)
		if err != nil {
			return err
		}
		allFiles = append(allFiles, files...)
	}
	tag := _putFrom(" ", recurs)
	return putFobjs(c, allFiles, bck, tag)
}

func putRange(c *cli.Context, pt cos.ParsedTemplate, bck cmn.Bck, trimPrefix, appendPrefixSubdir string) (err error) {
	var (
		allFiles = make([]fobj, 0, pt.Count())
		recurs   = flagIsSet(c, recursFlag)
	)
	pt.InitIter()
	for n, hasNext := pt.Next(); hasNext; n, hasNext = pt.Next() {
		files, err := lsFobj(c, n, trimPrefix, appendPrefixSubdir, recurs)
		if err != nil {
			return err
		}
		allFiles = append(allFiles, files...)
	}
	tag := _putFrom(" ", recurs)
	return putFobjs(c, allFiles, bck, tag)
}

func concatObject(c *cli.Context, bck cmn.Bck, objName string, fileNames []string) error {
	const verb = "Compose"
	var (
		totalSize  int64
		bar        *mpb.Bar
		progress   *mpb.Progress
		bname      = bck.DisplayName()
		l          = len(fileNames)
		fobjMatrix = make([]fobjSlice, l)
		sizes      = make(map[string]int64, l) // or greater
	)
	for i, fileName := range fileNames {
		fsl, err := lsFobj(c, fileName, "", "", flagIsSet(c, recursFlag))
		if err != nil {
			return err
		}
		sort.Sort(fsl)
		for _, f := range fsl {
			totalSize += f.size
			sizes[f.path] = f.size
		}
		fobjMatrix[i] = fsl
	}
	// setup progress bar
	if flagIsSet(c, progressFlag) {
		switch l {
		case 1:
			fmt.Fprintf(c.App.Writer, "%s %q as %s/%s\n", verb, fileNames[0], bname, objName)
		case 2, 3:
			fmt.Fprintf(c.App.Writer, "%s %v as %s/%s\n", verb, fileNames, bname, objName)
		default:
			fmt.Fprintf(c.App.Writer, "%s %d pathnames as %s/%s\n", verb, l, bname, objName)
		}
		var (
			bars []*mpb.Bar
			args = barArgs{barType: sizeArg, barText: "Progress:", total: totalSize}
		)
		progress, bars = simpleBar(args)
		bar = bars[0]
	}
	// do
	var handle string
	for _, fsl := range fobjMatrix {
		for _, f := range fsl {
			fh, err := cos.NewFileHandle(f.path)
			if err != nil {
				return err
			}
			appendArgs := api.AppendArgs{
				BaseParams: apiBP,
				Bck:        bck,
				Object:     objName,
				Reader:     fh,
				Handle:     handle,
			}
			handle, err = api.AppendObject(appendArgs)
			if err != nil {
				return fmt.Errorf("%v. Object not created", err)
			}
			if bar != nil {
				bar.IncrInt64(sizes[f.path])
			}
		}
	}

	if progress != nil {
		progress.Wait()
	}
	err := api.FlushObject(api.FlushArgs{
		BaseParams: apiBP,
		Bck:        bck,
		Object:     objName,
		Handle:     handle,
	})
	if err != nil {
		return fmt.Errorf("%v. Object not created", err)
	}

	units, errU := parseUnitsFlag(c, unitsFlag)
	if errU != nil {
		actionWarn(c, errU.Error())
		units = ""
	}
	fmt.Fprintf(c.App.Writer, "\nCreated %s/%s (size %s)\n", bname, objName, teb.FmtSize(totalSize, units, 2))
	return nil
}

func isObjPresent(c *cli.Context, bck cmn.Bck, object string) error {
	_, err := api.HeadObject(apiBP, bck, object, apc.FltPresentNoProps)
	if err != nil {
		if cmn.IsStatusNotFound(err) {
			fmt.Fprintf(c.App.Writer, "Cached: %v\n", false)
			return nil
		}
		return err
	}

	fmt.Fprintf(c.App.Writer, "Cached: %v\n", true)
	return nil
}

func calcPutRefresh(c *cli.Context) time.Duration {
	refresh := refreshRateDefault
	if flagIsSet(c, verboseFlag) && !flagIsSet(c, refreshFlag) {
		return 0
	}
	if flagIsSet(c, refreshFlag) {
		refresh = _refreshRate(c)
	}
	return refresh
}

// Displays object properties
func showObjProps(c *cli.Context, bck cmn.Bck, object string) error {
	var (
		propsFlag     []string
		selectedProps []string
		fltPresence   = apc.FltPresentCluster
	)
	if flagIsSet(c, objNotCachedPropsFlag) {
		fltPresence = apc.FltExists
	}
	objProps, err := api.HeadObject(apiBP, bck, object, fltPresence)
	if err != nil {
		if !cmn.IsStatusNotFound(err) {
			return err
		}
		var hint string
		if apc.IsFltPresent(fltPresence) {
			hint = fmt.Sprintf(" (hint: try %s option)", qflprn(objNotCachedPropsFlag))
		}
		return fmt.Errorf("%q not found in %s%s", object, bck.DisplayName(), hint)
	}
	if flagIsSet(c, jsonFlag) {
		opts := teb.Jopts(true)
		return teb.Print(objProps, teb.PropsSimpleTmpl, opts)
	}
	if flagIsSet(c, allPropsFlag) {
		propsFlag = apc.GetPropsAll
	} else if flagIsSet(c, objPropsFlag) {
		s := parseStrFlag(c, objPropsFlag)
		propsFlag = splitCsv(s)
	}

	// NOTE: three different defaults; compare w/ `listObjects()`
	if len(propsFlag) == 0 {
		selectedProps = apc.GetPropsMinimal
		if bck.IsAIS() {
			selectedProps = apc.GetPropsDefaultAIS
		} else if bck.IsCloud() {
			selectedProps = apc.GetPropsDefaultCloud
		}
	} else if cos.StringInSlice("all", propsFlag) {
		selectedProps = apc.GetPropsAll
	} else {
		selectedProps = propsFlag
	}

	propNVs := make(nvpairList, 0, len(selectedProps))
	for _, name := range selectedProps {
		if v := propVal(objProps, name); v != "" {
			if name == apc.GetPropsAtime && isUnsetTime(c, v) {
				v = teb.NotSetVal
			}
			propNVs = append(propNVs, nvpair{name, v})
		}
	}
	sort.Slice(propNVs, func(i, j int) bool {
		return propNVs[i].Name < propNVs[j].Name
	})

	return teb.Print(propNVs, teb.PropsSimpleTmpl)
}

func propVal(op *cmn.ObjectProps, name string) (v string) {
	switch name {
	case apc.GetPropsName:
		v = op.Bck.DisplayName() + "/" + op.Name
	case apc.GetPropsSize:
		v = cos.ToSizeIEC(op.Size, 2)
	case apc.GetPropsChecksum:
		v = op.Cksum.String()
	case apc.GetPropsAtime:
		v = cos.FormatNanoTime(op.Atime, "")
	case apc.GetPropsVersion:
		v = op.Ver
	case apc.GetPropsCached:
		if op.Bck.IsAIS() {
			debug.Assert(op.Present)
			return
		}
		v = teb.FmtBool(op.Present)
	case apc.GetPropsCopies:
		v = teb.FmtCopies(op.Mirror.Copies)
		if len(op.Mirror.Paths) != 0 {
			v += fmt.Sprintf(" %v", op.Mirror.Paths)
		}
	case apc.GetPropsEC:
		v = teb.FmtEC(op.EC.Generation, op.EC.DataSlices, op.EC.ParitySlices, op.EC.IsECCopy)
	case apc.GetPropsCustom:
		if custom := op.GetCustomMD(); len(custom) == 0 {
			v = teb.NotSetVal
		} else {
			v = cmn.CustomMD2S(custom)
		}
	case apc.GetPropsLocation:
		v = op.Location
	default:
		debug.Assert(false, name)
	}
	return
}

func rmRfAllObjects(c *cli.Context, bck cmn.Bck) error {
	var (
		l, cnt       int
		objList, err = api.ListObjects(apiBP, bck, nil, 0)
	)
	if err != nil {
		return err
	}
	if l = len(objList.Entries); l == 0 {
		fmt.Fprintln(c.App.Writer, "The bucket is empty, nothing to do.")
		return nil
	}
	for _, entry := range objList.Entries {
		if err := api.DeleteObject(apiBP, bck, entry.Name); err == nil {
			cnt++
			if flagIsSet(c, verboseFlag) {
				fmt.Fprintf(c.App.Writer, "deleted %q\n", entry.Name)
			}
		}
	}
	if cnt == l {
		if flagIsSet(c, verboseFlag) {
			fmt.Fprintln(c.App.Writer, "=====")
			fmt.Fprintf(c.App.Writer, "Deleted %d object%s from %s\n", cnt, cos.Plural(cnt), bck.DisplayName())
		} else {
			fmt.Fprintf(c.App.Writer, "Deleted %d object%s from %s\n", cnt, cos.Plural(cnt), bck.DisplayName())
		}
	} else {
		fmt.Fprintf(c.App.Writer, "Failed to delete %d object%s from %s: (%d total, %d deleted)\n",
			l-cnt, cos.Plural(l-cnt), bck, l, cnt)
	}
	return nil
}
