// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/sys"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

// Promote AIS-colocated files and directories to objects.
func promote(c *cli.Context, bck cmn.Bck, objName, fqn string) error {
	var (
		target = parseStrFlag(c, targetIDFlag)
		recurs = flagIsSet(c, recursFlag)
	)
	args := apc.PromoteArgs{
		DaemonID:       target,
		ObjName:        objName,
		SrcFQN:         fqn,
		Recursive:      recurs,
		SrcIsNotFshare: flagIsSet(c, notFshareFlag),
		OverwriteDst:   flagIsSet(c, overwriteFlag),
		DeleteSrc:      flagIsSet(c, deleteSrcFlag),
	}
	xid, err := api.Promote(apiBP, bck, &args)
	if err != nil {
		return V(err)
	}
	var s1, s2 string
	if recurs {
		s1 = "recursively "
	}
	if xid != "" {
		s2 = fmt.Sprintf(", xaction ID %q", xid)
	}
	// alternatively, print(fmtXactStatusCheck, apc.ActPromote, ...)
	msg := fmt.Sprintf("%spromoted %q => %s%s\n", s1, fqn, bck.Cname(""), s2)
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
				return fmt.Errorf("invalid custom property %q (tip: use syntax key1=value1 key2=value2 ...)", nv)
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
	msg := fmt.Sprintf("Custom props successfully updated (to show updates, run 'ais show object %s --props=all').",
		bck.Cname(objName))
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

func verbList(c *cli.Context, wop wop, fnames []string, bck cmn.Bck, appendPref string, incl bool) error {
	var (
		ndir     int
		allFobjs = make([]fobj, 0, len(fnames))
		recurs   = flagIsSet(c, recursFlag)
	)
	for _, n := range fnames {
		fobjs, err := lsFobj(c, n, "", appendPref, &ndir, recurs, incl)
		if err != nil {
			return err
		}
		allFobjs = append(allFobjs, fobjs...)
	}
	return verbFobjs(c, wop, allFobjs, bck, ndir, recurs)
}

func verbRange(c *cli.Context, wop wop, pt *cos.ParsedTemplate, bck cmn.Bck, trimPref, appendPref string, incl bool) (err error) {
	var (
		ndir     int
		allFobjs = make([]fobj, 0, pt.Count())
		recurs   = flagIsSet(c, recursFlag)
	)
	pt.InitIter()
	for n, hasNext := pt.Next(); hasNext; n, hasNext = pt.Next() {
		fobjs, err := lsFobj(c, n, trimPref, appendPref, &ndir, recurs, incl)
		if err != nil {
			return err
		}
		allFobjs = append(allFobjs, fobjs...)
	}
	return verbFobjs(c, wop, allFobjs, bck, ndir, recurs)
}

func concatObject(c *cli.Context, bck cmn.Bck, objName string, fileNames []string) error {
	const verb = "Compose"
	var (
		totalSize int64
		ndir      int
		bar       *mpb.Bar
		progress  *mpb.Progress

		l          = len(fileNames)
		fobjMatrix = make([]fobjs, l)
		sizes      = make(map[string]int64, l) // or greater
		name       = bck.Cname(objName)
		recurs     = flagIsSet(c, recursFlag)
	)
	for i, fileName := range fileNames {
		fobjs, err := lsFobj(c, fileName, "", "", &ndir, recurs, false /*incl src dir*/)
		if err != nil {
			return err
		}
		sort.Sort(fobjs)
		for _, f := range fobjs {
			totalSize += f.size
			sizes[f.path] = f.size
		}
		fobjMatrix[i] = fobjs
	}
	// setup progress bar
	if flagIsSet(c, progressFlag) {
		switch l {
		case 1:
			fmt.Fprintf(c.App.Writer, "%s %q as %s\n", verb, fileNames[0], name)
		case 2, 3:
			fmt.Fprintf(c.App.Writer, "%s %v as %s\n", verb, fileNames, name)
		default:
			tag := fmt.Sprintf("%s %d pathnames", verb, l)
			tag += ndir2tag(ndir, recurs)
			fmt.Fprintf(c.App.Writer, "%s as %s\n", tag, name)
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
			handle, err = api.AppendObject(&appendArgs)
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
	err := api.FlushObject(&api.FlushArgs{
		BaseParams: apiBP,
		Bck:        bck,
		Object:     objName,
		Handle:     handle,
	})
	if err != nil {
		return V(err)
	}

	units, errU := parseUnitsFlag(c, unitsFlag)
	if errU != nil {
		actionWarn(c, errU.Error())
		units = ""
	}
	fmt.Fprintf(c.App.Writer, "\nCreated %s (size %s)\n", name, teb.FmtSize(totalSize, units, 2))
	return nil
}

func isObjPresent(c *cli.Context, bck cmn.Bck, objName string) error {
	name := bck.Cname(objName)
	_, err := api.HeadObject(apiBP, bck, objName, apc.FltPresentNoProps, true)
	if err != nil {
		if cmn.IsStatusNotFound(err) {
			fmt.Fprintf(c.App.Writer, "%s is not present (\"not cached\")\n", name)
			return nil
		}
		return V(err)
	}

	fmt.Fprintf(c.App.Writer, "%s is present (is cached)\n", name)
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

// via `ais ls bucket/object` and `ais show bucket/object`
func showObjProps(c *cli.Context, bck cmn.Bck, objName string) error {
	var (
		propsFlag     []string
		selectedProps []string
		fltPresence   = apc.FltPresentCluster
		units, errU   = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}
	if flagIsSet(c, objNotCachedPropsFlag) || flagIsSet(c, allObjsOrBcksFlag) {
		fltPresence = apc.FltExists
	}
	objProps, err := api.HeadObject(apiBP, bck, objName, fltPresence, flagIsSet(c, silentFlag))
	if err != nil {
		if !cmn.IsStatusNotFound(err) {
			return err
		}
		var hint string
		if apc.IsFltPresent(fltPresence) && bck.IsRemote() {
			if actionIsHandler(c.Command.Action, listAnyHandler) {
				hint = fmt.Sprintf(" (tip: try %s option)", qflprn(allObjsOrBcksFlag))
			} else {
				hint = fmt.Sprintf(" (tip: try %s option)", qflprn(objNotCachedPropsFlag))
			}
		}
		return fmt.Errorf("%q not found in %s%s", objName, bck.Cname(""), hint)
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
			if name == apc.GetPropsSize && units != "" {
				// reformat
				size, err := cos.ParseSize(v, "")
				if err != nil {
					warn := fmt.Sprintf("failed to parse 'size': %v", err)
					actionWarn(c, warn)
				} else {
					v = teb.FmtSize(size, units, 2)
				}
			}
			propNVs = append(propNVs, nvpair{name, v})
		}
	}
	sort.Slice(propNVs, func(i, j int) bool {
		return propNVs[i].Name < propNVs[j].Name
	})

	if flagIsSet(c, noHeaderFlag) {
		return teb.Print(propNVs, teb.PropValTmplNoHdr)
	}
	return teb.Print(propNVs, teb.PropValTmpl)
}

func propVal(op *cmn.ObjectProps, name string) (v string) {
	switch name {
	case apc.GetPropsName:
		v = op.Bck.Cname(op.Name)
	case apc.GetPropsSize:
		v = cos.ToSizeIEC(op.Size, 2)
	case apc.GetPropsChecksum:
		v = op.Cksum.String()
	case apc.GetPropsAtime:
		v = cos.FormatNanoTime(op.Atime, "")
	case apc.GetPropsVersion:
		v = op.Version()
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
	case apc.GetPropsStatus:
		// no "object status" in `cmn.ObjectProps` - nothing to do (see also: `cmn.LsoEnt`)
	default:
		debug.Assert(false, "obj prop name: \""+name+"\"")
	}
	return
}

func rmRfAllObjects(c *cli.Context, bck cmn.Bck) error {
	objList, err := api.ListObjects(apiBP, bck, nil, api.ListArgs{})
	if err != nil {
		return err
	}
	l := len(objList.Entries)
	if l == 0 {
		fmt.Fprintln(c.App.Writer, bck.Cname(""), "is empty, nothing to do.")
		return nil
	}

	var (
		errCh    = make(chan error, 1)
		cnt64    int64
		errCnt64 int64
		progress int64
		period   int64 = 1000
		wg             = cos.NewLimitedWaitGroup(sys.NumCPU(), l)
		vrbs           = flagIsSet(c, verboseFlag)
	)
	if bck.IsCloud() {
		period = 100
	}
	for _, entry := range objList.Entries {
		wg.Add(1)
		// delete one
		go func(objName string) {
			err := api.DeleteObject(apiBP, bck, objName)
			if err != nil {
				if ratomic.AddInt64(&errCnt64, 1) == 1 {
					errCh <- err
				}
			} else {
				n := ratomic.AddInt64(&cnt64, 1)
				if vrbs {
					fmt.Fprintf(c.App.Writer, "deleted %s\n", bck.Cname(objName))
				} else if n > 1 && n%period == 0 {
					fmt.Fprintf(c.App.Writer, "\r%s", cos.FormatBigNum(int(n)))
					ratomic.AddInt64(&progress, 1)
				}
			}
			wg.Done()
		}(entry.Name)
	}
	wg.Wait()
	close(errCh)

	if ratomic.LoadInt64(&progress) > 0 {
		fmt.Fprintln(c.App.Writer)
	}
	cnt := int(cnt64)
	if cnt == l {
		debug.Assert(errCnt64 == 0)
		msg := fmt.Sprintf("Deleted %s object%s from %s\n", cos.FormatBigNum(cnt), cos.Plural(cnt), bck.Cname(""))
		actionDone(c, msg)
		return nil
	}

	debug.Assert(errCnt64 > 0)
	firstErr := <-errCh
	warn := fmt.Sprintf("failed to delete %d object%s from %s: (%d deleted, %d error%s)\n", l-cnt, cos.Plural(l-cnt),
		bck, cnt, errCnt64, cos.Plural(int(errCnt64)))
	actionWarn(c, warn)
	return firstErr
}
