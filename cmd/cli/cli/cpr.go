// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

type cprCtx struct {
	errCh   chan error
	barObjs *mpb.Bar
	barSize *mpb.Bar
	xid     string
	from    string // from-bucket name or _the_ bucket name
	to      string // to-bucket name (optional)
	xname   string
	loghdr  string
	totals  struct {
		objs int64
		size int64
	}
	timeout, sleep time.Duration
	// runtime
	objs     int64
	size     int64
	sinceUpd time.Duration
}

func (cpr *cprCtx) copyBucket(c *cli.Context, bckFrom, bckTo cmn.Bck, msg *apc.CopyBckMsg, fltPresence int) error {
	actionNote(c, "to initialize progress bar, running 'bucket summary' on the source: "+bckFrom.Cname(""))

	// 1. get from-bck summary
	var (
		qbck       = cmn.QueryBcks(bckFrom)
		objCached  = !flagIsSet(c, copyAllObjsFlag)
		bckPresent = apc.IsFltPresent(fltPresence)
	)
	debug.Assert(parseStrFlag(c, verbObjPrefixFlag) == msg.Prefix)
	ctx, err := newBsummCtxMsg(c, qbck, msg.Prefix, objCached, bckPresent)
	if err != nil {
		return err
	}
	if err = ctx.get(); err != nil {
		return err
	}

	// 2. got bucket summary(ies)
	summaries := ctx.res
	for _, res := range summaries {
		debug.Assertf(res.Bck.Equal(&bckFrom), "%s != %s", res.Bck.String(), bckFrom.String())
		cpr.totals.size += int64(res.TotalSize.PresentObjs + res.TotalSize.RemoteObjs)
		cpr.totals.objs += int64(res.ObjCount.Present + res.ObjCount.Remote)
	}
	if cpr.totals.objs == 0 {
		debug.Assert(cpr.totals.size == 0)
		if !apc.IsFltPresent(fltPresence) {
			err = fmt.Errorf("source %s is empty, nothing to do", cpr.from)
		} else {
			err = fmt.Errorf("source %s doesn't have any cached objects in the cluster, nothing to do"+
				" (see %s for details)", cpr.from, qflprn(cli.HelpFlag))
		}
		return err
	}

	// 3. setup progress bar
	var (
		progress *mpb.Progress
		bars     []*mpb.Bar
		objsArg  = barArgs{barType: unitsArg, barText: "Copied objects:", total: cpr.totals.objs}
		sizeArg  = barArgs{barType: sizeArg, barText: "Copied size:   ", total: cpr.totals.size}
	)
	progress, bars = simpleBar(objsArg, sizeArg)
	cpr.barObjs, cpr.barSize = bars[0], bars[1]

	cpr.xid, err = api.CopyBucket(apiBP, bckFrom, bckTo, msg, fltPresence)
	if err != nil {
		return err
	}
	// NOTE: may've transitioned TCB => TCO
	if !apc.IsFltPresent(fltPresence) {
		_, cpr.xname, err = getKindNameForID(cpr.xid, cpr.xname)
		if err != nil {
			return err
		}
	}
	if cpr.to != "" {
		cpr.loghdr = fmt.Sprintf("%s[%s] %s => %s", cpr.xname, cpr.xid, cpr.from, cpr.to)
	} else {
		cpr.loghdr = fmt.Sprintf("%s[%s] %s", cpr.xname, cpr.xid, cpr.from)
	}

	// 4. poll x-copy-bucket asynchronously and update the progress
	cpr.do(c)
	progress.Wait()

	// 5. done
	err = <-cpr.errCh
	if err == nil {
		actionDone(c, fmtXactSucceeded)
	}
	close(cpr.errCh)
	return err
}

func (cpr *cprCtx) multiobj(c *cli.Context, text string) (err error) {
	var (
		progress *mpb.Progress
		bars     []*mpb.Bar
		objsArg  = barArgs{barType: unitsArg, barText: text, total: cpr.totals.objs}
	)
	progress, bars = simpleBar(objsArg)
	cpr.barObjs = bars[0]

	cpr.do(c)
	progress.Wait()

	// 4. done
	err = <-cpr.errCh
	if err == nil {
		actionDone(c, fmtXactSucceeded)
	}
	close(cpr.errCh)
	return
}

func (cpr *cprCtx) do(c *cli.Context) {
	cpr.errCh = make(chan error, 1)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		cpr.timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	cpr.sleep = _refreshRate(c) // refreshFlag or default

	var (
		rerr      error
		totalWait time.Duration
		xargs     = xact.ArgsMsg{ID: cpr.xid}
	)

	// main loop
	for {
		var (
			size, objs int64
			nrun       int
		)
		xs, cms, err := queryXactions(&xargs, true /*summarize*/)
		if err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
				time.Sleep(refreshRateMinDur)
				continue
			}
			rerr = fmt.Errorf("%s failed: %v", cpr.loghdr, err)
			break
		}
		debug.Assert(cpr.xid == cms.xid, cpr.xid, " vs ", cms.xid)
		if cms.running {
			for _, snaps := range xs {
				debug.Assert(len(snaps) < 2)
				for _, xsnap := range snaps {
					debug.Assertf(cpr.xid == xsnap.ID, "%q vs %q", cpr.xid, xsnap.ID)
					size += xsnap.Stats.Bytes
					objs += xsnap.Stats.Objs
					if xsnap.Running() {
						if xsnap.IsIdle() {
							debug.Assert(xact.IdlesBeforeFinishing(cpr.xname))
						} else {
							nrun++
						}
					}
					break // expecting one from target
				}
			}
		}
		cpr.updObjs(objs)
		cpr.updSize(size)
		if cpr.objs >= cpr.totals.objs && cpr.size >= cpr.totals.size {
			if nrun > 0 {
				time.Sleep(cpr.sleep)
			}
			break // NOTE: not waiting for all targets to finish
		}
		if nrun == 0 {
			if cpr.objs >= cpr.totals.objs && cpr.size >= cpr.totals.size {
				break
			}
			// force bars -> 100%
			cpr.updObjs(cpr.totals.objs)
			cpr.updSize(cpr.totals.size)
		}
		time.Sleep(cpr.sleep)
		totalWait += cpr.sleep
		cpr.sinceUpd += cpr.sleep
		if cpr.sinceUpd > timeoutNoChange && cpr.objs < cpr.totals.objs {
			rerr = fmt.Errorf("%s: timeout with no apparent progress for %v (%s)", cpr.loghdr, cpr.sinceUpd, cpr.log())
			break
		}
		if cpr.timeout != 0 && totalWait > cpr.timeout {
			rerr = fmt.Errorf("%s: timeout %v (%s)", cpr.loghdr, cpr.timeout, cpr.log())
			break
		}
		if cms.aborted {
			if cpr.objs > 0 {
				rerr = fmt.Errorf("%s: aborted (%s)", cpr.loghdr, cpr.log())
			} else {
				rerr = fmt.Errorf("%s: aborted", cpr.loghdr)
			}
			break
		}
	}

	if rerr != nil {
		cpr.abortObjs()
		cpr.abortSize()
		cpr.errCh <- rerr
	} else {
		cpr.errCh <- nil
	}
}

func (cpr *cprCtx) updObjs(objs int64) {
	if objs <= cpr.objs {
		return
	}
	if cpr.barObjs != nil {
		cpr.barObjs.IncrInt64(objs - cpr.objs)
	}
	cpr.objs = objs
	cpr.sinceUpd = 0
}

func (cpr *cprCtx) updSize(size int64) {
	if size <= cpr.size {
		return
	}
	if cpr.barSize != nil {
		cpr.barSize.IncrInt64(size - cpr.size)
	}
	cpr.size = size
	cpr.sinceUpd = 0
}

func (cpr *cprCtx) abortObjs() {
	if cpr.barObjs != nil {
		cpr.barObjs.Abort(true)
	}
}

func (cpr *cprCtx) abortSize() {
	if cpr.barSize != nil {
		cpr.barSize.Abort(true)
	}
}

func (cpr *cprCtx) log() string {
	return fmt.Sprintf("objs %d/%d, size %d/%d", cpr.objs, cpr.totals.objs, cpr.size, cpr.totals.size)
}
