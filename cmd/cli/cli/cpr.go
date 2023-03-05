// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

const timeoutNoChange = 10 * time.Second // when stats stop moving for so much time

type cprCtx struct {
	errCh    chan error
	barObjs  *mpb.Bar
	barSize  *mpb.Bar
	xid      string
	from, to string
	xname    string
	loghdr   string
	totals   struct {
		objs int64
		size int64
	}
	timeout, sleep time.Duration
	// runtime
	objs     int64
	size     int64
	sinceUpd time.Duration
}

func (cpr *cprCtx) copyBucket(c *cli.Context, fromBck, toBck cmn.Bck) error {
	// 1. get summary
	qbck := cmn.QueryBcks(fromBck)
	summaries, err := bsummSlow(qbck, !flagIsSet(c, copyObjNotCachedFlag), true /*all buckets*/)
	if err != nil {
		return err
	}
	for _, res := range summaries {
		debug.Assertf(res.Bck.Equal(&fromBck), "%s != %s", res.Bck, fromBck)
		cpr.totals.size += int64(res.TotalSize.PresentObjs + res.TotalSize.RemoteObjs)
		cpr.totals.objs += int64(res.ObjCount.Present + res.ObjCount.Remote)
	}

	if cpr.totals.objs == 0 {
		debug.Assert(cpr.totals.size == 0)
		if flagIsSet(c, copyObjNotCachedFlag) {
			err = fmt.Errorf("source %s is empty, nothing to do", cpr.from)
		} else {
			err = fmt.Errorf("source %s doesn't have any cached objects in the cluster, nothing to do"+
				" (see %s for details)", cpr.from, qflprn(cli.HelpFlag))
		}
		return err
	}

	// 2. setup progress
	var (
		progress *mpb.Progress
		bars     []*mpb.Bar
		objsArg  = barArgs{barType: unitsArg, barText: "Copied objects:", total: cpr.totals.objs}
		sizeArg  = barArgs{barType: sizeArg, barText: "Copied size:   ", total: cpr.totals.size}
	)
	progress, bars = simpleBar(objsArg, sizeArg)
	cpr.barObjs, cpr.barSize = bars[0], bars[1]

	msg := &apc.CopyBckMsg{
		Prefix: parseStrFlag(c, copyPrefixFlag),
		DryRun: flagIsSet(c, copyDryRunFlag),
		Force:  flagIsSet(c, forceFlag),
	}
	cpr.xid, err = api.CopyBucket(apiBP, fromBck, toBck, msg)
	if err != nil {
		return err
	}
	cpr.loghdr = fmt.Sprintf("%s[%s] %s => %s", cpr.xname, cpr.xid, cpr.from, cpr.to)

	// 3. poll x-copy-bucket asynchronously and update the progress
	cpr.do(c)
	progress.Wait()

	// 4. done
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
outer:
	for {
		var (
			size, objs int64
			nrun       int
			xs, err    = queryXactions(xargs)
		)
		if err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
				time.Sleep(refreshRateMinDur)
				continue
			}
			rerr = fmt.Errorf("%s failed: %v", cpr.loghdr, err)
			break
		}
		for _, snaps := range xs {
			debug.Assert(len(snaps) < 2)
			for _, xsnap := range snaps {
				debug.Assertf(cpr.xid == xsnap.ID, "%q vs %q", cpr.xid, xsnap.ID)
				size += xsnap.Stats.Bytes
				objs += xsnap.Stats.Objs
				if xsnap.IsAborted() {
					rerr = fmt.Errorf("%s failed: aborted", cpr.loghdr)
					break outer
				}
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
