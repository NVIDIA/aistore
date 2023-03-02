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

func copyBucketProgress(c *cli.Context, fromBck, toBck cmn.Bck) error {
	var (
		size, objs int64
		progress   *mpb.Progress
		bars       []*mpb.Bar
		from, to   = fromBck.DisplayName(), toBck.DisplayName()
		qbck       = cmn.QueryBcks(fromBck)
	)
	// 1. get summary
	summaries, err := bsummSlow(qbck, !flagIsSet(c, copyObjNotCachedFlag), true /*all buckets*/)
	if err != nil {
		return err
	}
	for _, res := range summaries {
		debug.Assertf(res.Bck.Equal(&fromBck), "%s != %s", res.Bck, fromBck)
		size += int64(res.TotalSize.PresentObjs + res.TotalSize.RemoteObjs)
		objs += int64(res.ObjCount.Present + res.ObjCount.Remote)
	}

	if objs == 0 {
		debug.Assert(size == 0)
		if flagIsSet(c, copyObjNotCachedFlag) {
			err = fmt.Errorf("source %s is empty, nothing to do", from)
		} else {
			err = fmt.Errorf("source %s has zero cached objects in the cluster, nothing to do"+
				" (hint: see %s for details)", from, qflprn(cli.HelpFlag))
		}
		return err
	}

	// 2. setup progress
	objsArg := barArgs{barType: unitsArg, barText: "Copied objects", total: objs}
	sizeArg := barArgs{barType: sizeArg, barText: "Copied size", total: size}
	progress, bars = simpleBar(objsArg, sizeArg)

	msg := &apc.CopyBckMsg{
		Prefix: parseStrFlag(c, copyPrefixFlag),
		DryRun: flagIsSet(c, copyDryRunFlag),
		Force:  flagIsSet(c, forceFlag),
	}
	xid, err := api.CopyBucket(apiBP, fromBck, toBck, msg)
	if err != nil {
		return err
	}

	// 3. poll x-copy-bucket asynchronously and update progress
	errCh := make(chan error, 1)
	go _cpr(bars, objs, size, xid, from, to, errCh)

	progress.Wait()

	// 4. done
	err = <-errCh
	if err == nil {
		actionDone(c, fmtXactSucceeded)
	}
	close(errCh)
	return err
}

// TODO -- FIXME: draft
func _cpr(bars []*mpb.Bar, totalObjs, totalSize int64, xid, from, to string, errCh chan error) {
	var (
		copiedObjs, copiedSize int64
		rerr                   error
		xargs                  = xact.ArgsMsg{ID: xid}
		barObjs, barSize       = bars[0], bars[1]
		// TODO -- FIXME: time management
		elapsed         time.Duration
		sleep           = time.Second
		timeoutNoChange = 10 * time.Second
	)
outer:
	for {
		var (
			size, objs int64
			nrunning   int
			xs, err    = queryXactions(xargs)
		)
		if err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
				time.Sleep(time.Second)
				continue
			}
			rerr = fmt.Errorf("failed to copy %s => %s: %v", from, to, err)
			break
		}
		for _, snaps := range xs {
			debug.Assert(len(snaps) < 2)
			for _, xsnap := range snaps {
				debug.Assertf(xid == xsnap.ID, "%q vs %q", xid, xsnap.ID)
				size += xsnap.Stats.Bytes
				objs += xsnap.Stats.Objs
				if xsnap.IsAborted() {
					rerr = fmt.Errorf("failed to copy %s => %s: operation aborted", from, to)
					break outer
				}
				if xsnap.Running() {
					nrunning++
				}
				break // expecting one from target
			}
		}
		if objs > copiedObjs {
			barObjs.IncrInt64(objs - copiedObjs)
			copiedObjs = objs
			elapsed = 0 // reset here and below
		}
		if size > copiedSize {
			barSize.IncrInt64(size - copiedSize)
			copiedSize = size
			elapsed = 0
		}
		if copiedObjs >= totalObjs && copiedSize >= totalSize {
			if nrunning > 0 {
				time.Sleep(sleep)
			}
			break // NOTE: not waiting for all to finish or, same, nrunning == 0
		}
		if nrunning == 0 {
			if copiedObjs >= totalObjs && copiedSize >= totalSize {
				break
			}
			if copiedObjs < totalObjs {
				barObjs.IncrInt64(totalObjs - copiedObjs) // to 100%
				copiedObjs = totalObjs
				elapsed = 0
			}
			if copiedSize < totalSize {
				barSize.IncrInt64(totalSize - copiedSize) // to 100%
				copiedSize = totalSize
				elapsed = 0
			}
		}
		time.Sleep(sleep)
		elapsed += sleep
		if elapsed > timeoutNoChange && copiedObjs < totalObjs {
			rerr = fmt.Errorf("timeout with no apparent progress being made for %v (objs %d/%d, size %d/%d)",
				elapsed, copiedObjs, totalObjs, copiedSize, totalSize)
			break
		}
	}

	if rerr != nil {
		barObjs.Abort(true)
		barSize.Abort(true)
		errCh <- rerr
	} else {
		errCh <- nil
	}
}
