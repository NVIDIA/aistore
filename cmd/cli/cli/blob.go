// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

func blobDownloadHandler(c *cli.Context) error {
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, false /*emptyObjnameOK*/)
	if err != nil {
		return err
	}
	if bck.Props, err = headBucket(bck, true /* add */); err != nil {
		return err
	}
	if !bck.IsRemote() {
		return fmt.Errorf("expecting remote bucket (have %s)", bck.Cname(""))
	}

	// message
	var msg apc.BlobMsg
	if flagIsSet(c, chunkSizeFlag) {
		msg.ChunkSize, err = parseSizeFlag(c, chunkSizeFlag)
		if err != nil {
			return err
		}
	}
	if flagIsSet(c, numWorkersFlag) {
		msg.NumWorkers = parseIntFlag(c, numWorkersFlag)
	}
	msg.LatestVer = flagIsSet(c, latestVerFlag)

	// do
	xid, err := api.BlobDownload(apiBP, bck, objName, &msg)
	if err != nil {
		return V(err)
	}

	text := fmt.Sprintf("%s[%s] %s", apc.ActBlobDl, xid, bck.Cname(objName))

	// progress
	if flagIsSet(c, progressFlag) {
		// TODO -- FIXME: node that runs xid (to set xargs.DaemonID), and full size for progress bar
		actionDone(c, text)
		var (
			xargs = xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl}
			// FIXME: hardcoded full size
			args           = barArgs{barType: sizeArg, barText: bck.Cname(objName), total: 100 * cos.MiB}
			progress, bars = simpleBar(args)
			longRun        = &longRun{}
			xs             xact.MultiSnap
			err            error
		)

		longRun.init(c, false /*run once unless*/)
		if longRun.refreshRate == 0 {
			longRun.refreshRate = _refreshRate(c)
		}
	outer:
		for countdown := longRun.count; countdown > 0 || longRun.isForever(); countdown-- {
			xs, err = api.QueryXactionSnaps(apiBP, &xargs)
			if err != nil {
				bars[0].Abort(true)
				break
			}
		inner:
			for _, snaps := range xs {
				for _, snap := range snaps {
					debug.Assert(snap.ID == xid)
					if snap.Stats.Bytes != 0 {
						bars[0].IncrInt64(snap.Stats.Bytes)
					}
					if snap.IsAborted() {
						err = errors.New(snap.AbortErr)
					}
					if snap.IsAborted() || snap.Finished() {
						bars[0].Abort(true)
						break outer
					}
					break inner // expecting one from target
				}
			}
			time.Sleep(longRun.refreshRate)
		}

		progress.Wait()
		return err
	}

	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		actionDone(c, text+". "+toMonitorMsg(c, xid, ""))
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: timeout}
	if err := waitXact(apiBP, &xargs); err != nil {
		fmt.Fprintf(c.App.ErrWriter, "Failed to download blob %s\n", bck.Cname(objName))
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return nil
}
