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
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

func blobDownloadHandler(c *cli.Context) error {
	var (
		objNames []string
		bck      cmn.Bck
		err      error
		uri      = c.Args().Get(0)
		jobs     map[string]string
	)
	if flagIsSet(c, listFlag) {
		listObjs := parseStrFlag(c, listFlag)
		objNames = splitCsv(listObjs)
		bck, err = parseBckURI(c, uri, true)
	} else {
		var objName string
		bck, objName, err = parseBckObjURI(c, uri, false /*emptyObjnameOK*/)
		objNames = []string{objName}
	}
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

	// start
	jobs = make(map[string]string, len(objNames))
	for _, o := range objNames {
		xid, err := api.BlobDownload(apiBP, bck, o, &msg)
		if err != nil {
			for xid := range jobs {
				errN := api.AbortXaction(apiBP, &xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl})
				if errN != nil {
					actionWarn(c, errN.Error())
				}
			}
			return V(err)
		}
		jobs[xid] = o
	}
	for xid, objName := range jobs {
		var errN error
		if flagIsSet(c, progressFlag) {
			errN = _blobOneProgress(c, bck, xid, objName)
		} else {
			errN = _blobOne(c, bck, xid, objName)
		}
		if errN != nil {
			if len(jobs) > 1 {
				actionWarn(c, errN.Error())
			}
			err = errN
		}
	}
	return err
}

func _blobOneProgress(c *cli.Context, bck cmn.Bck, xid, objName string) error {
	fmt.Fprintln(c.App.Writer, fcyan(fmt.Sprintf("%s[%s]", apc.ActBlobDl, xid)))
	var (
		// total is zero at init time with full size set upon the first call ("known")
		args           = barArgs{barType: sizeArg, barText: bck.Cname(objName), total: 0}
		progress, bars = simpleBar(args)
		errCh          = make(chan error, 1)
	)
	go func(c *cli.Context, xid string, errCh chan error) {
		var (
			size        int64
			refreshRate = _refreshRate(c)
			xargs       = xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl}
			sizeKnown   bool
		)
		for {
			newSize, done, known, err := blobProgress(&xargs, bars[0], size, sizeKnown)
			if err != nil {
				bars[0].Abort(true)
				errCh <- err
				return
			}
			if done {
				bars[0].SetTotal(newSize, true)
				return
			}
			time.Sleep(refreshRate)
			size, sizeKnown = newSize, known
		}
	}(c, xid, errCh)
	progress.Wait()

	var err error
	select {
	case err = <-errCh:
	default:
	}
	if err == nil {
		actionDone(c, fmtXactSucceeded)
	}
	close(errCh)
	return err
}

func _blobOne(c *cli.Context, bck cmn.Bck, xid, objName string) error {
	text := fmt.Sprintf("%s[%s] %s", apc.ActBlobDl, xid, bck.Cname(objName))
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

func blobProgress(xargs *xact.ArgsMsg, bar *mpb.Bar, prevSize int64, knownSize bool) (newSize int64, done, sizeKnown bool, err error) {
	daemonID, snap, errN := getXactSnap(xargs)
	if errN != nil {
		return 0, false, false, errN
	}
	done = snap.Finished()
	debug.Assert(snap.ID == xargs.ID)
	if xargs.DaemonID == "" {
		xargs.DaemonID = daemonID
	}

	if !knownSize && snap.Stats.InBytes != 0 {
		sizeKnown = true
		bar.SetTotal(snap.Stats.InBytes, false)
	}

	debug.Assert(xargs.DaemonID == daemonID)
	if snap.Stats.Bytes != 0 {
		newSize = snap.Stats.Bytes
		bar.IncrInt64(newSize - prevSize)
	}
	if snap.IsAborted() {
		err = errors.New(snap.AbortErr)
	}
	return
}
