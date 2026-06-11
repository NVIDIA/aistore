// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

type (
	shardSummCtx struct {
		c      *cli.Context
		units  string
		bck    cmn.Bck
		prefix string
		msg    apc.ShardSummMsg
		args   api.ShardSummArgs
		l      int
		n      int
	}
	shardSummRow struct {
		Name       string
		NotIndexed uint64
		apc.ShardSummResult
	}
)

func shardIndexSummaryHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing bucket name")
	}
	if c.NArg() > 2 {
		return incorrectUsageMsg(c, "", c.Args()[2:])
	}

	bck, objName, err := parseBckObjURI(c, c.Args().Get(0), true /*optObjName*/)
	if err != nil {
		return err
	}
	prefix, err := parseBckObjPrefix(c, objName)
	if err != nil {
		return err
	}
	ctx, err := newShardSummCtxMsg(c, bck, prefix)
	if err != nil {
		return err
	}

	// Optional JOB_ID means polling an existing summary job.
	isNew := true
	if xid := c.Args().Get(1); xid != "" {
		if !cos.IsValidUUID(xid) {
			return incorrectUsageMsg(c, "invalid job ID %q", xid)
		}
		if !ctx.args.DontWait {
			return incorrectUsageMsg(c, "JOB_ID requires %s", flprn(dontWaitFlag))
		}
		ctx.msg.UUID = xid
		isNew = false
	}

	// Start or poll the summary, depending on msg.UUID and --dont-wait.
	xid, res, err := api.GetBucketShardSummary(apiBP, ctx.bck, &ctx.msg, ctx.args)

	// New --dont-wait starts the job; if no snapshot exists yet, print the xaction ID.
	dontWait := flagIsSet(c, dontWaitFlag)
	if err == nil && dontWait && isNew && res.IsEmpty() {
		actionDone(c, shardSummStartedMsg(xid, bck.Cname(prefix), isNew))
		return nil
	}

	// For --dont-wait polling, 202 means no snapshot yet; 206 prints the latest partial snapshot.
	var status int
	if err != nil {
		if herr := cmn.AsErrHTTP(err); herr != nil {
			status = herr.Status
		}
		if dontWait && status == http.StatusAccepted {
			actionDone(c, shardSummStartedMsg(xid, bck.Cname(prefix), isNew))
			return nil
		}
		if dontWait && status == http.StatusPartialContent {
			msg := fmt.Sprintf("%s[%s] is still running - showing partial results:", apc.ActSummaryShard, ctx.msg.UUID)
			actionNote(c, msg)
			err = nil
		}
	}
	if err != nil {
		return V(err)
	}
	// Print the summary table.
	return ctx.print(res)
}

func shardSummStartedMsg(xid, bucket string, isNew bool) string {
	verb := "has started"
	if !isNew {
		verb = "is running"
	}
	return fmt.Sprintf("Job %s[%s] %s. To monitor, run 'ais bucket shard-index summary %s %s %s'",
		apc.ActSummaryShard, xid, verb, bucket, xid, flprn(dontWaitFlag))
}

func newShardSummCtxMsg(c *cli.Context, bck cmn.Bck, prefix string) (*shardSummCtx, error) {
	units, err := parseUnitsFlag(c, unitsFlag)
	if err != nil {
		return nil, err
	}
	ctx := &shardSummCtx{
		c:      c,
		units:  units,
		bck:    bck,
		prefix: prefix,
	}
	ctx.msg.Prefix = prefix
	if ctx.args.DontWait = flagIsSet(c, dontWaitFlag); ctx.args.DontWait {
		return ctx, nil
	}

	ctx.args.CallAfter = _refreshRate(c)
	ctx.args.Callback = ctx.progress
	return ctx, nil
}

func (ctx *shardSummCtx) progress(res *apc.ShardSummResult, done bool) {
	if done {
		if ctx.n > 0 {
			fmt.Fprintln(ctx.c.App.Writer)
		}
		return
	}
	if res == nil || res.IsEmpty() {
		return
	}
	ctx.n++

	s := fmt.Sprintf("%s: %s/%s indexed (%s indexed, %s total)",
		ctx.bck.Cname(ctx.prefix),
		cos.FormatBigI64(int64(res.Shards)),
		cos.FormatBigI64(int64(res.TarObjs)),
		teb.FmtSize(int64(res.ShardSize), ctx.units, 2),
		teb.FmtSize(int64(res.TarSize), ctx.units, 2))
	if ctx.l < len(s) {
		ctx.l = len(s) + 4
	}
	s += strings.Repeat(" ", ctx.l-len(s))
	fmt.Fprintf(ctx.c.App.Writer, "\r%s", s)
}

func (ctx *shardSummCtx) print(res *apc.ShardSummResult) error {
	row := shardSummRow{
		Name:            ctx.bck.Cname(ctx.prefix),
		NotIndexed:      shardSummNotIndexed(res),
		ShardSummResult: *res,
	}
	opts := teb.Opts{AltMap: teb.FuncMapUnits(ctx.units, false /*incl. calendar date*/)}
	if flagIsSet(ctx.c, noHeaderFlag) {
		return teb.Print([]shardSummRow{row}, teb.ShardSummariesBody, opts)
	}
	return teb.Print([]shardSummRow{row}, teb.ShardSummariesTmpl, opts)
}

func shardSummNotIndexed(res *apc.ShardSummResult) uint64 {
	if res.Shards > res.TarObjs {
		return 0
	}
	return res.TarObjs - res.Shards
}
