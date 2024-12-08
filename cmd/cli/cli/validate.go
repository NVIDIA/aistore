// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

// TODO: revisit

type (
	preScrub struct {
		Bck           cmn.Bck
		ObjectCnt     uint64
		Misplaced     uint64
		MissingCopies uint64
		ZeroSize      uint64
		FiveGBplus    uint64
	}
	ctxScrub struct {
		c    *cli.Context
		qbck cmn.QueryBcks
		pref string
	}
)

func prelimScrub(c *cli.Context) (err error) {
	var (
		ctx = ctxScrub{c: c}
		uri = preparseBckObjURI(c.Args().Get(0))
	)
	ctx.qbck, ctx.pref, err = parseQueryBckURI(uri)
	if err != nil {
		return err
	}

	// embedded prefix vs '--prefix'
	prefix := parseStrFlag(c, bsummPrefixFlag)
	switch {
	case ctx.pref != "" && prefix != "":
		s := fmt.Sprintf(": via '%s' and %s option", uri, qflprn(bsummPrefixFlag))
		if ctx.pref != prefix {
			return errors.New("two different prefix values" + s)
		}
		actionWarn(c, "redundant and duplicated prefix assignment"+s)
	case prefix != "":
		ctx.pref = prefix
	}

	return waitForFunc(ctx.do, longClientTimeout)
}

//////////////
// preScrub //
//////////////

func (scr *preScrub) upd(en *cmn.LsoEnt, bprops *cmn.Bprops) {
	scr.ObjectCnt++
	if !en.IsStatusOK() {
		scr.Misplaced++
		return
	}
	if bprops.Mirror.Enabled && en.Copies < int16(bprops.Mirror.Copies) {
		scr.MissingCopies++
	}
	if en.Size == 0 {
		scr.ZeroSize++
	} else if en.Size >= 5*cos.GB {
		scr.FiveGBplus++
	}
}

//////////////
// ctxScrub //
//////////////

func (ctx *ctxScrub) do() error {
	// TODO: when !ctx.qbck.IsQuery do HEAD instead of list-buckets, skip HEADing below
	bcks, err := api.ListBuckets(apiBP, ctx.qbck, apc.FltPresent)
	if err != nil {
		return V(err)
	}

	var (
		scrubs = make([]*preScrub, 0, len(bcks))
		msg    = &apc.LsoMsg{Prefix: ctx.pref, Flags: apc.LsObjCached | apc.LsMissing}
	)
	msg.AddProps(apc.GetPropsAll...)

	for i := range bcks {
		bck := bcks[i]
		if ctx.qbck.Name != "" && !ctx.qbck.Equal(&bck) {
			continue
		}

		bprops, err := headBucket(bck, true /* don't add */)
		if err != nil {
			return err
		}

		var (
			callAfter = listObjectsWaitTime
			_listed   = &_listed{c: ctx.c, bck: &bck, msg: msg}
			lsargs    api.ListArgs
		)
		if flagIsSet(ctx.c, refreshFlag) {
			callAfter = parseDurationFlag(ctx.c, refreshFlag)
		}

		lsargs.Callback = _listed.cb
		lsargs.CallAfter = callAfter
		lst, err := api.ListObjects(apiBP, bck, msg, lsargs)
		if err != nil {
			return err
		}

		scr := &preScrub{Bck: bck}
		for _, en := range lst.Entries {
			if en.IsDir() || cos.IsLastB(en.Name, filepath.Separator) {
				continue
			}
			debug.Assert(en.IsPresent(), bck.Cname(en.Name), " expected to be present") // vs apc.LsObjCached
			scr.upd(en, bprops)
		}
		scrubs = append(scrubs, scr)
	}

	return teb.Print(scrubs, teb.BucketSummaryValidateTmpl)
}
