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
	"sync"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/sys"
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
		c      *cli.Context
		scrubs []*preScrub
		qbck   cmn.QueryBcks
		pref   string
		tmpl   string
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

	ctx.tmpl = teb.BucketSummaryValidateTmpl
	if flagIsSet(ctx.c, noHeaderFlag) {
		ctx.tmpl = teb.BucketSummaryValidateBody
	}
	if ctx.qbck.IsBucket() {
		return waitForFunc(ctx.one, longClientTimeout)
	}
	return waitForFunc(ctx.many, longClientTimeout)
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

func (ctx *ctxScrub) many() error {
	bcks, err := api.ListBuckets(apiBP, ctx.qbck, apc.FltPresent)
	if err != nil {
		return V(err)
	}
	var (
		num = len(bcks)
		wg  = cos.NewLimitedWaitGroup(sys.NumCPU(), num)
		mu  = &sync.Mutex{}
	)
	ctx.scrubs = make([]*preScrub, 0, num)
	for i := range bcks {
		bck := bcks[i]
		if ctx.qbck.Name != "" && !ctx.qbck.Equal(&bck) {
			continue
		}

		wg.Add(1)
		go ctx.gols(bck, wg, mu)
	}
	wg.Wait()

	return teb.Print(ctx.scrubs, ctx.tmpl)
}

func (ctx *ctxScrub) gols(bck cmn.Bck, wg cos.WG, mu *sync.Mutex) {
	defer wg.Done()
	scr, err := ctx.ls(bck)
	if err != nil {
		warn := fmt.Sprintf("cannot validate %s: %v", bck.Cname(ctx.pref), err)
		actionWarn(ctx.c, warn)
		return
	}
	mu.Lock()
	ctx.scrubs = append(ctx.scrubs, scr)
	mu.Unlock()
}

func (ctx *ctxScrub) one() error {
	scr, err := ctx.ls(cmn.Bck(ctx.qbck))
	if err != nil {
		return err
	}
	return teb.Print([]*preScrub{scr}, ctx.tmpl)
}

func (ctx *ctxScrub) ls(bck cmn.Bck) (*preScrub, error) {
	bprops, err := headBucket(bck, true /* don't add */)
	if err != nil {
		return nil, err
	}
	bck.Props = bprops
	var (
		callAfter = listObjectsWaitTime
		lsargs    api.ListArgs
		lsmsg     = &apc.LsoMsg{Prefix: ctx.pref, Flags: apc.LsObjCached | apc.LsMissing}
		_listed   = &_listed{c: ctx.c, bck: &bck, msg: lsmsg}
	)
	lsmsg.AddProps(apc.GetPropsName, apc.GetPropsSize)

	if flagIsSet(ctx.c, refreshFlag) {
		callAfter = parseDurationFlag(ctx.c, refreshFlag)
	}

	scr := &preScrub{Bck: bck}

	pageSize, maxPages, limit, err := _setPage(ctx.c, bck)
	if err != nil {
		return nil, err
	}
	lsmsg.PageSize = pageSize
	{
		lsargs.Callback = _listed.cb
		lsargs.CallAfter = callAfter
		lsargs.Limit = limit
	}

	// pages
	pageCounter, toShow := 0, int(limit)
	for {
		lst, err := api.ListObjectsPage(apiBP, bck, lsmsg, lsargs)
		if err != nil {
			return nil, err
		}
		// one page
		for _, en := range lst.Entries {
			if en.IsDir() || cos.IsLastB(en.Name, filepath.Separator) {
				continue
			}
			debug.Assert(en.IsPresent(), bck.Cname(en.Name), " must be present") // (LsObjCached)
			scr.upd(en, bprops)
		}

		if lsmsg.ContinuationToken == "" {
			break
		}
		pageCounter++
		if maxPages > 0 && pageCounter >= int(maxPages) {
			break
		}
		if limit > 0 {
			toShow -= len(lst.Entries)
			if toShow <= 0 {
				break
			}
		}
	}

	return scr, nil
}
