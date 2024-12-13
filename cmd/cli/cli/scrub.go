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
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/sys"
	"github.com/urfave/cli"
)

type (
	scrubCtx struct {
		c      *cli.Context
		scrubs []*scrubOne
		qbck   cmn.QueryBcks
		pref   string
		tmpl   string
		// sizing
		small int64
		large int64
		// timing
		ival time.Duration
		last atomic.Int64
	}
	// fields exported => teb/template
	scrubOne struct {
		parent *scrubCtx
		Bck    cmn.Bck
		Listed uint64
		Stats  struct {
			Misplaced uint64
			MissingCp uint64
			SmallSz   uint64
			LargeSz   uint64
		}
	}
)

func scrubHandler(c *cli.Context) (err error) {
	var (
		ctx = scrubCtx{c: c}
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

	ctx.last.Store(mono.NanoTime()) // pace interim results
	ctx.tmpl = teb.ScrubTmpl
	if flagIsSet(ctx.c, noHeaderFlag) {
		ctx.tmpl = teb.ScrubBody
	}

	ctx.ival = listObjectsWaitTime
	if flagIsSet(c, refreshFlag) {
		ctx.ival = parseDurationFlag(c, refreshFlag)
	}
	ctx.ival = max(ctx.ival, 5*time.Second)

	if flagIsSet(c, smallSizeFlag) {
		ctx.small, err = parseSizeFlag(c, smallSizeFlag)
		if err != nil {
			return err
		}
	}
	if ctx.small < 0 {
		return fmt.Errorf("%s (%s) cannot be negative", qflprn(smallSizeFlag), cos.ToSizeIEC(ctx.small, 0))
	}

	ctx.large = 5 * cos.GiB
	if flagIsSet(c, largeSizeFlag) {
		ctx.large, err = parseSizeFlag(c, largeSizeFlag)
		if err != nil {
			return err
		}
	}
	if ctx.large < ctx.small {
		return fmt.Errorf("%s (%s) cannot be smaller than %s (%s)",
			qflprn(largeSizeFlag), cos.ToSizeIEC(ctx.large, 0),
			qflprn(smallSizeFlag), cos.ToSizeIEC(ctx.small, 0))
	}

	// TODO -- FIXME: support async execution
	if ctx.qbck.IsBucket() {
		return waitForFunc(ctx.one, ctx.ival)
	}
	return waitForFunc(ctx.many, ctx.ival)
}

//////////////
// scrubOne //
//////////////

func (scr *scrubOne) upd(en *cmn.LsoEnt, bprops *cmn.Bprops) {
	scr.Listed++
	if !en.IsStatusOK() {
		scr.Stats.Misplaced++
		return
	}
	if bprops.Mirror.Enabled && en.Copies < int16(bprops.Mirror.Copies) {
		scr.Stats.MissingCp++
	}
	if en.Size <= scr.parent.small {
		scr.Stats.SmallSz++
	} else if en.Size >= scr.parent.large {
		scr.Stats.LargeSz++
	}
}

func (scr *scrubOne) toSB(sb *strings.Builder, total int) {
	sb.WriteString(scr.Bck.Cname(""))
	sb.WriteString(": scrubbed ")
	sb.WriteString(cos.FormatBigNum(total))
	sb.WriteString(" names")

	var scr0 scrubOne
	if scr.Stats == scr0.Stats {
		return
	}

	sb.WriteByte(' ')
	s := fmt.Sprintf("%+v", scr.Stats)
	sb.WriteString(s)
}

//////////////
// scrubCtx //
//////////////

func (ctx *scrubCtx) many() error {
	bcks, err := api.ListBuckets(apiBP, ctx.qbck, apc.FltPresent)
	if err != nil {
		return V(err)
	}
	var (
		num = len(bcks)
		wg  = cos.NewLimitedWaitGroup(sys.NumCPU(), num)
		mu  = &sync.Mutex{}
	)
	ctx.scrubs = make([]*scrubOne, 0, num)
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

func (ctx *scrubCtx) gols(bck cmn.Bck, wg cos.WG, mu *sync.Mutex) {
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

func (ctx *scrubCtx) one() error {
	scr, err := ctx.ls(cmn.Bck(ctx.qbck))
	if err != nil {
		return err
	}
	return teb.Print([]*scrubOne{scr}, ctx.tmpl)
}

func (ctx *scrubCtx) ls(bck cmn.Bck) (*scrubOne, error) {
	bprops, errV := headBucket(bck, true /* don't add */)
	if errV != nil {
		return nil, errV
	}
	bck.Props = bprops
	var (
		lsargs api.ListArgs
		scr    = &scrubOne{parent: ctx, Bck: bck}
		lsmsg  = &apc.LsoMsg{Prefix: ctx.pref, Flags: apc.LsObjCached | apc.LsMissing}
	)
	lsmsg.AddProps(apc.GetPropsName, apc.GetPropsSize)

	pageSize, maxPages, limit, err := _setPage(ctx.c, bck)
	if err != nil {
		return nil, err
	}
	lsmsg.PageSize = pageSize
	lsargs.Limit = limit

	var (
		pgcnt  int
		listed int
		yelped bool
	)
	// pages
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
		pgcnt++
		if maxPages > 0 && pgcnt >= int(maxPages) {
			break
		}
		listed += len(lst.Entries)
		if limit > 0 && listed >= int(limit) {
			break
		}

		//
		// show interim results
		//
		const maxline = 128
		var (
			sb   strings.Builder
			now  = mono.NanoTime()
			last = ctx.last.Load()
		)
		if !yelped {
			if time.Duration(now-last) < ctx.ival+2*time.Second {
				continue
			}
		} else {
			if time.Duration(now-last) < ctx.ival {
				continue
			}
		}
		if ctx.last.CAS(last, now) {
			sb.Grow(maxline)
			scr.toSB(&sb, listed)
			l := sb.Len()
			if len(ctx.scrubs) > 1 {
				// in an attempt to fit multiple gols() updaters
				for range maxline - l {
					sb.WriteByte(' ')
				}
			}
			fmt.Fprintf(ctx.c.App.Writer, "\r%s", sb.String())
			yelped = true
		}
	}
	if yelped {
		fmt.Fprintln(ctx.c.App.Writer)
	}

	return scr, nil
}
