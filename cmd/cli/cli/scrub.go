// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"os"
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

// [TODO] in order of priority:
// - hide zero columns
//   - emit "100% healthy" when all counters are zero
// - when waiting for a long time, show log filenames (to tail -f)
// - version-changed
// - version-deleted
// - async execution
// - pretty print

type (
	_log struct {
		fh  *os.File
		tag string
		fn  string
		cnt int
		mu  sync.Mutex
	}
	// fields exported => teb/template
	scrubOne teb.ScrubOne
)

type (
	scrubCtx struct {
		c      *cli.Context
		scrubs []*scrubOne
		qbck   cmn.QueryBcks
		pref   string
		units  string
		// sizing
		small int64
		large int64
		// timing
		ival time.Duration
		last atomic.Int64
		// detailed log
		log struct {
			misplaced _log
			missing   _log
			small     _log
			large     _log
			vchanged  _log
			vremoved  _log
		}
		_many bool
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

	if err := ctx.createLogs(); err != nil {
		return err
	}

	ctx.units, err = parseUnitsFlag(ctx.c, unitsFlag)
	if err != nil {
		return err
	}

	if ctx.qbck.IsBucket() {
		err = waitForFunc(ctx.one, ctx.ival)
	} else {
		err = waitForFunc(ctx.many, ctx.ival)
	}

	ctx.closeLogs(c)
	return err
}

//////////////
// scrubCtx //
//////////////

func (ctx *scrubCtx) createLogs() error {
	var (
		logs = []*_log{&ctx.log.misplaced, &ctx.log.missing, &ctx.log.small, &ctx.log.large, &ctx.log.vchanged, &ctx.log.vremoved}
		tags = []string{"misplaced", "missing", "small", "large", "version-changed", "version-removed"}
	)
	debug.Assert(len(logs) == len(tags))
	for i := range logs {
		logs[i].tag = tags[i]
		if err := ctx._create(logs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (ctx *scrubCtx) closeLogs(c *cli.Context) {
	var (
		logs   = []*_log{&ctx.log.misplaced, &ctx.log.missing, &ctx.log.small, &ctx.log.large, &ctx.log.vchanged, &ctx.log.vremoved}
		titled bool
	)
	for _, log := range logs {
		cos.Close(log.fh)
		if log.cnt == 0 {
			cos.RemoveFile(log.fn)
			continue
		}
		if !titled {
			const title = "Detailed Logs"
			fmt.Fprintln(c.App.Writer)
			fmt.Fprintln(c.App.Writer, fcyan(title))
			fmt.Fprintln(c.App.Writer, strings.Repeat("-", len(title)))
			titled = true
		}
		fmt.Fprintf(c.App.Writer, "* %s objects: %s (%d record%s)\n", log.tag, log.fn, log.cnt, cos.Plural(log.cnt))
	}
}

func (*scrubCtx) _create(log *_log) (err error) {
	fn := fmt.Sprintf(".ais-scrub-%s.%d.log", log.tag, os.Getpid())
	log.fn = filepath.Join(os.TempDir(), fn)
	log.fh, err = cos.CreateFile(log.fn)
	return err
}

func (ctx *scrubCtx) many() error {
	bcks, err := api.ListBuckets(apiBP, ctx.qbck, apc.FltPresent)
	if err != nil {
		return V(err)
	}
	num := len(bcks)
	if num == 1 {
		ctx.qbck = cmn.QueryBcks(bcks[0])
		return ctx.one()
	}
	debug.Assert(num > 1)

	// many
	ctx._many = true
	var (
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), num)
		mu = &sync.Mutex{}
	)
	ctx.scrubs = make([]*scrubOne, 0, num)
	for i := range bcks {
		bck := bcks[i]
		wg.Add(1)
		go ctx.gols(bck, wg, mu)
	}
	wg.Wait()

	return ctx.prnt()
}

// print and be done
func (ctx *scrubCtx) prnt() error {
	out := make([]*teb.ScrubOne, len(ctx.scrubs))
	for i, scr := range ctx.scrubs {
		out[i] = (*teb.ScrubOne)(scr)
	}
	all := teb.ScrubHelper{All: out}
	tab := all.MakeTab(ctx.units)

	return teb.Print(out, tab.Template(flagIsSet(ctx.c, noHeaderFlag)))
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

	ctx.scrubs = []*scrubOne{scr}
	return ctx.prnt()
}

func (ctx *scrubCtx) ls(bck cmn.Bck) (*scrubOne, error) {
	bprops, errV := headBucket(bck, true /* don't add */)
	if errV != nil {
		return nil, errV
	}
	bck.Props = bprops
	var (
		lsargs api.ListArgs
		scr    = &scrubOne{Bck: bck, Prefix: ctx.pref}
		lsmsg  = &apc.LsoMsg{
			Prefix: ctx.pref,
			Flags:  apc.LsObjCached | apc.LsMissing | apc.LsVerChanged,
		}
	)
	lsmsg.AddProps(apc.GetPropsName, apc.GetPropsSize, apc.GetPropsCustom)

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
			scr.upd(ctx, en, bprops)
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

//////////////
// scrubOne //
//////////////

func (scr *scrubOne) upd(parent *scrubCtx, en *cmn.LsoEnt, bprops *cmn.Bprops) {
	scr.Listed++
	if !en.IsStatusOK() {
		scr.Stats.Misplaced++
		scr.log(&parent.log.misplaced, scr.Bck.Cname(en.Name), parent._many)
		return // no further checking
	}

	if bprops.Mirror.Enabled && en.Copies < int16(bprops.Mirror.Copies) {
		scr.Stats.MissingCp++
		scr.log(&parent.log.missing, scr.Bck.Cname(en.Name), parent._many)
	}

	if en.Size <= parent.small {
		scr.Stats.SmallSz++
		scr.log(&parent.log.small, scr.Bck.Cname(en.Name), parent._many)
	} else if en.Size >= parent.large {
		scr.Stats.LargeSz++
		scr.log(&parent.log.large, scr.Bck.Cname(en.Name), parent._many)
	}

	if en.IsVerChanged() {
		scr.Stats.Vchanged++
		scr.log(&parent.log.vchanged, scr.Bck.Cname(en.Name), parent._many)
	} else if en.IsVerRemoved() {
		scr.Stats.Vremoved++
		scr.log(&parent.log.vremoved, scr.Bck.Cname(en.Name), parent._many)
	}
}

func (*scrubOne) log(to *_log, s string, lock bool) {
	if lock {
		to.mu.Lock()
	}
	fmt.Fprintln(to.fh, s)
	to.cnt++
	if lock {
		to.mu.Unlock()
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
