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
	"strconv"
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

// [TODO]
// - when waiting for a long time, show log filenames (to tail -f)
// - async execution, with --wait option
// - add options:
//   --mountpath-misplaced
//   --checksum
//   --fix (***)
// - speed-up `ls` via multiple workers (***)
// - reuse strings.Builder buf, here and elsewhere

type (
	_log struct {
		fh  *os.File
		tag string
		fn  string
		cnt int
		mu  sync.Mutex
	}
	// fields exported => teb/template
	scrBp teb.ScrBp
)

type (
	scrCtx struct {
		c      *cli.Context
		scrubs []*scrBp
		qbck   cmn.QueryBcks
		pref   string
		units  string
		// sizing
		small int64
		large int64
		// timing
		ival time.Duration
		last atomic.Int64
		// detailed logs
		logs       [teb.ScrNumStats]_log
		_many      bool
		haveRemote atomic.Bool
	}
)

func scrubHandler(c *cli.Context) (err error) {
	var (
		ctx = scrCtx{c: c}
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
	if ctx.large <= ctx.small {
		return fmt.Errorf("%s (%s) must be greater than %s (%s)",
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
// scrCtx //
//////////////

func (ctx *scrCtx) createLogs() error {
	pid := os.Getpid()
	for i := 1; i < len(ctx.logs); i++ { // skipping listed objects
		log := &ctx.logs[i]
		log.tag = strings.ToLower(teb.ScrCols[i])
		if err := ctx._create(log, pid); err != nil {
			// cleanup
			for j := range i - 1 {
				cos.Close(ctx.logs[j].fh)
				cos.RemoveFile(ctx.logs[j].fn)
			}
			return err
		}
	}
	return nil
}

func (*scrCtx) _create(log *_log, pid int) (err error) {
	fn := fmt.Sprintf(".ais-scrub-%s.%d.log", log.tag, pid)
	log.fn = filepath.Join(os.TempDir(), fn)
	log.fh, err = cos.CreateFile(log.fn)
	return err
}

func (ctx *scrCtx) closeLogs(c *cli.Context) {
	var titled bool
	for i := 1; i < len(ctx.logs); i++ { // skipping listed objects
		log := &ctx.logs[i]
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

func (ctx *scrCtx) many() error {
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
	ctx.scrubs = make([]*scrBp, 0, num)
	for i := range bcks {
		bck := bcks[i]
		wg.Add(1)
		go ctx.gols(bck, wg, mu)
	}
	wg.Wait()

	return ctx.prnt()
}

// print and be done
func (ctx *scrCtx) prnt() error {
	out := make([]*teb.ScrBp, len(ctx.scrubs))
	for i, scr := range ctx.scrubs {
		out[i] = (*teb.ScrBp)(scr)
	}
	all := teb.ScrubHelper{All: out}
	tab := all.MakeTab(ctx.units, ctx.haveRemote.Load())

	return teb.Print(out, tab.Template(flagIsSet(ctx.c, noHeaderFlag)))
}

func (ctx *scrCtx) gols(bck cmn.Bck, wg cos.WG, mu *sync.Mutex) {
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

func (ctx *scrCtx) one() error {
	scr, err := ctx.ls(cmn.Bck(ctx.qbck))
	if err != nil {
		return err
	}

	ctx.scrubs = []*scrBp{scr}
	return ctx.prnt()
}

func (ctx *scrCtx) ls(bck cmn.Bck) (*scrBp, error) {
	bprops, errV := headBucket(bck, true /* don't add */)
	if errV != nil {
		return nil, errV
	}
	bck.Props = bprops

	var (
		lsargs api.ListArgs
		scr    = &scrBp{Bck: bck, Prefix: ctx.pref}
		lsmsg  = &apc.LsoMsg{
			Prefix: ctx.pref,
			Flags:  apc.LsMissing,
		}
	)
	propNames := []string{apc.GetPropsName, apc.GetPropsSize, apc.GetPropsAtime, apc.GetPropsLocation, apc.GetPropsCustom}
	if bck.IsRemote() {
		lsmsg.Flags |= apc.LsObjCached | apc.LsVerChanged
		lsmsg.AddProps(propNames...)
		ctx.haveRemote.Store(true) // incl. version-changed etc. columns
	} else {
		lsmsg.AddProps(propNames[:len(propNames)-1]...) // minus apc.GetPropsCustom
	}

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
	// main loop: pages
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
			scr.upd(ctx, en, &bck)
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
// scrBp //
//////////////

func (scr *scrBp) upd(parent *scrCtx, en *cmn.LsoEnt, bck *cmn.Bck) {
	scr.Stats[teb.ScrObjects].Cnt++
	scr.Stats[teb.ScrObjects].Siz += en.Size

	if !en.IsStatusOK() {
		scr.Stats[teb.ScrMisplaced].Cnt++
		scr.Stats[teb.ScrMisplaced].Siz += en.Size
		scr.log(parent, en, bck, teb.ScrMisplaced)
		// no further checking
		return
	}

	if bck.Props.Mirror.Enabled && en.Copies < int16(bck.Props.Mirror.Copies) {
		scr.Stats[teb.ScrMissingCp].Cnt++
		scr.log(parent, en, bck, teb.ScrMissingCp)
	}

	if en.Size <= parent.small {
		scr.Stats[teb.ScrSmallSz].Cnt++
		scr.Stats[teb.ScrSmallSz].Siz += en.Size
		scr.log(parent, en, bck, teb.ScrSmallSz)
	} else if en.Size >= parent.large {
		scr.Stats[teb.ScrLargeSz].Cnt++
		scr.Stats[teb.ScrLargeSz].Siz += en.Size
		scr.log(parent, en, bck, teb.ScrLargeSz)
	}

	if en.IsVerChanged() {
		scr.Stats[teb.ScrVchanged].Cnt++
		scr.Stats[teb.ScrVchanged].Siz += en.Size
		scr.log(parent, en, bck, teb.ScrVchanged)
	} else if en.IsVerRemoved() {
		scr.Stats[teb.ScrVremoved].Cnt++
		scr.Stats[teb.ScrVremoved].Siz += en.Size
		scr.log(parent, en, bck, teb.ScrVremoved)
	}
}

const (
	logTitle = "Name,Size,Atime,Location"
	delim    = `","`
)

func (*scrBp) log(parent *scrCtx, en *cmn.LsoEnt, bck *cmn.Bck, i int) {
	log := &parent.logs[i]
	if parent._many {
		log.mu.Lock()
	}
	if log.cnt == 0 {
		fmt.Fprintln(log.fh, logTitle)
		fmt.Fprintln(log.fh, strings.Repeat("=", len(logTitle)))
	}

	var sb strings.Builder
	sb.Grow(256)
	sb.WriteByte('"')
	sb.WriteString(bck.Cname(en.Name))
	sb.WriteString(delim)
	sb.WriteString(strconv.FormatInt(en.Size, 10))
	sb.WriteString(delim)
	sb.WriteString(en.Atime)
	sb.WriteString(delim)
	sb.WriteString(en.Location)
	sb.WriteByte('"')
	fmt.Fprintln(log.fh, sb.String())
	log.cnt++
	if parent._many {
		log.mu.Unlock()
	}
}

func (scr *scrBp) toSB(sb *strings.Builder, total int) {
	sb.WriteString(scr.Bck.Cname(""))
	sb.WriteString(": scrubbed ")
	sb.WriteString(cos.FormatBigNum(total))
	sb.WriteString(" names")

	var scr0 scrBp
	if scr.Stats == scr0.Stats {
		return
	}

	sb.WriteByte(' ')
	s := fmt.Sprintf("%+v", scr.Stats)
	sb.WriteString(s)
}
