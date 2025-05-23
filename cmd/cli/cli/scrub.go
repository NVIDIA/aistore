// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
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
// - '--checksum' option (slow)
// - '--fix' option (***)
// - multiple buckets vs one-log-per-scrub-metric - a problem
// - async execution with '--wait' option
// - speed-up `ls` via multiple workers

const (
	logFname           = ".ais-scrub-%s.%x.log"
	logTitleDflt       = "Name,Size"
	logTitleVerChanged = "Name,Size,Custom"
	logTitleMisplaced  = "Name,Size,Atime,Location"
	logTitleCopies     = "Name,Size,Copies"
	logDelim           = `","`

	logMaxLn = 256
)

type (
	logcb func(scr *scrBp, en *cmn.LsoEnt)
	_log  struct {
		fh    *os.File
		do    logcb
		title string
		tag   string
		fn    string
		cnt   int
		mu    sync.Mutex
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
		// total num listed names
		total atomic.Int64
		// detailed logs
		logs       [teb.ScrNumStats]_log
		progLine   cos.Builder
		numBcks    int
		pid        int
		haveRemote atomic.Bool
	}
)

var (
	scrubFlags = append(
		longRunFlags,
		bsummPrefixFlag,
		pageSizeFlag,
		objLimitFlag,
		noHeaderFlag,
		noFooterFlag,
		maxPagesFlag,
		nonRecursFlag,
		smallSizeFlag,
		largeSizeFlag,
		scrubObjCachedFlag,
		allColumnsFlag,
	)
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
	ctx.units, err = parseUnitsFlag(ctx.c, unitsFlag)
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

	now := mono.NanoTime()

	// setup progress updates
	ctx.last.Store(now)
	ctx.ival = max(listObjectsWaitTime, refreshRateDefault)
	if flagIsSet(c, refreshFlag) {
		// (compare w/ _refreshRate())
		refreshRate := parseDurationFlag(c, refreshFlag)
		ctx.ival = max(refreshRate, refreshRateDefault)
	}

	// validate small/large
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

	bcks, errN := ctx.lsBcks()
	if errN != nil {
		return V(err)
	}

	ctx.pid = os.Getpid()

	ctx.iniLogs()

	if ctx.numBcks > 1 {
		err = ctx.many(bcks)
	} else {
		err = ctx.one()
	}

	ctx.closeLogs(c)

	// elapsed
	if !flagIsSet(c, noFooterFlag) {
		elapsed := teb.FormatDuration(mono.Since(now))
		fmt.Fprintln(c.App.Writer, "---")
		if ctx.numBcks > 1 {
			total := cos.FormatBigI64(ctx.total.Load())
			fmt.Fprintln(c.App.Writer, "Total:", total, "names in", elapsed)
		} else {
			fmt.Fprintln(c.App.Writer, "Elapsed:", elapsed)
		}
	}

	return err
}

////////////
// scrCtx //
////////////

func (ctx *scrCtx) iniLogs() {
	for i := range ctx.logs {
		// default
		log := &ctx.logs[i]
		log.title = logTitleDflt
		log.do = log.dflt

		// assorted overrides
		switch i {
		case teb.ScrMisplacedNode, teb.ScrMisplacedMpath:
			log.title = logTitleMisplaced
			log.do = log.misplaced
		case teb.ScrVchanged:
			log.title = logTitleVerChanged
			log.do = log.vchanged
		case teb.ScrMissingCp:
			log.title = logTitleCopies
			log.do = log.copies
		}
	}
}

func (ctx *scrCtx) closeLogs(c *cli.Context) {
	var titled bool
	for i := 1; i < len(ctx.logs); i++ { // skipping listed objects
		log := &ctx.logs[i]
		if log.fh == nil {
			continue
		}
		debug.Assert(log.cnt > 0, log.fn)
		cos.Close(log.fh)
		if !titled {
			const title = "Detailed Logs"
			fmt.Fprintln(c.App.Writer)
			fmt.Fprintln(c.App.Writer, fcyan(title))
			fmt.Fprintln(c.App.Writer, strings.Repeat("-", len(title)))
			titled = true
		}
		fmt.Fprintf(c.App.Writer, "* %s objects: \t%s (%d record%s)\n", log.tag, log.fn, log.cnt, cos.Plural(log.cnt))
	}
}

func (ctx *scrCtx) lsBcks() (bcks cmn.Bcks, err error) {
	if ctx.qbck.IsBucket() {
		ctx.numBcks = 1
		return
	}
	bcks, err = api.ListBuckets(apiBP, ctx.qbck, apc.FltPresent)
	if err != nil {
		return
	}
	ctx.numBcks = len(bcks)
	if ctx.numBcks == 1 {
		ctx.qbck = cmn.QueryBcks(bcks[0])
	}
	return
}

func (ctx *scrCtx) many(bcks cmn.Bcks) error {
	var (
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), ctx.numBcks)
		mu = &sync.Mutex{}
	)
	ctx.scrubs = make([]*scrBp, 0, ctx.numBcks)
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
	tab := all.MakeTab(ctx.units, ctx.haveRemote.Load(), flagIsSet(ctx.c, allColumnsFlag))

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
	scr.Cname = bck.Cname("")
	propNames := []string{apc.GetPropsName, apc.GetPropsSize, apc.GetPropsVersion, apc.GetPropsCopies, apc.GetPropsLocation, apc.GetPropsCustom}
	if bck.IsRemote() {
		lsmsg.Flags |= apc.LsDiff
		lsmsg.AddProps(propNames...)
		ctx.haveRemote.Store(true) // columns version-changed etc.
	} else {
		lsmsg.AddProps(propNames[:len(propNames)-1]...) // minus apc.GetPropsCustom
	}

	// note: this flag (always) defines the way we traverse content:
	// - (remote) => (in-cluster) when not specified
	// - (in-cluster) otherwise
	if flagIsSet(ctx.c, scrubObjCachedFlag) {
		lsmsg.SetFlag(apc.LsCached)
	}

	pageSize, maxPages, limit, err := setLsoPage(ctx.c, bck)
	if err != nil {
		return nil, err
	}
	lsmsg.PageSize = pageSize
	lsargs.Limit = limit

	var (
		pgcnt  int
		listed int64
		yes    bool
	)
	// main loop (pages)
	for {
		lst, err := api.ListObjectsPage(apiBP, bck, lsmsg, lsargs)
		if err != nil {
			return nil, err
		}
		ctx.total.Add(int64(len(lst.Entries)))
		// one page
		for _, en := range lst.Entries {
			if en.IsAnyFlagSet(apc.EntryIsDir) || cos.IsLastB(en.Name, filepath.Separator) {
				continue
			}
			scr.upd(ctx, en)
		}
		if lsmsg.ContinuationToken == "" {
			break
		}
		pgcnt++
		if maxPages > 0 && pgcnt >= int(maxPages) {
			break
		}
		listed += int64(len(lst.Entries))
		if limit > 0 && listed >= limit {
			break
		}

		ctx.progress(scr, listed, &yes)
	}

	if yes {
		fmt.Fprintln(ctx.c.App.Writer)
	}
	return scr, nil
}

func (ctx *scrCtx) progress(scr *scrBp, listed int64, yes *bool) {
	var (
		now  = mono.NanoTime()
		last = ctx.last.Load()
	)
	if time.Duration(now-last) < ctx.ival {
		return
	}
	if !ctx.last.CAS(last, now) {
		return
	}

	sb := &ctx.progLine
	sb.Reset(160)

	sb.WriteString(scr.Cname)
	if scr.Prefix != "" {
		sb.WriteByte(filepath.Separator)
		sb.WriteString(scr.Prefix)
	}
	sb.WriteString(": scrubbed ")
	sb.WriteString(cos.FormatBigI64(listed))
	sb.WriteString(" names")

	var found bool
	for i := 1; i < len(scr.Stats); i++ { // skipping listed objects (same as elsewhere)
		if cnt := scr.Stats[i].Cnt; cnt != 0 {
			if !found {
				sb.WriteByte(' ')
				sb.WriteByte('{')
				found = true
			} else {
				sb.WriteByte(' ')
			}
			sb.WriteString(strings.ToLower(teb.ScrCols[i]))
			sb.WriteByte(':')
			sb.WriteString(strconv.FormatInt(cnt, 10))
		}
	}
	if found {
		sb.WriteByte('}')
	}

	for range min(sb.Cap()-sb.Len(), 8) {
		sb.WriteByte(' ')
	}

	fmt.Fprintf(ctx.c.App.Writer, "\r%s", sb.String())
	*yes = true
}

///////////
// scrBp //
///////////

func (scr *scrBp) upd(parent *scrCtx, en *cmn.LsoEnt) {
	scr.Stats[teb.ScrObjects].Cnt++
	scr.Stats[teb.ScrObjects].Siz += en.Size

	if !en.IsPresent() {
		scr.Stats[teb.ScrNotIn].Cnt++
		scr.Stats[teb.ScrNotIn].Siz += en.Size
		scr.log(parent, en, teb.ScrNotIn)
		// no further checking
		return
	}
	if en.Status() == apc.LocMisplacedNode {
		scr.Stats[teb.ScrMisplacedNode].Cnt++
		scr.Stats[teb.ScrMisplacedNode].Siz += en.Size
		scr.log(parent, en, teb.ScrMisplacedNode)
		// no further checking
		return
	}

	// or-ing rest conditions (x num-copies)
	if en.Status() == apc.LocMisplacedMountpath {
		scr.Stats[teb.ScrMisplacedMpath].Cnt++
		scr.Stats[teb.ScrMisplacedMpath].Siz += en.Size
		scr.log(parent, en, teb.ScrMisplacedMpath)
	}

	if scr.Bck.Props.Mirror.Enabled && en.Copies < int16(scr.Bck.Props.Mirror.Copies) {
		scr.Stats[teb.ScrMissingCp].Cnt++
		scr.log(parent, en, teb.ScrMissingCp)
	}

	if en.Size <= parent.small {
		scr.Stats[teb.ScrSmallSz].Cnt++
		scr.Stats[teb.ScrSmallSz].Siz += en.Size
		scr.log(parent, en, teb.ScrSmallSz)
	} else if en.Size >= parent.large {
		scr.Stats[teb.ScrLargeSz].Cnt++
		scr.Stats[teb.ScrLargeSz].Siz += en.Size
		scr.log(parent, en, teb.ScrLargeSz)
	}

	if en.IsAnyFlagSet(apc.EntryVerChanged) {
		scr.Stats[teb.ScrVchanged].Cnt++
		scr.Stats[teb.ScrVchanged].Siz += en.Size
		scr.log(parent, en, teb.ScrVchanged)
	} else if en.IsAnyFlagSet(apc.EntryVerRemoved) {
		scr.Stats[teb.ScrVremoved].Cnt++
		scr.Stats[teb.ScrVremoved].Siz += en.Size
		scr.log(parent, en, teb.ScrVremoved)
	}
}

// NOTE: exit upon (unlikely) failure
func (*scrBp) _create(log *_log, pid int) {
	fn := fmt.Sprintf(logFname, log.tag, pid)
	log.fn = filepath.Join(os.TempDir(), fn)
	fh, err := cos.CreateFile(log.fn)
	if err != nil {
		exitln("failed to create scrub log:", err)
	}
	log.fh = fh
}

func (scr *scrBp) log(parent *scrCtx, en *cmn.LsoEnt, i int) {
	log := &parent.logs[i]
	if parent.numBcks > 1 {
		log.mu.Lock()
	}
	if log.fh == nil {
		log.tag = strings.ToLower(teb.ScrCols[i])
		scr._create(log, parent.pid)
		fmt.Fprintln(log.fh, log.title)
		fmt.Fprintln(log.fh, strings.Repeat("=", len(log.title)))
	}

	log.do(scr, en)

	if parent.numBcks > 1 {
		log.mu.Unlock()
	}
}

func (scr *scrBp) cname(objname string) {
	sb := &scr.Line
	sb.WriteString(scr.Cname)
	sb.WriteByte(filepath.Separator)
	sb.WriteString(objname)
}

//////////
// _log //
//////////

// logTitleDflt = "Name,Size"
func (log *_log) dflt(scr *scrBp, en *cmn.LsoEnt) {
	sb := &scr.Line
	sb.Reset(logMaxLn)
	sb.WriteByte('"')

	scr.cname(en.Name)

	sb.WriteString(logDelim)
	sb.WriteString(strconv.FormatInt(en.Size, 10))
	sb.WriteByte('"')
	fmt.Fprintln(log.fh, sb.String())
	log.cnt++
}

// logTitleVerChanged = "Name,Size,Custom"
func (log *_log) vchanged(scr *scrBp, en *cmn.LsoEnt) {
	sb := &scr.Line
	sb.Reset(logMaxLn)
	sb.WriteByte('"')

	scr.cname(en.Name)

	sb.WriteString(logDelim)
	sb.WriteString(strconv.FormatInt(en.Size, 10))
	sb.WriteString(logDelim)
	sb.WriteString(en.Custom)
	sb.WriteByte('"')
	fmt.Fprintln(log.fh, sb.String())
	log.cnt++
}

// logTitleMisplaced  = "Name,Size,Atime,Location"
func (log *_log) misplaced(scr *scrBp, en *cmn.LsoEnt) {
	sb := &scr.Line
	sb.Reset(logMaxLn)
	sb.WriteByte('"')

	scr.cname(en.Name)

	sb.WriteString(logDelim)
	sb.WriteString(strconv.FormatInt(en.Size, 10))
	sb.WriteString(logDelim)
	sb.WriteString(en.Atime)
	sb.WriteString(logDelim)
	sb.WriteString(en.Location)
	sb.WriteByte('"')
	fmt.Fprintln(log.fh, sb.String())
	log.cnt++
}

// logTitleCopies = "Name,Size,Copies"
func (log *_log) copies(scr *scrBp, en *cmn.LsoEnt) {
	sb := &scr.Line
	sb.Reset(logMaxLn)
	sb.WriteByte('"')

	scr.cname(en.Name)

	sb.WriteString(logDelim)
	sb.WriteString(strconv.FormatInt(en.Size, 10))
	sb.WriteString(logDelim)
	sb.WriteString(strconv.Itoa(int(en.Copies)))
	sb.WriteByte('"')
	fmt.Fprintln(log.fh, sb.String())
	log.cnt++
}
