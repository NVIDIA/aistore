// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

type (
	uparams struct {
		wop        wop
		bck        cmn.Bck
		fobjs      []fobj
		numWorkers int
		refresh    time.Duration
		cksum      *cos.Cksum
		cptn       string
		totalSize  int64
		dryRun     bool
	}
	uctx struct {
		wg            cos.WG
		errCh         chan string
		errCount      atomic.Int32 // uploads failed so far
		processedCnt  atomic.Int32 // files processed so far
		processedSize atomic.Int64 // size of already processed files
		barObjs       *mpb.Bar
		barSize       *mpb.Bar
		progress      *mpb.Progress
		errSb         strings.Builder
		lastReport    time.Time
		reportEvery   time.Duration
		mx            sync.Mutex
		verbose       bool
		showProgress  bool
	}
)

func verbFobjs(c *cli.Context, wop wop, fobjs []fobj, bck cmn.Bck, ndir int, recurs bool) error {
	l := len(fobjs)
	if l == 0 {
		return fmt.Errorf("no files to %s (check source name and formatting, see examples)", wop.verb())
	}

	var (
		cptn                string
		totalSize, extSizes = groupByExt(fobjs)
		units, errU         = parseUnitsFlag(c, unitsFlag)
		tmpl                = teb.MultiPutTmpl + strconv.Itoa(l) + "\t " + cos.ToSizeIEC(totalSize, 2) + "\n"
		opts                = teb.Opts{AltMap: teb.FuncMapUnits(units, false /*incl. calendar date*/)}
	)
	if errU != nil {
		return errU
	}
	if totalSize == 0 {
		return fmt.Errorf("total size of all files is zero (%s, %v)", wop.verb(), fobjs)
	}

	if err := teb.Print(extSizes, tmpl, opts); err != nil {
		return err
	}

	cksum, err := cksumToCompute(c, bck)
	if err != nil {
		return err
	}

	cptn = fmt.Sprintf("\n%s %d file%s", wop.verb(), l, cos.Plural(l))
	cptn += ndir2tag(ndir, recurs)
	cptn += " => " + wop.dest()

	// confirm
	if flagIsSet(c, dryRunFlag) {
		actionCptn(c, dryRunHeader(), cptn)
	} else if !flagIsSet(c, yesFlag) {
		if ok := confirm(c, cptn+"?"); !ok {
			fmt.Fprintln(c.App.Writer, "Operation canceled")
			return nil
		}
	}
	refresh := calcPutRefresh(c)
	numWorkers, err := parseNumWorkersFlag(c, numPutWorkersFlag)
	if err != nil {
		return err
	}
	uparams := &uparams{
		wop:        wop,
		bck:        bck,
		fobjs:      fobjs,
		numWorkers: numWorkers,
		refresh:    refresh,
		cksum:      cksum,
		cptn:       cptn,
		totalSize:  totalSize,
		dryRun:     flagIsSet(c, dryRunFlag),
	}
	return uparams.do(c)
}

func ndir2tag(ndir int, recurs bool) (tag string) {
	if ndir == 0 {
		return
	}
	if ndir == 1 {
		tag = " (one directory"
	} else {
		tag = fmt.Sprintf(" (%d directories", ndir)
	}
	if recurs {
		tag += ", recursively)"
	} else {
		tag += ", non-recursive)"
	}
	return
}

/////////////
// uparams //
/////////////

// PUT/APPEND fobj-s in parallel
func (p *uparams) do(c *cli.Context) error {
	u := &uctx{
		verbose:      flagIsSet(c, verboseFlag),
		showProgress: flagIsSet(c, progressFlag),
		wg:           cos.NewLimitedWaitGroup(p.numWorkers, 0),
		lastReport:   time.Now(),
		reportEvery:  p.refresh,
	}
	if u.showProgress {
		var (
			filesBarArg = barArgs{ // bar[0]
				total:   int64(len(p.fobjs)),
				barText: "Uploaded files:",
				barType: unitsArg,
			}
			sizeBarArg = barArgs{ // bar[1]
				total:   p.totalSize,
				barText: "Total size:    ",
				barType: sizeArg,
			}
			totalBars []*mpb.Bar
		)
		u.progress, totalBars = simpleBar(filesBarArg, sizeBarArg)
		u.barObjs = totalBars[0]
		u.barSize = totalBars[1]
	}

	_ = parseRetriesFlag(c, putRetriesFlag, true) // to warn once, if need be

	u.errCh = make(chan string, len(p.fobjs))
	for _, fobj := range p.fobjs {
		u.wg.Add(1) // cos.NewLimitedWaitGroup
		go u.run(c, p, fobj)
	}
	u.wg.Wait()

	close(u.errCh)

	if u.showProgress {
		u.progress.Wait()
		fmt.Fprint(c.App.Writer, u.errSb.String())
	}
	if numFailed := u.errCount.Load(); numFailed > 0 {
		fn := fmt.Sprintf(".ais-%s-failures.%d.log", strings.ToLower(p.wop.verb()), os.Getpid())
		fn = filepath.Join(os.TempDir(), fn)
		fh, err := cos.CreateFile(fn)
		if err == nil {
			for failedPath := range u.errCh {
				fmt.Fprintln(fh, failedPath)
			}
			fh.Close()
		}
		return fmt.Errorf("failed to %s %d file%s (%q)", p.wop.verb(), numFailed, cos.Plural(int(numFailed)), fn)
	}
	if !flagIsSet(c, dryRunFlag) {
		if !flagIsSet(c, yesFlag) {
			actionDone(c, "Done") // confirmed above (2nd time redundant)
		} else {
			actionDone(c, p.cptn)
		}
	}
	return nil
}

func (p *uparams) _putOne(c *cli.Context, fobj fobj, reader cos.ReadOpenCloser, skipVC, isTout bool) (err error) {
	if p.dryRun {
		fmt.Fprintf(c.App.Writer, "%s %s -> %s\n", p.wop.verb(), fobj.path, p.bck.Cname(fobj.dstName))
		return
	}
	putArgs := api.PutArgs{
		BaseParams: apiBP,
		Bck:        p.bck,
		ObjName:    fobj.dstName,
		Reader:     reader,
		Cksum:      p.cksum,
		Size:       uint64(fobj.size),
		SkipVC:     skipVC,
	}
	if isTout {
		putArgs.BaseParams.Client.Timeout = longClientTimeout
	}
	_, err = api.PutObject(&putArgs)
	return
}

func (p *uparams) _a2aOne(c *cli.Context, fobj fobj, reader cos.ReadOpenCloser, skipVC bool) error {
	a, ok := p.wop.(*archput)
	debug.Assert(ok)

	archpath := fobj.dstName // an actual individual `archpath`
	if p.dryRun {
		if fobj.path == archpath {
			fmt.Fprintf(c.App.Writer, "%s %s to %s\n", p.wop.verb(), fobj.path, a.dst.oname)
		} else {
			fmt.Fprintf(c.App.Writer, "%s %s to %s as %s\n", p.wop.verb(), fobj.path, a.dst.oname, archpath)
		}
		return nil
	}

	putArgs := api.PutArgs{
		BaseParams: apiBP,
		Bck:        p.bck,
		ObjName:    a.dst.oname, // SHARD_NAME (compare w/ append-prefix for put)
		Reader:     reader,
		Cksum:      nil,
		Size:       uint64(fobj.size),
		SkipVC:     skipVC,
	}

	putApndArchArgs := api.PutApndArchArgs{
		PutArgs:  putArgs,
		ArchPath: archpath,
	}
	if a.appendOnly {
		putApndArchArgs.Flags = apc.ArchAppend
	}
	if a.appendOrPut {
		debug.Assert(!a.appendOnly)
		putApndArchArgs.Flags = apc.ArchAppendIfExist
	}
	return api.PutApndArch(&putApndArchArgs)
}

//////////
// uctx //
//////////

func (u *uctx) run(c *cli.Context, p *uparams, fobj fobj) {
	fh, bar, err := u.init(c, fobj)
	if err == nil {
		updateBar := func(n int, _ error) {
			if !u.showProgress {
				return
			}
			u.barSize.IncrBy(n)
			if bar != nil {
				bar.IncrBy(n)
			}
		}
		u.do(c, p, fobj, fh, updateBar)
	}
	u.fini(c, p, fobj)
}

func (u *uctx) init(c *cli.Context, fobj fobj) (fh *cos.FileHandle, bar *mpb.Bar, err error) {
	fh, err = cos.NewFileHandle(fobj.path)
	if err != nil {
		str := fmt.Sprintf("Failed to open %q: %v\n", fobj.path, err)
		if u.showProgress {
			u.errSb.WriteString(str)
		} else {
			fmt.Fprint(c.App.Writer, str)
		}
		u.errCount.Inc()
		return
	}
	if !u.showProgress || !u.verbose {
		return
	}

	// setup "verbose" bar
	options := make([]mpb.BarOption, 0, 6)
	options = append(options,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(fobj.dstName+" ", decor.WC{W: len(fobj.dstName) + 1, C: decor.DSyncWidthR}),
			decor.Counters(decor.UnitKiB, "%.1f/%.1f", decor.WCSyncWidth),
		),
	)
	// NOTE: conditional/hardcoded
	if fobj.size >= 512*cos.KiB {
		options = appendDefaultDecorators(options)
	} else if fobj.size >= 32*cos.KiB {
		options = append(options, mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)))
	}
	bar = u.progress.AddBar(fobj.size, options...)
	return
}

func (u *uctx) do(c *cli.Context, p *uparams, fobj fobj, fh *cos.FileHandle, updateBar func(int, error)) {
	var (
		err         error
		skipVC      = flagIsSet(c, skipVerCksumFlag)
		countReader = cos.NewCallbackReadOpenCloser(fh, updateBar /*progress callback*/)
		iters       = 1
		isTout      bool
	)
	iters += parseRetriesFlag(c, putRetriesFlag, false /*warn*/)

	switch p.wop.verb() {
	case "PUT":
		for i := range iters {
			err = p._putOne(c, fobj, countReader, skipVC, isTout)
			if err == nil {
				if i > 0 {
					fmt.Fprintf(c.App.Writer, "[#%d] %s - done.\n", i+1, fobj.path)
				}
				break
			}
			e := stripErr(err)
			if i < iters-1 {
				s := fmt.Sprintf("[#%d] %s: %v - retrying...", i+1, fobj.path, e)
				fmt.Fprintln(c.App.ErrWriter, s)
				briefPause(1)

				ffh, errO := fh.Open()
				if errO != nil {
					fmt.Fprintf(c.App.ErrWriter, "failed to reopen %s: %v\n", fobj.path, errO)
					break
				}
				countReader = cos.NewCallbackReadOpenCloser(ffh, updateBar /*progress callback*/)
				isTout = isTimeout(e)
			}
		}
	case "APPEND":
		// TODO: retry as well
		err = p._a2aOne(c, fobj, countReader, skipVC)
	default:
		debug.Assert(false, p.wop.verb()) // "ARCHIVE"
		actionWarn(c, fmt.Sprintf("%q "+NIY, p.wop.verb()))
		return
	}
	if err != nil {
		e := stripErr(err)
		str := fmt.Sprintf("Failed to %s %s => %s: %v\n", p.wop.verb(), fobj.path, p.bck.Cname(fobj.dstName), e)
		if u.showProgress {
			u.errSb.WriteString(str)
		} else {
			fmt.Fprint(c.App.ErrWriter, str)
		}
		u.errCount.Inc()
		u.errCh <- fobj.path
	} else if u.verbose && !u.showProgress && !p.dryRun {
		fmt.Fprintf(c.App.Writer, "%s -> %s\n", fobj.path, fobj.dstName) // needed?
	}
}

func (u *uctx) fini(c *cli.Context, p *uparams, f fobj) {
	var (
		total = int(u.processedCnt.Inc())
		size  = u.processedSize.Add(f.size)
	)
	if u.showProgress {
		u.barObjs.Increment()
	}
	u.wg.Done()
	if u.reportEvery == 0 {
		return
	}

	// lock after releasing semaphore, so the next file can start
	// uploading even if we are stuck on mutex for a while
	u.mx.Lock()
	if !u.showProgress && time.Since(u.lastReport) > u.reportEvery {
		fmt.Fprintf(
			c.App.Writer, "Uploaded %d(%d%%) objects, %s (%d%%).\n",
			total, 100*total/len(p.fobjs), cos.ToSizeIEC(size, 1), 100*size/p.totalSize,
		)
		u.lastReport = time.Now()
	}
	u.mx.Unlock()
}

func putRegular(c *cli.Context, bck cmn.Bck, objName, path string, finfo os.FileInfo) error {
	var (
		reader   cos.ReadOpenCloser
		progress *mpb.Progress
		bars     []*mpb.Bar
		cksum    *cos.Cksum
	)
	if flagIsSet(c, dryRunFlag) {
		// resulting message printed upon return
		return nil
	}
	cksum, err := cksumToCompute(c, bck)
	if err != nil {
		return err
	}
	fh, err := cos.NewFileHandle(path)
	if err != nil {
		return err
	}
	reader = fh
	if flagIsSet(c, progressFlag) {
		// setup progress bar
		args := barArgs{barType: sizeArg, barText: objName, total: finfo.Size()}
		progress, bars = simpleBar(args)
		cb := func(n int, _ error) { bars[0].IncrBy(n) }
		reader = cos.NewCallbackReadOpenCloser(fh, cb)
	}

	putArgs := api.PutArgs{
		BaseParams: apiBP,
		Bck:        bck,
		ObjName:    objName,
		Reader:     reader,
		Cksum:      cksum,
		SkipVC:     flagIsSet(c, skipVerCksumFlag),
	}
	iters := 1
	iters += parseRetriesFlag(c, putRetriesFlag, true /*warn*/)

	for i := range iters {
		_, err = api.PutObject(&putArgs)
		if err == nil {
			if i > 0 {
				fmt.Fprintf(c.App.Writer, "[#%d] %s - done.\n", i+1, path)
			}
			break
		}
		e := stripErr(err)
		if i < iters-1 {
			s := fmt.Sprintf("[#%d] %s: %v - retrying...", i+1, path, e)
			fmt.Fprintln(c.App.ErrWriter, s)
			briefPause(1)

			putArgs.Reader, err = fh.Open()
			if isTimeout(e) {
				putArgs.BaseParams.Client.Timeout = longClientTimeout
			}
		}
	}
	if progress != nil {
		progress.Wait()
	}

	return err
}

// PUT and then APPEND fixed-sized chunks using `api.PutObject`, `api.AppendObject` and `api.FlushObject`
// - currently, is only used to PUT from standard input when we do expect to overwrite existing destination object
// - APPEND and flush will only be executed with there's a second chunk
func putAppendChunks(c *cli.Context, bck cmn.Bck, objName string, r io.Reader, cksumType string, chunkSize int64) error {
	var (
		handle string
		cksum  = cos.NewCksumHash(cksumType)
		pi     = newProgIndicator(objName)
	)
	if flagIsSet(c, progressFlag) {
		pi.start()
	}
	for i := 0; ; i++ {
		var (
			b      = bytes.NewBuffer(nil)
			n      int64
			err    error
			reader cos.ReadOpenCloser
		)
		if cksumType != cos.ChecksumNone {
			n, err = io.CopyN(cos.NewWriterMulti(cksum.H, b), r, chunkSize)
		} else {
			n, err = io.CopyN(b, r, chunkSize)
		}
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		reader = cos.NewByteHandle(b.Bytes())
		if flagIsSet(c, progressFlag) {
			actualChunkOffset := atomic.NewInt64(0)
			reader = cos.NewCallbackReadOpenCloser(reader, func(n int, _ error) {
				if n == 0 {
					return
				}
				newChunkOffset := actualChunkOffset.Add(int64(n))
				// `actualChunkOffset` is needed to not count the bytes read more than
				// once upon redirection
				if newChunkOffset > chunkSize {
					// This part of the file was already read, so don't read it again
					pi.printProgress(chunkSize - newChunkOffset + int64(n))
					return
				}
				pi.printProgress(int64(n))
			})
		}
		if i == 0 {
			// overwrite, if exists
			// NOTE: when followed by APPEND (below) will increment resulting ais object's version one extra time
			putArgs := api.PutArgs{
				BaseParams: apiBP,
				Bck:        bck,
				ObjName:    objName,
				Reader:     reader,
				Size:       uint64(n),
			}
			_, err = api.PutObject(&putArgs)
		} else {
			handle, err = api.AppendObject(&api.AppendArgs{
				BaseParams: apiBP,
				Bck:        bck,
				Object:     objName,
				Handle:     handle,
				Reader:     reader,
				Size:       n,
			})
		}
		if err != nil {
			return err
		}
	}

	if flagIsSet(c, progressFlag) {
		pi.stop()
	}
	if cksumType != cos.ChecksumNone {
		cksum.Finalize()
	}
	if handle == "" {
		return nil
	}
	return api.FlushObject(&api.FlushArgs{
		BaseParams: apiBP,
		Bck:        bck,
		Object:     objName,
		Handle:     handle,
		Cksum:      cksum.Clone(),
	})
}

//
// PUT checksum
//

func initPutObjCksumFlags() (flags []cli.Flag) {
	checksums := cos.SupportedChecksums()
	flags = make([]cli.Flag, 0, len(checksums)-1)
	for _, cksum := range checksums {
		if cksum == cos.ChecksumNone {
			continue
		}
		flags = append(flags, cli.StringFlag{
			Name:  cksum,
			Usage: fmt.Sprintf("compute client-side %s checksum\n"+putObjCksumText, cksum),
		})
	}
	return
}

func cksumToCompute(c *cli.Context, bck cmn.Bck) (*cos.Cksum, error) {
	// bucket-configured checksum takes precedence
	if flagIsSet(c, putObjDfltCksumFlag) {
		bckProps, err := headBucket(bck, false /* don't add */)
		if err != nil {
			return nil, err
		}
		return cos.NewCksum(bckProps.Cksum.Type, ""), nil
	}
	// otherwise, one of the supported iff requested
	cksums := altCksumToComp(c)
	if len(cksums) > 1 {
		return nil, errors.New("at most one checksum flag can be set (multi-checksum is not supported yet)")
	}
	if len(cksums) == 0 {
		return nil, nil
	}
	return cksums[0], nil
}

// in addition to computeCksumFlag
// an alternative way to specify expected PUT checksum
func altCksumToComp(c *cli.Context) []*cos.Cksum {
	cksums := []*cos.Cksum{}
	for _, ckflag := range putObjCksumFlags {
		if flagIsSet(c, ckflag) { // this one
			cksums = append(cksums, cos.NewCksum(ckflag.GetName(), parseStrFlag(c, ckflag)))
		}
	}
	return cksums
}
