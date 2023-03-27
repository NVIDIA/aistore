// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

type (
	uparams struct {
		bck       cmn.Bck
		fromTag   string
		files     []fobj
		workerCnt int
		refresh   time.Duration
		cksum     *cos.Cksum
		totalSize int64
	}
	uctx struct {
		wg            cos.WG
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

func putFobjs(c *cli.Context, files []fobj, bck cmn.Bck, fromTag string) error {
	if len(files) == 0 {
		return fmt.Errorf("no files to PUT (hint: check filename pattern and/or source directory name)")
	}

	// calculate total size, group by extension
	totalSize, extSizes := groupByExt(files)
	totalCount := int64(len(files))

	if flagIsSet(c, dryRunFlag) {
		i := 0
		for ; i < dryRunExamplesCnt; i++ {
			fmt.Fprintf(c.App.Writer, "PUT %q => %q\n", files[i].path, bck.Cname(files[i].name))
		}
		if i < len(files) {
			fmt.Fprintf(c.App.Writer, "(and %d more)\n", len(files)-i)
		}
		return nil
	}

	var (
		units, errU = parseUnitsFlag(c, unitsFlag)
		tmpl        = teb.MultiPutTmpl + strconv.FormatInt(totalCount, 10) + "\t " + cos.ToSizeIEC(totalSize, 2) + "\n"
		opts        = teb.Opts{AltMap: teb.FuncMapUnits(units)}
	)
	if errU != nil {
		return errU
	}
	if err := teb.Print(extSizes, tmpl, opts); err != nil {
		return err
	}

	cksum, err := cksumToCompute(c, bck)
	if err != nil {
		return err
	}

	// ask a user for confirmation
	if !flagIsSet(c, yesFlag) {
		l := len(files)
		if ok := confirm(c, fmt.Sprintf("PUT %d file%s => %s?", l, cos.Plural(l), bck)); !ok {
			fmt.Fprintln(c.App.Writer, "Operation canceled")
			return nil
		}
	}
	refresh := calcPutRefresh(c)
	numWorkers := parseIntFlag(c, concurrencyFlag)
	params := &uparams{
		bck:       bck,
		fromTag:   fromTag,
		files:     files,
		workerCnt: numWorkers,
		refresh:   refresh,
		cksum:     cksum,
		totalSize: totalSize,
	}
	return _putFobjs(c, params)
}

// PUT fobj-s in parallel
func _putFobjs(c *cli.Context, p *uparams) error {
	u := &uctx{
		verbose:      flagIsSet(c, verboseFlag),
		showProgress: flagIsSet(c, progressFlag),
		wg:           cos.NewLimitedWaitGroup(p.workerCnt, 0),
		lastReport:   time.Now(),
		reportEvery:  p.refresh,
	}
	if u.showProgress {
		var (
			filesBarArg = barArgs{ // bar[0]
				total:   int64(len(p.files)),
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

	for _, f := range p.files {
		u.wg.Add(1)
		go u.put(c, p, f)
	}
	u.wg.Wait()

	if u.showProgress {
		u.progress.Wait()
		fmt.Fprint(c.App.Writer, u.errSb.String())
	}
	if numFailed := u.errCount.Load(); numFailed > 0 {
		return fmt.Errorf("failed to PUT %d object%s", numFailed, cos.Plural(int(numFailed)))
	}
	msg := fmt.Sprintf("PUT %d object%s%s to %q\n", len(p.files), cos.Plural(len(p.files)), p.fromTag, p.bck.Cname(""))
	actionDone(c, msg)
	return nil
}

//////////
// uctx //
//////////

func (u *uctx) put(c *cli.Context, p *uparams, f fobj) {
	defer u.fini(c, p, f)

	fh, err := cos.NewFileHandle(f.path)
	if err != nil {
		str := fmt.Sprintf("Failed to open %q: %v\n", f.path, err)
		if u.showProgress {
			u.errSb.WriteString(str)
		} else {
			fmt.Fprint(c.App.Writer, str)
		}
		u.errCount.Inc()
		return
	}

	// setup progress bar(s)
	var (
		bar       *mpb.Bar
		updateBar = func(int, error) {} // no-op unless (below)
	)
	if u.showProgress {
		if u.verbose {
			bar = u.progress.AddBar(
				f.size,
				mpb.BarRemoveOnComplete(),
				mpb.PrependDecorators(
					decor.Name(f.name+" ", decor.WC{W: len(f.name) + 1, C: decor.DSyncWidthR}),
					decor.Counters(decor.UnitKiB, "%.1f/%.1f", decor.WCSyncWidth),
				),
				mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
			)
			updateBar = func(n int, _ error) {
				u.barSize.IncrBy(n)
				bar.IncrBy(n)
			}
		} else {
			updateBar = func(n int, _ error) {
				u.barSize.IncrBy(n)
			}
		}
	}

	var (
		countReader = cos.NewCallbackReadOpenCloser(fh, updateBar /*progress callback*/)
		putArgs     = api.PutArgs{
			BaseParams: apiBP,
			Bck:        p.bck,
			ObjName:    f.name,
			Reader:     countReader,
			Cksum:      p.cksum,
			SkipVC:     flagIsSet(c, skipVerCksumFlag),
		}
	)
	if _, err := api.PutObject(putArgs); err != nil {
		str := fmt.Sprintf("Failed to PUT %s: %v\n", p.bck.Cname(f.name), err)
		if u.showProgress {
			u.errSb.WriteString(str)
		} else {
			fmt.Fprint(c.App.Writer, str)
		}
		u.errCount.Inc()
	} else if u.verbose && !u.showProgress {
		fmt.Fprintf(c.App.Writer, "%s -> %s\n", f.path, f.name)
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
			total, 100*total/len(p.files), cos.ToSizeIEC(size, 1), 100*size/p.totalSize,
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
	_, err = api.PutObject(putArgs)
	if progress != nil {
		progress.Wait()
	}
	return err
}

func appendToArch(c *cli.Context, bck cmn.Bck, objName, path, archPath string, finfo os.FileInfo) error {
	var (
		reader   cos.ReadOpenCloser
		progress *mpb.Progress
		bars     []*mpb.Bar
		cksum    *cos.Cksum
	)
	fh, err := cos.NewFileHandle(path)
	if err != nil {
		return err
	}
	reader = fh
	if flagIsSet(c, progressFlag) {
		fi, err := fh.Stat()
		if err != nil {
			return err
		}
		// setup progress bar
		args := barArgs{barType: sizeArg, barText: objName, total: fi.Size()}
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
		Size:       uint64(finfo.Size()),
		SkipVC:     flagIsSet(c, skipVerCksumFlag),
	}
	appendArchArgs := api.AppendToArchArgs{
		PutArgs:  putArgs,
		ArchPath: archPath,
	}
	err = api.AppendToArch(appendArchArgs)
	if progress != nil {
		progress.Wait()
	}
	return err
}

// PUT fixed-sized chunks using `api.AppendObject` and `api.FlushObject`
func putAppendChunks(c *cli.Context, bck cmn.Bck, objName string, r io.Reader, cksumType string, chunkSize int64) error {
	var (
		handle string
		cksum  = cos.NewCksumHash(cksumType)
		pi     = newProgIndicator(objName)
	)
	if flagIsSet(c, progressFlag) {
		pi.start()
	}
	for {
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
		handle, err = api.AppendObject(api.AppendArgs{
			BaseParams: apiBP,
			Bck:        bck,
			Object:     objName,
			Handle:     handle,
			Reader:     reader,
			Size:       n,
		})
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
	return api.FlushObject(api.FlushArgs{
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
		return nil, fmt.Errorf("at most one checksum flag can be set (multi-checksum is not supported yet)")
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
