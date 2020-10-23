// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"golang.org/x/sync/errgroup"
)

const (
	throttleNumObjects = 16 // unit of self-throttling
)

const (
	noLoad LoadType = iota
	Load
	LoadRLock
	LoadLock
)

type (
	LoadType int

	JoggerGroupOpts struct {
		T        cluster.Target
		Bck      cmn.Bck
		CTs      []string
		VisitObj func(lom *cluster.LOM, buf []byte) error
		VisitCT  func(ct *cluster.CT, buf []byte) error
		Slab     *memsys.Slab

		DoLoad                LoadType // Loads the LOM and if specified takes requested lock.
		IncludeCopy           bool     // Traverses LOMs that are copies.
		SkipGloballyMisplaced bool     // Skips content types that are globally misplaced.
		Throttle              bool     // Determines if the jogger should throttle itself.

		// Additional function which should be set by JoggerGroup and called
		// by each of the jogger if they finish.
		onFinish func()
	}

	// JoggerGroup runs jogger per mountpath which walk the entire bucket and
	// call callback on each of the encountered object. When jogger encounters
	// error it stops and informs other joggers about the error (so they stop too).
	JoggerGroup struct {
		wg      *errgroup.Group
		joggers map[string]*jogger

		finishedCnt atomic.Uint32
		finishedCh  *cmn.StopCh // Informs when all joggers have finished.
	}

	// jogger is being run on each mountpath and executes fs.Walk which call
	// provided callback.
	jogger struct {
		buf []byte

		ctx       context.Context
		opts      *JoggerGroupOpts
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		stopCh    *cmn.StopCh

		num int64
	}
)

func NewJoggerGroup(opts *JoggerGroupOpts) *JoggerGroup {
	cmn.Assert(!opts.IncludeCopy || (opts.IncludeCopy && opts.DoLoad > noLoad))

	var (
		mpaths, _ = fs.Get()
		wg, ctx   = errgroup.WithContext(context.Background())
		joggers   = make(map[string]*jogger, len(mpaths))
	)

	for _, mpathInfo := range mpaths {
		joggers[mpathInfo.Path] = newJogger(ctx, opts, mpathInfo)
	}

	jg := &JoggerGroup{
		wg:         wg,
		joggers:    joggers,
		finishedCh: cmn.NewStopCh(),
	}
	opts.onFinish = jg.markFinished
	return jg
}

func (jg *JoggerGroup) Run() {
	for _, jogger := range jg.joggers {
		jg.wg.Go(jogger.run)
	}
}

func (jg *JoggerGroup) Stop() error {
	for _, jogger := range jg.joggers {
		jogger.abort()
	}
	return jg.wg.Wait()
}

func (jg *JoggerGroup) ListenFinished() <-chan struct{} {
	return jg.finishedCh.Listen()
}

func (jg *JoggerGroup) markFinished() {
	if n := jg.finishedCnt.Inc(); n == uint32(len(jg.joggers)) {
		jg.finishedCh.Close()
	}
}

func newJogger(ctx context.Context, opts *JoggerGroupOpts, mpathInfo *fs.MountpathInfo) *jogger {
	return &jogger{
		ctx:       ctx,
		opts:      opts,
		mpathInfo: mpathInfo,
		config:    cmn.GCO.Get(),
		stopCh:    cmn.NewStopCh(),
	}
}

func (j *jogger) run() error {
	defer j.opts.onFinish()

	glog.Infof("%s started", j)

	if j.opts.Slab != nil {
		j.buf = j.opts.Slab.Alloc()
		defer j.opts.Slab.Free(j.buf)
	}

	var (
		aborted bool
		err     error
	)

	// In case the bucket is not specified, walk in bucket-by-bucket fashion.
	if j.opts.Bck.IsEmpty() {
		j.opts.T.Bowner().Get().Range(nil, nil, func(bck *cluster.Bck) bool {
			aborted, err = j.runBck(bck.Bck)
			return err != nil || aborted
		})
		return err
	}
	_, err = j.runBck(j.opts.Bck)
	return err
}

func (j *jogger) runBck(bck cmn.Bck) (aborted bool, err error) {
	opts := &fs.Options{
		Mpath:    j.mpathInfo,
		Bck:      bck,
		CTs:      j.opts.CTs,
		Callback: j.jog,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		if errors.As(err, &cmn.AbortedError{}) {
			glog.Infof("%s stopping traversal: %v", j, err)
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (j *jogger) jog(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}

	if err := j.checkStopped(); err != nil {
		return err
	}

	ct, err := cluster.NewCTFromFQN(fqn, j.opts.T.Bowner())
	if err != nil {
		return err
	}

	if j.opts.SkipGloballyMisplaced {
		uname := ct.Bck().MakeUname(ct.ObjName())
		tsi, err := cluster.HrwTarget(uname, j.opts.T.Sowner().Get()) // TODO: should we get smap once?
		if err != nil {
			return err
		}
		if tsi.ID() != j.opts.T.Snode().ID() {
			return nil
		}
	}

	switch ct.ContentType() {
	case fs.ObjectType:
		lom := &cluster.LOM{T: j.opts.T, FQN: fqn}
		if err := lom.Init(j.opts.Bck, j.config); err != nil {
			return err
		}
		if err := j.visitObj(lom); err != nil {
			return err
		}
	default:
		if err := j.visitCT(ct); err != nil {
			return err
		}
	}

	if j.opts.Throttle {
		j.num++
		if (j.num % throttleNumObjects) == 0 {
			j.throttle()
		} else {
			runtime.Gosched()
		}
	}
	return nil
}

func (j *jogger) visitObj(lom *cluster.LOM) error {
	if j.opts.DoLoad > noLoad {
		if j.opts.DoLoad == LoadRLock {
			lom.Lock(false)
			defer lom.Unlock(false)
		} else if j.opts.DoLoad == LoadLock {
			lom.Lock(true)
			defer lom.Unlock(true)
		}
		if err := lom.Load(); err != nil {
			return err
		}
		if !j.opts.IncludeCopy && lom.IsCopy() {
			return nil
		}
	}
	return j.opts.VisitObj(lom, j.buf)
}

func (j *jogger) visitCT(ct *cluster.CT) error { return j.opts.VisitCT(ct, j.buf) }

func (j *jogger) checkStopped() error {
	select {
	case <-j.ctx.Done(): // Some other worker has exited with error and canceled context.
		return nil
	case <-j.stopCh.Listen(): // Worker has been aborted.
		return cmn.NewAbortedError(j.String())
	default:
		return nil
	}
}

func (j *jogger) throttle() {
	curUtil := fs.GetMpathUtil(j.mpathInfo.Path)
	if curUtil >= j.config.Disk.DiskUtilHighWM {
		time.Sleep(cmn.ThrottleMin)
	}
}

func (j *jogger) abort()         { j.stopCh.Close() }
func (j *jogger) String() string { return fmt.Sprintf("jogger [%s/%s]", j.mpathInfo, j.opts.Bck) }
