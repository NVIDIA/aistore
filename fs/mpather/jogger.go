// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"golang.org/x/sync/errgroup"
)

const (
	throttleNumObjects = 16 // unit of self-throttling
)

type LoadType int

const (
	noLoad LoadType = iota
	LoadUnsafe
	Load
)

const (
	ThrottleMinDur = time.Millisecond
	ThrottleAvgDur = time.Millisecond * 10
	ThrottleMaxDur = time.Millisecond * 100
)

type (
	JgroupOpts struct {
		T                     cluster.Target
		onFinish              func()
		VisitObj              func(lom *cluster.LOM, buf []byte) error
		VisitCT               func(ct *cluster.CT, buf []byte) error
		Slab                  *memsys.Slab
		Bck                   cmn.Bck
		Prefix                string
		CTs                   []string
		DoLoad                LoadType // if specified, lom.Load(lock type)
		Parallel              int      // num parallel calls
		IncludeCopy           bool     // visit copies (aka replicas)
		SkipGloballyMisplaced bool     // skip globally misplaced
		Throttle              bool     // true: pace itself depending on disk utilization
	}

	// Jgroup runs jogger per mountpath which walk the entire bucket and
	// call callback on each of the encountered object. When jogger encounters
	// error it stops and informs other joggers about the error (so they stop too).
	Jgroup struct {
		wg          *errgroup.Group
		joggers     map[string]*jogger
		finishedCh  cos.StopCh // when all joggers are done
		finishedCnt atomic.Uint32
	}

	// jogger is being run on each mountpath and executes fs.Walk which call
	// provided callback.
	jogger struct {
		ctx       context.Context
		syncGroup *joggerSyncGroup
		opts      *JgroupOpts
		mi        *fs.Mountpath
		bdir      string // mi.MakePath(bck)
		objPrefix string // fully-qualified prefix, as in: join(bdir, opts.Prefix)
		config    *cmn.Config
		stopCh    cos.StopCh
		bufs      [][]byte
		num       int64
	}

	joggerSyncGroup struct {
		sema   chan int // Positional number of a buffer to use by a goroutine.
		group  *errgroup.Group
		cancel context.CancelFunc
	}
)

func NewJoggerGroup(opts *JgroupOpts, selectedMpaths ...string) *Jgroup {
	var (
		joggers             map[string]*jogger
		available, disabled = fs.Get()
		wg, ctx             = errgroup.WithContext(context.Background())
		l                   = len(selectedMpaths)
	)
	debug.Assert(!opts.IncludeCopy || (opts.IncludeCopy && opts.DoLoad > noLoad))

	if l == 0 {
		joggers = make(map[string]*jogger, len(available))
	} else {
		joggers = make(map[string]*jogger, l)
	}
	for _, mi := range available {
		if l == 0 {
			joggers[mi.Path] = newJogger(ctx, opts, mi)
			continue
		}
		for _, mpath := range selectedMpaths {
			if mi, ok := available[mpath]; ok {
				joggers[mi.Path] = newJogger(ctx, opts, mi)
			}
		}
	}
	jg := &Jgroup{wg: wg, joggers: joggers}
	jg.finishedCh.Init()
	opts.onFinish = jg.markFinished

	// NOTE: this jogger group is a no-op
	if len(joggers) == 0 {
		nlog.Errorf("%v: avail=%v, disabled=%v, selected=%v",
			cmn.ErrNoMountpaths, available, disabled, selectedMpaths)
		jg.finishedCh.Close()
	}
	return jg
}

func (jg *Jgroup) Num() int { return len(jg.joggers) }

func (jg *Jgroup) Run() {
	for _, jogger := range jg.joggers {
		jg.wg.Go(jogger.run)
	}
}

func (jg *Jgroup) Stop() error {
	for _, jogger := range jg.joggers {
		jogger.abort()
	}
	return jg.wg.Wait()
}

func (jg *Jgroup) ListenFinished() <-chan struct{} {
	return jg.finishedCh.Listen()
}

func (jg *Jgroup) markFinished() {
	if n := jg.finishedCnt.Inc(); n == uint32(len(jg.joggers)) {
		jg.finishedCh.Close()
	}
}

func newJogger(ctx context.Context, opts *JgroupOpts, mi *fs.Mountpath) (j *jogger) {
	var syncGroup *joggerSyncGroup
	if opts.Parallel > 1 {
		var (
			group  *errgroup.Group
			cancel context.CancelFunc
		)
		ctx, cancel = context.WithCancel(ctx)
		group, ctx = errgroup.WithContext(ctx)
		syncGroup = &joggerSyncGroup{
			sema:   make(chan int, opts.Parallel),
			group:  group,
			cancel: cancel,
		}
		for i := 0; i < opts.Parallel; i++ {
			syncGroup.sema <- i
		}
	}
	j = &jogger{
		ctx:       ctx,
		opts:      opts,
		mi:        mi,
		config:    cmn.GCO.Get(),
		syncGroup: syncGroup,
	}
	if opts.Prefix != "" {
		j.bdir = mi.MakePathCT(&j.opts.Bck, fs.ObjectType) // this mountpath's bucket dir that contains objects
		j.objPrefix = filepath.Join(j.bdir, opts.Prefix)
	}
	j.stopCh.Init()
	return
}

func (j *jogger) run() error {
	defer j.opts.onFinish()
	if j.opts.Slab != nil {
		if j.opts.Parallel <= 1 {
			j.bufs = [][]byte{j.opts.Slab.Alloc()}
		} else {
			j.bufs = make([][]byte, j.opts.Parallel)
			for i := 0; i < j.opts.Parallel; i++ {
				j.bufs[i] = j.opts.Slab.Alloc()
			}
		}

		defer func() {
			for _, buf := range j.bufs {
				j.opts.Slab.Free(buf)
			}
		}()
	}

	var (
		aborted bool
		err     error
	)
	// walk all buckets, one at a time
	if j.opts.Bck.IsEmpty() {
		bmd := j.opts.T.Bowner().Get()
		bmd.Range(nil, nil, func(bck *meta.Bck) bool {
			aborted, err = j.runBck(bck.Bucket())
			return err != nil || aborted
		})
		return err
	}
	// walk the specified bucket
	_, err = j.runBck(&j.opts.Bck)
	return err
}

func (j *jogger) runBck(bck *cmn.Bck) (aborted bool, err error) {
	opts := &fs.WalkOpts{
		Mi:       j.mi,
		CTs:      j.opts.CTs,
		Callback: j.jog,
		Sorted:   false,
	}
	opts.Bck.Copy(bck)

	err = fs.Walk(opts)
	if j.syncGroup != nil {
		// If callbacks are executed in goroutines, fs.Walk can stop before the callbacks return.
		// We have to wait for them and check if there was any error.
		if err == nil {
			err = j.syncGroup.waitForAsyncTasks()
		} else {
			j.syncGroup.abortAsyncTasks()
		}
	}

	if err != nil {
		if cmn.IsErrAborted(err) {
			nlog.Infof("%s stopping traversal: %v", j, err)
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (j *jogger) jog(fqn string, de fs.DirEntry) error {
	if j.objPrefix != "" && strings.HasPrefix(fqn, j.bdir) {
		if de.IsDir() {
			if !cmn.DirHasOrIsPrefix(fqn, j.objPrefix) {
				return filepath.SkipDir
			}
		} else if !strings.HasPrefix(fqn, j.objPrefix) {
			return nil
		}
	}
	if de.IsDir() {
		return nil
	}

	if err := j.checkStopped(); err != nil {
		return err
	}

	var bufPosition int
	if j.syncGroup == nil {
		if err := j.visitFQN(fqn, j.getBuf(0)); err != nil {
			return err
		}
	} else {
		select {
		case bufPosition = <-j.syncGroup.sema:
			break
		case <-j.ctx.Done():
			return j.ctx.Err()
		}

		j.syncGroup.group.Go(func() error {
			defer func() {
				// NOTE: There is no need to select j.ctx.Done() as put to this chanel is immediate.
				j.syncGroup.sema <- bufPosition
			}()
			return j.visitFQN(fqn, j.getBuf(bufPosition))
		})
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

func (j *jogger) visitFQN(fqn string, buf []byte) error {
	ct, err := cluster.NewCTFromFQN(fqn, j.opts.T.Bowner())
	if err != nil {
		return err
	}

	if j.opts.SkipGloballyMisplaced {
		smap := j.opts.T.Sowner().Get()
		tsi, err := smap.HrwHash2T(ct.Digest(), true /*skip maint*/)
		if err != nil {
			return err
		}
		if tsi.ID() != j.opts.T.SID() {
			return nil
		}
	}

	switch ct.ContentType() {
	case fs.ObjectType:
		lom := cluster.AllocLOM("")
		lom.InitCT(ct)
		err := j.visitObj(lom, buf)
		// NOTE: j.visitObj() callback impl-s must either finish the entire
		//       operation synchronously OR pass lom.LIF to other gorouine(s)
		cluster.FreeLOM(lom)
		return err
	default:
		if err := j.visitCT(ct, buf); err != nil {
			return err
		}
	}
	return nil
}

func (j *jogger) visitObj(lom *cluster.LOM, buf []byte) (err error) {
	switch j.opts.DoLoad {
	case noLoad:
		goto visit
	case LoadUnsafe:
		err = lom.LoadUnsafe()
	case Load:
		err = lom.Load(false, false)
	default:
		debug.Assert(false, "invalid 'opts.DoLoad'", j.opts.DoLoad)
	}
	if err != nil {
		return
	}
	if !j.opts.IncludeCopy && lom.IsCopy() {
		return nil
	}
visit:
	return j.opts.VisitObj(lom, buf)
}

func (j *jogger) visitCT(ct *cluster.CT, buf []byte) error { return j.opts.VisitCT(ct, buf) }

func (j *jogger) getBuf(position int) []byte {
	if j.bufs == nil {
		return nil
	}
	return j.bufs[position]
}

func (j *jogger) checkStopped() error {
	select {
	case <-j.ctx.Done(): // Some other worker has exited with error and canceled context.
		return j.ctx.Err()
	case <-j.stopCh.Listen(): // Worker has been aborted.
		return cmn.NewErrAborted(j.String(), "mpath-jog", nil)
	default:
		return nil
	}
}

func (sg *joggerSyncGroup) waitForAsyncTasks() error {
	return sg.group.Wait()
}

func (sg *joggerSyncGroup) abortAsyncTasks() error {
	sg.cancel()
	return sg.waitForAsyncTasks()
}

func (j *jogger) throttle() {
	curUtil := fs.GetMpathUtil(j.mi.Path)
	if curUtil >= j.config.Disk.DiskUtilHighWM {
		time.Sleep(ThrottleMinDur)
	}
}

func (j *jogger) abort()         { j.stopCh.Close() }
func (j *jogger) String() string { return fmt.Sprintf("jogger [%s/%s]", j.mi, j.opts.Bck) }
