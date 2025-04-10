// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	"golang.org/x/sync/errgroup"
)

// walk all or selected buckets, one at a time

type LoadType int

const (
	noLoad LoadType = iota
	LoadUnsafe
	Load
)

type (
	JgroupOpts struct {
		onFinish    func()
		VisitObj    func(lom *core.LOM, buf []byte) error
		VisitCT     func(ct *core.CT, buf []byte) error
		Slab        *memsys.Slab
		Bck         cmn.Bck
		Buckets     cmn.Bcks
		Prefix      string
		CTs         []string
		DoLoad      LoadType // if specified, lom.Load(lock type)
		IncludeCopy bool     // visit copies (aka replicas)
		PerBucket   bool     // num joggers = (num mountpaths) x (num buckets)
		Throttle    bool     // true: pace itself depending on disk utilization
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
		opts      *JgroupOpts
		mi        *fs.Mountpath
		bdir      string // mi.MakePath(bck)
		objPrefix string // fully-qualified prefix, as in: join(bdir, opts.Prefix)
		config    *cmn.Config
		stopCh    cos.StopCh
		buf       []byte
		numvis    atomic.Int64 // counter: num visited objects
	}
)

func NewJoggerGroup(opts *JgroupOpts, config *cmn.Config, smi *fs.Mountpath) *Jgroup {
	var (
		joggers map[string]*jogger
		avail   = fs.GetAvail()
		la      = len(avail)
		wg, ctx = errgroup.WithContext(context.Background())
		jg      = &Jgroup{wg: wg}
	)
	debug.Assert(!opts.IncludeCopy || (opts.IncludeCopy && opts.DoLoad > noLoad))

	opts.onFinish = jg.markFinished

	switch {
	case smi != nil: // selected mountpath
		if _, ok := avail[smi.Path]; !ok {
			nlog.Errorln(smi.String(), "is not available, nothing to do")
		} else {
			joggers = make(map[string]*jogger, 1)
			joggers[smi.Path] = newJogger(ctx, opts, smi, config)
		}
	case opts.PerBucket:
		debug.Assert(len(opts.Buckets) > 1)
		joggers = make(map[string]*jogger, la*len(opts.Buckets))
		for _, bck := range opts.Buckets {
			nopts := *opts
			nopts.Buckets = nil
			nopts.Bck = bck
			uname := bck.MakeUname("")
			for _, mi := range avail {
				k := mi.Path + "|" + cos.UnsafeS(uname)
				joggers[k] = newJogger(ctx, &nopts, mi, config)
			}
		}
	default:
		joggers = make(map[string]*jogger, la)
		for _, mi := range avail {
			joggers[mi.Path] = newJogger(ctx, opts, mi, config)
		}
	}

	if len(joggers) == 0 {
		// this jogger group is a no-op (unlikely)
		if smi == nil {
			_, disabled := fs.Get()
			nlog.Errorf("%v: avail=%v, disabled=%v", cmn.ErrNoMountpaths, avail, disabled)
		}
	}

	jg.joggers = joggers
	jg.finishedCh.Init()

	return jg
}

func (jg *Jgroup) NumJ() int { return len(jg.joggers) }

func (jg *Jgroup) NumVisits() (n int64) {
	for _, jogger := range jg.joggers {
		n += jogger.numvis.Load()
	}
	return n
}

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

func newJogger(ctx context.Context, opts *JgroupOpts, mi *fs.Mountpath, config *cmn.Config) (j *jogger) {
	j = &jogger{
		ctx:    ctx,
		opts:   opts,
		mi:     mi,
		config: config,
	}
	if opts.Prefix != "" {
		j.bdir = mi.MakePathCT(&j.opts.Bck, fs.ObjectType) // this mountpath's bucket dir that contains objects
		j.objPrefix = filepath.Join(j.bdir, opts.Prefix)
	}
	j.stopCh.Init()
	return
}

////////////
// jogger //
////////////

func (j *jogger) String() string { return fmt.Sprintf("jogger [%s/%s]", j.mi, j.opts.Bck.String()) }

func (j *jogger) run() error {
	if err := j.mi.CheckFS(); err != nil {
		nlog.Errorln(err)
		core.T.FSHC(err, j.mi, "")
		j.opts.onFinish()
		return err
	}

	if j.opts.Slab != nil {
		j.buf = j.opts.Slab.Alloc()
	}

	// 3 running options
	var err error
	switch {
	case len(j.opts.Buckets) > 0:
		debug.Assert(j.opts.Bck.IsEmpty())
		err = j.runSelected()
	case j.opts.Bck.IsQuery():
		err = j.runQbck(cmn.QueryBcks(j.opts.Bck))
	default:
		_, err = j.runBck(&j.opts.Bck)
	}

	if j.buf != nil {
		j.opts.Slab.Free(j.buf)
	}
	j.opts.onFinish()
	return err
}

// run selected buckets, one at a time
func (j *jogger) runSelected() error {
	var errs cos.Errs
	for i := range j.opts.Buckets {
		aborted, err := j.runBck(&j.opts.Buckets[i])
		if err != nil {
			errs.Add(err)
		}
		if aborted {
			return &errs
		}
	}
	return nil
}

// run matching, one at a time
func (j *jogger) runQbck(qbck cmn.QueryBcks) (err error) {
	var (
		bmd      = core.T.Bowner().Get()
		provider *string
		ns       *cmn.Ns
		errs     cos.Errs
	)
	if qbck.Provider != "" {
		provider = &qbck.Provider
	}
	if !qbck.Ns.IsGlobal() {
		ns = &qbck.Ns
	}
	bmd.Range(provider, ns, func(bck *meta.Bck) bool {
		aborted, errV := j.runBck(bck.Bucket())
		if err != nil {
			errs.Add(errV)
			err = &errs
		}
		return aborted
	})
	return
}

// run single (see also: `PerBucket` above)
func (j *jogger) runBck(bck *cmn.Bck) (aborted bool, err error) {
	opts := &fs.WalkOpts{
		Mi:       j.mi,
		CTs:      j.opts.CTs,
		Callback: j.jog,
		Sorted:   false,
	}
	opts.Bck.Copy(bck)

	err = fs.Walk(opts)

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

	if err := j.visitFQN(fqn, j.buf); err != nil {
		return err
	}

	n := j.numvis.Inc()

	// poor man's throttle; see "rate limit"
	if j.opts.Throttle {
		if fs.IsThrottle(n) {
			j.throttle()
		} else {
			runtime.Gosched()
		}
	}
	return nil
}

func (j *jogger) visitFQN(fqn string, buf []byte) error {
	ct, err := core.NewCTFromFQN(fqn, core.T.Bowner())
	if err != nil {
		return err
	}

	switch ct.ContentType() {
	case fs.ObjectType:
		lom := core.AllocLOM("")
		lom.InitCT(ct)
		err := j.visitObj(lom, buf)
		// NOTE:
		// j.opts.visitObj() callback implementations must either finish
		// synchronously or pass lom.LIF to another goroutine
		core.FreeLOM(lom)
		return err
	default:
		if err := j.visitCT(ct, buf); err != nil {
			return err
		}
	}
	return nil
}

func (j *jogger) visitObj(lom *core.LOM, buf []byte) (err error) {
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

func (j *jogger) visitCT(ct *core.CT, buf []byte) error { return j.opts.VisitCT(ct, buf) }

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

func (j *jogger) throttle() {
	curUtil := fs.GetMpathUtil(j.mi.Path)
	if curUtil >= j.config.Disk.DiskUtilHighWM {
		time.Sleep(fs.Throttle1ms)
	}
}

func (j *jogger) abort() { j.stopCh.Close() }
