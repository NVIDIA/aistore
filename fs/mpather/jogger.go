// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// walk all or selected buckets, one at a time

type LoadType int

const (
	noLoad LoadType = iota
	Load
)

type (
	JgroupOpts struct {
		onFinish    func()
		Parent      cos.Stopper // TODO -- FIXME: start using xaction parent
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
		RW          bool     // true when performs data IO
	}

	// Jgroup runs jogger per mountpath which walk the entire bucket and
	// call callback on each of the encountered object. When jogger encounters
	// error it stops and informs other joggers about the error (so they stop too).
	Jgroup struct {
		wg         sync.WaitGroup
		joggers    map[string]*jogger
		stopCh     cos.StopCh // group stop (first error or external Stop)
		finishedCh cos.StopCh // when all joggers are done

		finishedCnt atomic.Uint32

		errOnce sync.Once
		err     error
	}

	// jogger is being run on each mountpath and executes fs.Walk which call
	// provided callback.
	jogger struct {
		opts      *JgroupOpts
		mi        *fs.Mountpath
		bdir      string // mi.MakePath(bck)
		objPrefix string // fully-qualified prefix, as in: join(bdir, opts.Prefix)
		config    *cmn.Config
		stopCh    *cos.StopCh // shared group stop
		buf       []byte
		numvis    atomic.Int64 // counter: num visited objects
		adv       load.Advice  // throttle
	}
)

func NewJgroup(opts *JgroupOpts, config *cmn.Config, smi *fs.Mountpath) *Jgroup {
	var (
		joggers map[string]*jogger
		avail   = fs.GetAvail()
		la      = len(avail)
		jg      = &Jgroup{}
	)
	debug.Assert(!opts.IncludeCopy || (opts.IncludeCopy && opts.DoLoad > noLoad))

	opts.onFinish = jg.markFinished

	jg.stopCh.Init()
	jg.finishedCh.Init()

	switch {
	case smi != nil: // selected mountpath
		if _, ok := avail[smi.Path]; !ok {
			nlog.Errorln(smi.String(), "is not available, nothing to do")
		} else {
			joggers = make(map[string]*jogger, 1)
			joggers[smi.Path] = newJogger(opts, smi, config, &jg.stopCh)
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
				joggers[k] = newJogger(&nopts, mi, config, &jg.stopCh)
			}
		}
	default:
		joggers = make(map[string]*jogger, la)
		for _, mi := range avail {
			joggers[mi.Path] = newJogger(opts, mi, config, &jg.stopCh)
		}
	}

	if len(joggers) == 0 {
		// this jogger group is a no-op (unlikely)
		if smi == nil {
			_, disabled := fs.Get()
			nlog.Errorf("%v: avail=%v, disabled=%v", cmn.ErrNoMountpaths, avail, disabled)
		}
		jg.stopCh.Close()
		jg.finishedCh.Close()
	}

	jg.joggers = joggers
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
	for _, j := range jg.joggers {
		jg.wg.Add(1)
		go func(j *jogger) {
			defer jg.wg.Done()
			if err := j.run(); err != nil {
				jg.setErr(err)
			}
		}(j)
	}
}

func (jg *Jgroup) Stop() error {
	jg.stopCh.Close()
	jg.wg.Wait()
	return jg.err
}

func (jg *Jgroup) ListenFinished() <-chan struct{} { return jg.finishedCh.Listen() }

func (jg *Jgroup) markFinished() {
	if n := jg.finishedCnt.Inc(); n == uint32(len(jg.joggers)) {
		jg.finishedCh.Close()
	}
}

func (jg *Jgroup) setErr(err error) {
	debug.Assert(err != fs.ErrWalkStopped, "walk-stopped sentinel must be consumed by fs.walk")
	if err == nil || cmn.IsErrAborted(err) {
		return
	}
	jg.errOnce.Do(func() {
		jg.err = err
		jg.stopCh.Close()
	})
}

func newJogger(opts *JgroupOpts, mi *fs.Mountpath, config *cmn.Config, stopCh *cos.StopCh) (j *jogger) {
	j = &jogger{
		opts:   opts,
		mi:     mi,
		config: config,
		stopCh: stopCh,
	}
	if opts.Prefix != "" {
		j.bdir = mi.MakePathCT(&j.opts.Bck, fs.ObjCT) // this mountpath's bucket dir that contains objects
		j.objPrefix = filepath.Join(j.bdir, opts.Prefix)
	}
	// throttling context
	j.adv.Init(load.FlMem|load.FlDsk, &load.Extra{Mi: j.mi, Cfg: &j.config.Disk, RW: j.opts.RW})
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
	errs := cos.NewErrs()
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
		errs     = cos.NewErrs()
		provider *string
		ns       *cmn.Ns
	)
	if qbck.Provider != "" {
		provider = &qbck.Provider
	}
	if !qbck.Ns.IsGlobal() {
		ns = &qbck.Ns
	}
	bmd.Range(provider, ns, func(bck *meta.Bck) bool {
		aborted, errV := j.runBck(bck.Bucket())
		if errV != nil {
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

	if j.stopped() {
		return fs.ErrWalkStopped
	}
	if err := j.visitFQN(fqn, j.buf); err != nil {
		return err
	}

	n := j.numvis.Inc()
	if j.opts.RW && j.adv.ShouldCheck(n) {
		j.adv.Refresh()
		if j.adv.Sleep > 0 {
			time.Sleep(j.adv.Sleep)
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
	case fs.ObjCT:
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
	if j.opts.DoLoad == Load {
		if err = lom.Load(false, false); err != nil {
			return
		}
		if !j.opts.IncludeCopy && lom.IsCopy() {
			return nil
		}
	}
	return j.opts.VisitObj(lom, buf)
}

func (j *jogger) visitCT(ct *core.CT, buf []byte) error { return j.opts.VisitCT(ct, buf) }

func (j *jogger) stopped() bool {
	var xabortCh <-chan error // nil
	if j.opts.Parent != nil {
		xabortCh = j.opts.Parent.ChanAbort()
	}
	select {
	// 1. jgroup.Stop()
	case <-j.stopCh.Listen():
		return true
	// 2. (optional) parent xaction aborted
	// caller is expected to prioritize xact.AbortErr() over the one returned by jgroup.Stop()
	case <-xabortCh:
		return true
	default:
		return false
	}
}
