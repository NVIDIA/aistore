// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const lcacheTestCksum = cos.ChecksumOneXxh

type (
	lcacheTestTarget struct {
		Target
		t   *testing.T
		bmd lcacheTestBowner
	}
	lcacheTestBowner struct{ meta.BMD }
	lcacheTestStats  struct{ cos.StatsUpdater }
)

func (tt *lcacheTestTarget) Bowner() meta.Bowner         { return &tt.bmd }
func (*lcacheTestTarget) PageMM() *memsys.MMSA           { return memsys.PageMM() }
func (*lcacheTestTarget) ByteMM() *memsys.MMSA           { return memsys.ByteMM() }
func (*lcacheTestTarget) StatsUpdater() cos.StatsUpdater { return lcacheTestStats{} }
func (o *lcacheTestBowner) Get() *meta.BMD               { return &o.BMD }
func (lcacheTestStats) Inc(string)                       {}

func (tt *lcacheTestTarget) FSHC(err error, _ *fs.Mountpath, fqn string) {
	tt.t.Errorf("unexpected filesystem error on %s: %v", fqn, err)
}

func newLcacheTest(t *testing.T, policy apc.WritePolicy) *LOM {
	t.Helper()
	mi, err := fs.NewMountpath(t.TempDir(), cos.TestMpathLabel)
	tassert.CheckFatal(t, err)

	// install the mountpath without probing block devices
	fs.NewTestMFS(nil)
	fs.PutMPI(fs.MPI{mi.Path: mi}, nil)
	t.Cleanup(func() { fs.PutMPI(nil, nil) })

	bck := meta.NewBck("lcache-test", apc.AIS, cmn.NsGlobal, &cmn.Bprops{
		BID:         NewBID(1, true),
		Cksum:       cmn.CksumConf{Type: lcacheTestCksum},
		WritePolicy: cmn.WritePolicyConf{MD: policy},
	})
	prev := T
	tt := &lcacheTestTarget{t: t}
	tt.bmd.Providers = make(meta.Providers)
	tt.bmd.Add(bck)
	Tinit(tt, nil, false /*run HK*/)
	t.Cleanup(func() { T = prev })

	lom := &LOM{ObjName: "object"}
	tassert.CheckFatal(t, lom.InitBck(bck))
	tassert.CheckFatal(t, os.MkdirAll(filepath.Dir(lom.FQN), 0o755))
	return lom
}

func lcacheTestChecksum(data string) *cos.Cksum {
	return cos.NewCksum(lcacheTestCksum, cos.ChecksumB2S([]byte(data), lcacheTestCksum))
}

func lcacheTestWrite(t *testing.T, lom *LOM, data string) {
	t.Helper()
	work := lom.GenFQN(fs.WorkCT, fs.WorkfilePut)
	tassert.CheckFatal(t, os.MkdirAll(filepath.Dir(work), 0o755))
	tassert.CheckFatal(t, os.WriteFile(work, []byte(data), 0o600))
	lom.Lock(true)
	defer lom.Unlock(true)
	lom.SetSize(int64(len(data)))
	lom.SetCksum(lcacheTestChecksum(data))
	lom.SetAtimeUnix(time.Now().Add(-time.Hour).UnixNano())
	tassert.CheckFatal(t, lom.RenameFinalize(work))
	tassert.CheckFatal(t, lom.PersistMain(false /*chunked*/))
}

func lcacheTestCandidate(t *testing.T, lom *LOM) (*evct, *lmeta) {
	t.Helper()
	cache, md := lom.fromCache()
	if md == nil {
		t.Fatal("expected a cached LOM")
	}
	e := &evct{mi: lom.mi, cache: cache, parent: &lchk{}, now: time.Now().Add(time.Hour)}
	e.adv.Batch = 0xff // keep these passes independent of sampled system load
	return e, md
}

func lcacheTestCheckDisk(t *testing.T, lom *LOM, want string) {
	t.Helper()
	fresh := &LOM{ObjName: lom.ObjName}
	tassert.CheckFatal(t, fresh.InitBck(lom.Bck()))
	tassert.CheckFatal(t, fresh.FromFS())
	data, err := os.ReadFile(fresh.FQN)
	tassert.CheckFatal(t, err)
	if string(data) != want || fresh.Lsize(true) != int64(len(data)) ||
		!lcacheTestChecksum(string(data)).Equal(fresh.Checksum()) {
		t.Fatalf("disk content %q and LOM size/checksum %d/%v, want %q and matching metadata",
			data, fresh.Lsize(true), fresh.Checksum(), want)
	}
}

func lcacheTestCheckNoXattr(t *testing.T, lom *LOM) {
	t.Helper()
	fresh := &LOM{ObjName: lom.ObjName}
	tassert.CheckFatal(t, fresh.InitBck(lom.Bck()))
	if err := fresh.LoadMetaFromFS(); !cmn.IsErrLmetaNotFound(err) {
		t.Fatalf("expected no LOM xattr, got %v", err)
	}
}

func TestLcacheEvictConcurrentRead(t *testing.T) {
	const (
		content = "read while evicting"
		n       = 4
		rounds  = 64
	)
	lom := newLcacheTest(t, apc.WriteImmediate)
	lcacheTestWrite(t, lom, content)
	e, old := lcacheTestCandidate(t, lom)
	want := lcacheTestChecksum(content)
	oldAtime := old.Atime

	readers := make([]LOM, n)
	for i := range readers {
		readers[i].ObjName = lom.ObjName
		tassert.CheckFatal(t, readers[i].InitBck(lom.Bck()))
	}
	start := make(chan struct{})
	var ready, workers sync.WaitGroup
	ready.Add(n)
	for i := range readers {
		reader := &readers[i]
		workers.Go(func() {
			reader.Lock(false)
			err := reader.Load(true /*cache it*/, true /*locked*/)
			ready.Done()
			<-start
			reader.Unlock(false)
			if err != nil {
				t.Errorf("initial load: %v", err)
				return
			}

			for j := range rounds {
				reader.Lock(false)
				err = reader.Load(true, true)
				if err != nil || reader.Lsize() != int64(len(content)) || !want.Equal(reader.Checksum()) {
					reader.Unlock(false)
					t.Errorf("load %d: size=%d checksum=%v err=%v", j, reader.Lsize(), reader.Checksum(), err)
					return
				}
				reader.SetAtimeUnix(time.Now().Add(-time.Duration(i%2) * time.Second).UnixNano())
				reader.Recache()
				reader.Unlock(false)
			}
		})
	}

	ready.Wait()
	done := make(chan struct{})
	go func() {
		e.f(lom.digest, old)
		close(done)
	}()
	// clean eviction proceeds even with readers holding the lock
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(start)
		workers.Wait()
		<-done
		t.Fatal("clean eviction waited for readers")
	}
	firstEvicted := e.evicted
	close(start)
	for range rounds {
		if current, ok := e.cache.Load(lom.digest); ok {
			e.f(lom.digest, current)
		}
		runtime.Gosched()
	}
	workers.Wait()

	if firstEvicted != 1 {
		t.Fatalf("expected clean eviction while readers held the lock, got %d", firstEvicted)
	}
	if old.uname == nil || *old.uname != lom.Uname() || old.Atime != oldAtime ||
		old.Size != int64(len(content)) || !want.Equal(old.Cksum) {
		t.Fatal("eviction changed metadata still held by a reader")
	}
	lcacheTestCheckDisk(t, lom, content)
}

func TestLcacheEvictDirtyBusy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		exclusive bool
	}{
		{"reader", false},
		{"writer", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const content = "dirty while locked"
			lom := newLcacheTest(t, apc.WriteDelayed)
			lcacheTestWrite(t, lom, content)
			e, md := lcacheTestCandidate(t, lom)
			if !md.isDirty() {
				t.Fatal("expected a dirty candidate")
			}

			lom.Lock(tc.exclusive)
			done := make(chan struct{})
			go func() {
				e.f(lom.digest, md)
				close(done)
			}()
			select {
			case <-done:
				lom.Unlock(tc.exclusive)
			case <-time.After(5 * time.Second):
				lom.Unlock(tc.exclusive)
				<-done
				t.Fatal("eviction waited for the object lock")
			}

			if current, _ := e.cache.Load(lom.digest); current != md || e.evicted != 0 || !md.isDirty() {
				t.Fatal("eviction removed or cleaned a busy dirty entry")
			}
			lcacheTestCheckNoXattr(t, lom)

			e.f(lom.digest, md)
			if _, ok := e.cache.Load(lom.digest); ok || e.evicted != 1 {
				t.Fatal("expected writeback and eviction after releasing the lock")
			}
			lcacheTestCheckDisk(t, lom, content)
		})
	}
}

func TestLcacheEvictDirtyOverwrite(t *testing.T) {
	const replacement = "a different size and checksum"
	lom := newLcacheTest(t, apc.WriteDelayed)
	lcacheTestWrite(t, lom, "old")
	e, old := lcacheTestCandidate(t, lom)
	if !old.isDirty() {
		t.Fatal("expected a dirty candidate")
	}

	// replace the file after the walk has selected the old entry
	lcacheTestWrite(t, lom, replacement)
	_, current := lcacheTestCandidate(t, lom)
	if current == old || !current.isDirty() {
		t.Fatal("expected a new dirty entry after overwrite")
	}
	lcacheTestCheckNoXattr(t, lom)
	e.f(lom.digest, old)
	if value, _ := e.cache.Load(lom.digest); value != current || e.evicted != 0 {
		t.Fatal("stale eviction removed the replacement entry")
	}
	lcacheTestCheckNoXattr(t, lom)

	e.f(lom.digest, current)
	if _, ok := e.cache.Load(lom.digest); ok || e.evicted != 1 {
		t.Fatal("expected the current entry to flush and evict")
	}
	lcacheTestCheckDisk(t, lom, replacement)
}
