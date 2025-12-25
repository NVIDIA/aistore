// Package res_test: unit tests
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package res_test

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// TODO: remove this one once the helpers (below) get properly utilized
func TestDummy(t *testing.T) {
	const (
		numMpaths = 3
		bucket    = "test-bucket"
		objName   = "a/b/c/test-object"
	)
	bowner, bcks := newBBB(bucket)
	target := mock.NewTarget(bowner)

	var (
		mpaths = make([]string, 0, numMpaths)
		tmpDir = t.TempDir()
	)

	for i := range numMpaths {
		mpaths = append(mpaths, fmt.Sprintf("%s/mpath-%d", tmpDir, i))
	}

	newFS(t, mpaths, target.SID())
	newConfig(tmpDir, numMpaths)

	_ = newLOM(t, bcks[0], objName, 123)
	_ = newChunkedLOM(t, bcks[0], objName, 20, 123)
}

//
// misc. helpers
// intentionally keeping minimal and reusable (newBBB, newLOM, FS/config init).
//

func newFS(t *testing.T, mpaths []string, targetID string) {
	fs.TestNew(nil)
	for _, mpath := range mpaths {
		err := cos.CreateDir(mpath)
		tassert.Errorf(t, err == nil, "%s (%v)", mpath, err)
		_, err = fs.Add(mpath, targetID)
		tassert.Errorf(t, err == nil, "%s (%v)", mpath, err)
	}
}

func newConfig(confDir string, numMpaths int) *cmn.Config {
	config := cmn.GCO.BeginUpdate()
	config.ConfigDir = confDir
	config.TestFSP.Count = numMpaths
	config.Log.Level = "3"
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)
	return config
}

func newBBB(buckets ...string) (meta.Bowner, []*meta.Bck) {
	bcks := make([]*meta.Bck, 0, len(buckets))
	for _, name := range buckets {
		b := meta.NewBck(
			name, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}}, // just an example
		)
		bcks = append(bcks, b)
	}
	return mock.NewBaseBownerMock(bcks...), bcks
}

func newLOM(t *testing.T, bck *meta.Bck, objName string, size int64) *core.LOM {
	t.Helper()
	var (
		wfh    *os.File
		reader readers.Reader
	)
	lom := &core.LOM{ObjName: objName}
	err := lom.InitBck(bck)
	tassert.CheckFatal(t, err)

	wfh, err = cos.CreateFile(lom.FQN)
	tassert.CheckFatal(t, err)
	defer wfh.Close()

	lom.SetSize(size)
	lom.IncVersion()
	lom.SetAtimeUnix(time.Now().UnixNano())

	if size == 0 {
		goto ret
	}

	reader, err = readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: bck.CksumConf().Type})
	tassert.CheckFatal(t, err)
	defer reader.Close()

	_, err = io.Copy(wfh, reader)
	tassert.CheckFatal(t, err)
ret:
	lom.Lock(true /*exclusive*/)
	lom.PersistMain(false /*chunked*/)
	lom.Unlock(true)
	return lom
}

func newChunkedLOM(t *testing.T, bck *meta.Bck, objName string, numChunks int, sizeChunk int64) *core.LOM {
	t.Helper()
	lom := &core.LOM{ObjName: objName}
	err := lom.InitBck(bck)
	tassert.CheckFatal(t, err)

	totalSize := int64(numChunks) * sizeChunk
	lom.SetSize(totalSize)

	// Create Ufest for chunked upload
	ufest, err := core.NewUfest("", lom, false)
	tassert.CheckFatal(t, err)

	// Create chunks
	for i := 1; i <= numChunks; i++ {
		chunk, err := ufest.NewChunk(i, lom)
		tassert.CheckFatal(t, err)

		createTestFile(t, bck, chunk.Path(), sizeChunk)

		err = ufest.Add(chunk, sizeChunk, int64(i))
		tassert.CheckFatal(t, err)
	}

	err = lom.CompleteUfest(ufest, false)
	tassert.CheckFatal(t, err)

	err = lom.Load(false, false)
	tassert.CheckFatal(t, err)

	tassert.Fatal(t, lom.IsChunked(), "expecting lom _chunked_ upon loading")
	return lom
}
