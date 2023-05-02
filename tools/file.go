// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/cryptorand"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

type (
	DirTreeDesc struct {
		InitDir string // Directory where the tree is created (can be empty).
		Dirs    int    // Number of (initially empty) directories at each depth (we recurse into single directory at each depth).
		Files   int    // Number of files at each depth.
		Depth   int    // Depth of tree/nesting.
		Empty   bool   // Determines if there is a file somewhere in the directories.
	}

	ContentTypeDesc struct {
		Type       string
		ContentCnt int
	}

	ObjectsDesc struct {
		CTs           []ContentTypeDesc // Content types which are interesting for the test.
		MountpathsCnt int               // Number of mountpaths to be created.
		ObjectSize    int64
	}

	ObjectsOut struct {
		Dir             string
		T               cluster.Target
		Bck             cmn.Bck
		FQNs            map[string][]string // ContentType => FQN
		MpathObjectsCnt map[string]int      // mpath -> # objects on the mpath
	}
)

func RandomObjDir(dirLen, maxDepth int) (dir string) {
	depth := rand.Intn(maxDepth)
	for i := 0; i < depth; i++ {
		dir = filepath.Join(dir, trand.String(dirLen))
	}
	return
}

func SetXattrCksum(fqn string, bck cmn.Bck, cksum *cos.Cksum) error {
	lom := &cluster.LOM{}
	// NOTE: this is an intentional hack to go ahead and corrupt the checksum
	//       - init and/or load errors are ignored on purpose
	_ = lom.InitFQN(fqn, &bck)
	_ = lom.LoadMetaFromFS()
	lom.SetCksum(cksum)
	return lom.Persist()
}

func CheckPathExists(t *testing.T, path string, dir bool) {
	if fi, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else {
		if dir && !fi.IsDir() {
			t.Fatalf("expected path %q to be directory", path)
		} else if !dir && fi.IsDir() {
			t.Fatalf("expected path %q to not be directory", path)
		}
	}
}

func CheckPathNotExists(t *testing.T, path string) {
	if err := cos.Stat(path); err == nil || !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func PrepareDirTree(tb testing.TB, desc DirTreeDesc) (string, []string) {
	fileNames := make([]string, 0, 100)
	topDirName, err := os.MkdirTemp(desc.InitDir, "")
	tassert.CheckFatal(tb, err)

	nestedDirectoryName := topDirName
	for depth := 1; depth <= desc.Depth; depth++ {
		names := make([]string, 0, desc.Dirs)
		for i := 1; i <= desc.Dirs; i++ {
			name, err := os.MkdirTemp(nestedDirectoryName, "")
			tassert.CheckFatal(tb, err)
			names = append(names, name)
		}
		for i := 1; i <= desc.Files; i++ {
			f, err := os.CreateTemp(nestedDirectoryName, "")
			tassert.CheckFatal(tb, err)
			fileNames = append(fileNames, f.Name())
			f.Close()
		}
		sort.Strings(names)
		if desc.Dirs > 0 {
			// We only recurse into last directory.
			nestedDirectoryName = names[len(names)-1]
		}
	}

	if !desc.Empty {
		f, err := os.CreateTemp(nestedDirectoryName, "")
		tassert.CheckFatal(tb, err)
		fileNames = append(fileNames, f.Name())
		f.Close()
	}
	return topDirName, fileNames
}

func PrepareObjects(t *testing.T, desc ObjectsDesc) *ObjectsOut {
	var (
		buf       = make([]byte, desc.ObjectSize)
		fqns      = make(map[string][]string, len(desc.CTs))
		mpathCnts = make(map[string]int, desc.MountpathsCnt)

		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
			Ns:       cmn.NsGlobal,
			Props: &cmn.BucketProps{
				Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash},
				BID:   0xa5b6e7d8,
			},
		}
		bmd   = mock.NewBaseBownerMock((*meta.Bck)(&bck))
		tMock cluster.Target
	)

	mios := mock.NewIOS()
	fs.TestNew(mios)
	fs.TestDisableValidation()

	_ = fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{})
	_ = fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.Reg(fs.ECSliceType, &fs.ECSliceContentResolver{})
	_ = fs.CSM.Reg(fs.ECMetaType, &fs.ECMetaContentResolver{})

	dir := t.TempDir()

	for i := 0; i < desc.MountpathsCnt; i++ {
		mpath, err := os.MkdirTemp(dir, "")
		tassert.CheckFatal(t, err)
		mp, err := fs.Add(mpath, "daeID")
		tassert.CheckFatal(t, err)
		mpathCnts[mp.Path] = 0
	}

	if len(desc.CTs) == 0 {
		return nil
	}

	tMock = mock.NewTarget(bmd)

	errs := fs.CreateBucket(&bck, false /*nilbmd*/)
	if len(errs) > 0 {
		tassert.CheckFatal(t, errs[0])
	}

	for _, ct := range desc.CTs {
		for i := 0; i < ct.ContentCnt; i++ {
			fqn, _, err := cluster.HrwFQN(&bck, ct.Type, trand.String(15))
			tassert.CheckFatal(t, err)

			fqns[ct.Type] = append(fqns[ct.Type], fqn)

			f, err := cos.CreateFile(fqn)
			tassert.CheckFatal(t, err)
			_, _ = cryptorand.Read(buf)
			_, err = f.Write(buf)
			f.Close()
			tassert.CheckFatal(t, err)

			parsedFQN, err := fs.ParseFQN(fqn)
			tassert.CheckFatal(t, err)
			mpathCnts[parsedFQN.Mountpath.Path]++

			switch ct.Type {
			case fs.ObjectType:
				lom := &cluster.LOM{}
				err = lom.InitFQN(fqn, nil)
				tassert.CheckFatal(t, err)

				lom.SetSize(desc.ObjectSize)
				lom.SetAtimeUnix(time.Now().UnixNano())
				err = lom.Persist()
				tassert.CheckFatal(t, err)
			case fs.WorkfileType, fs.ECSliceType, fs.ECMetaType:
			default:
				cos.AssertMsg(false, "non-implemented type")
			}
		}
	}

	return &ObjectsOut{
		Dir:             dir,
		T:               tMock,
		Bck:             bck,
		FQNs:            fqns,
		MpathObjectsCnt: mpathCnts,
	}
}

func PrepareMountPaths(t *testing.T, cnt int) fs.MPI {
	PrepareObjects(t, ObjectsDesc{
		MountpathsCnt: cnt,
	})
	AssertMountpathCount(t, cnt, 0)
	return fs.GetAvail()
}

func RemoveMpaths(t *testing.T, mpaths fs.MPI) {
	for _, mpath := range mpaths {
		removedMP, err := fs.Remove(mpath.Path)
		tassert.CheckError(t, err)
		tassert.Errorf(t, removedMP != nil, "expected remove to be successful")
		tassert.CheckError(t, fs.RemoveAll(mpath.Path))
	}
}

func AddMpath(t *testing.T, path string) {
	err := cos.CreateDir(path) // Create directory if not exists
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		fs.RemoveAll(path)
	})
	_, err = fs.Add(path, "daeID")
	tassert.Errorf(t, err == nil, "Failed adding mountpath %q, err: %v", path, err)
}

func AssertMountpathCount(t *testing.T, availableCount, disabledCount int) {
	availableMountpaths, disabledMountpaths := fs.Get()
	if len(availableMountpaths) != availableCount ||
		len(disabledMountpaths) != disabledCount {
		t.Errorf(
			"wrong mountpaths: %d/%d, %d/%d",
			len(availableMountpaths), availableCount,
			len(disabledMountpaths), disabledCount,
		)
	}
}

func CreateFileFromReader(t *testing.T, fileName string, r io.Reader) string {
	filePath := filepath.Join(t.TempDir(), fileName)
	f, err := os.Create(filePath)
	tassert.CheckFatal(t, err)

	_, err = io.Copy(f, r)
	tassert.CheckFatal(t, err)

	err = f.Close()
	tassert.CheckFatal(t, err)

	return filePath
}

func FilesEqual(file1, file2 string) (bool, error) {
	f1, err := os.ReadFile(file1)
	if err != nil {
		return false, err
	}
	f2, err := os.ReadFile(file2)
	if err != nil {
		return false, err
	}
	return bytes.Equal(f1, f2), nil
}
