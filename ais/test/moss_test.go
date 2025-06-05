// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"archive/tar"
	"fmt"
	"io"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func TestMoss(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1}) // TODO -- FIXME: remove and PASS
	t.Run("plain", testMossPlain)
	t.Run("missing-plain", testMossMissing)

	t.Skipf("skipping %s - not implemented yet", t.Name()) // TODO -- FIXME: remove and PASS
	t.Run("tar", testMossTar)
}

func testMossPlain(t *testing.T) {
	const (
		bucketName = "moss-plain-bucket"
		numObjects = 10
		objectSize = 1024
		cksumType  = cos.ChecksumNone
	)
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: bucketName + cos.GenTie(), Provider: apc.AIS}
		mem        = memsys.PageMM()
	)
	tlog.Logfln("Creating bucket %s...", bucketName)
	err := api.CreateBucket(baseParams, bck, nil)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tlog.Logfln("Destroying bucket %s...", bucketName)
		err := api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(t, err)
	})

	// Put random objects and record their names and sizes
	plainObjectNames := make(map[string]int64)
	allPlainObjectNames := make([]string, numObjects)
	tlog.Logfln("Putting %d random objects...", numObjects)
	for i := range numObjects {
		objectName := fmt.Sprintf("plain_object_%d", i)
		allPlainObjectNames[i] = objectName
		reader, err := readers.NewRand(int64(objectSize), cksumType)
		tassert.CheckFatal(t, err)
		putArgs := &api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objectName,
			Cksum:      reader.Cksum(),
			Reader:     reader,
			Size:       uint64(objectSize),
			SkipVC:     true,
		}
		_, err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)
		plainObjectNames[objectName] = int64(objectSize)
		tlog.Logfln("Put object %s (%d bytes)", objectName, objectSize)
	}

	// Prepare api.MossReq and call GetBatch with a subset of plain objects
	numToGet := rand.IntN(numObjects) + 1
	rand.Shuffle(len(allPlainObjectNames), func(i, j int) {
		allPlainObjectNames[i], allPlainObjectNames[j] = allPlainObjectNames[j], allPlainObjectNames[i]
	})
	subsetPlainNames := allPlainObjectNames[:numToGet]

	mossInSlice := make([]api.MossIn, len(subsetPlainNames))
	for i, name := range subsetPlainNames {
		mossInSlice[i] = api.MossIn{ObjName: name}
	}
	mossReq := api.MossReq{In: mossInSlice}

	sgl := mem.NewSGL(0)
	defer sgl.Free()

	resp, err := api.GetBatch(baseParams, bck, &mossReq, sgl)
	tassert.CheckFatal(t, err)
	tlog.Logfln("GetBatch: xid %q, num %d", resp.UUID, len(resp.Out))

	// Verify api.MossResp
	tassert.Errorf(t, len(resp.Out) == len(mossReq.In), "expected %d responses, got %d", len(mossReq.In), len(resp.Out))

	// Verify the TAR archive in sgl
	tr := tar.NewReader(sgl)
	foundObjects := make(map[string]int64)
	for i := 0; ; i++ {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		tassert.CheckFatal(t, err)
		name := header.Name
		size := header.Size
		foundObjects[name] = size

		if i < len(mossReq.In) {
			expectedObjName := mossReq.NameInRespArch(&bck, i)
			originalObjName := mossReq.In[i].ObjName
			tassert.Errorf(t, name == expectedObjName, "expected TAR entry '%s' at index %d, got '%s'", expectedObjName, i, name)
			if out := findMossOut(resp.Out, originalObjName); out != nil {
				tassert.Errorf(t, out.Size == size, "expected size %d for '%s', got %d in TAR", plainObjectNames[originalObjName], originalObjName, size)
			} else {
				t.Errorf("api.MossOut for '%s' not found in response", originalObjName)
			}
		}
		tlog.Logfln("Found file in TAR: %s (%d bytes)", name, size)
	}

	tassert.Errorf(t, len(foundObjects) == len(mossReq.In), "expected %d files in TAR, got %d", len(mossReq.In), len(foundObjects))
}

func testMossTar(t *testing.T) {
	tlog.Logfln("Running TestMoss - tar...")
	const (
		bucketName    = "moss-tar-bucket"
		tarFileName   = "moss_archive.tar"
		numFilesInTar = 5
		fileSizeInTar = 512
	)
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: bucketName + cos.GenTie(), Provider: apc.AIS}
		mem        = memsys.PageMM()
	)
	tlog.Logfln("Creating bucket %s...", bucketName)
	err := api.CreateBucket(baseParams, bck, nil)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tlog.Logfln("Destroying bucket %s...", bucketName)
		err := api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(t, err)
	})

	// Create a local tar file with random content using tarch and record internal file sizes
	tlog.Logfln("Creating local tar file %s with %d objects...", tarFileName, numFilesInTar)
	tarDir := t.TempDir()
	tarFullPath := filepath.Join(tarDir, tarFileName)
	tarInternalFileNames := make([]string, numFilesInTar)
	tarObjectNames := make([]string, numFilesInTar)
	internalFileSizes := make(map[string]int, numFilesInTar)
	for i := range tarInternalFileNames {
		tarInternalFileNames[i] = fmt.Sprintf("file_%d.txt", i)
		tarObjectNames[i] = fmt.Sprintf("%s/%s", tarFileName, tarInternalFileNames[i])
		internalFileSizes[tarInternalFileNames[i]] = fileSizeInTar
	}

	err = tarch.CreateArchRandomFiles(
		tarFullPath,
		tar.FormatGNU, // You can choose other formats as well
		".tar",
		numFilesInTar,
		fileSizeInTar,
		false,                // duplication
		false,                // random dir prefix
		nil,                  // record extensions
		tarInternalFileNames, // pregenerated filenames
	)
	tassert.CheckFatal(t, err)
	tlog.Logfln("Local tar file created: %s", tarFullPath)

	// Put the tar file into the bucket
	tlog.Logfln("Putting tar file %s into bucket %s...", tarFileName, bucketName)
	fileReader, err := cos.NewFileHandle(tarFullPath)
	tassert.CheckFatal(t, err)
	defer fileReader.Close()

	putArgs := &api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: tarFileName, Reader: fileReader}
	_, err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	// Prepare api.MossReq and call GetBatch with a subset of tar objects
	numToGet := rand.IntN(numFilesInTar) + 1
	rand.Shuffle(len(tarObjectNames), func(i, j int) { tarObjectNames[i], tarObjectNames[j] = tarObjectNames[j], tarObjectNames[i] })
	subsetTarNames := tarObjectNames[:numToGet]

	mossInSlice := make([]api.MossIn, len(subsetTarNames))
	for i, name := range subsetTarNames {
		mossInSlice[i] = api.MossIn{ObjName: name}
	}
	mossReq := api.MossReq{In: mossInSlice}

	sgl := mem.NewSGL(0)
	defer sgl.Free()

	resp, err := api.GetBatch(baseParams, bck, &mossReq, sgl)
	tassert.CheckFatal(t, err)
	tlog.Logfln("GetBatch: xid %q, num %d", resp.UUID, len(resp.Out))

	// Verify api.MossResp
	tassert.Errorf(t, len(resp.Out) == len(mossReq.In), "expected %d responses, got %d", len(mossReq.In), len(resp.Out))

	// Verify the TAR archive in sgl
	tr := tar.NewReader(sgl)
	foundEntries := make(map[string]int64, len(mossReq.In))
	for i := 0; ; i++ {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		tassert.CheckFatal(t, err)
		name := header.Name
		size := header.Size
		foundEntries[name] = size

		tlog.Logfln("\t%2d: entry in response TAR: %s (%d)", i, name, size)

		if i < len(mossReq.In) {
			originalObjName := mossReq.In[i].ObjName

			// For tar-within-tar, we expect the internal file name in the result TAR
			// The originalObjName is like "archive.tar/file_1.txt",
			// and we expect just "file_1.txt" in the resulting TAR
			parts := strings.SplitN(originalObjName, "/", 2)
			var expectedInternalName string
			if len(parts) > 1 {
				expectedInternalName = parts[1] // Extract "file_1.txt" from "archive.tar/file_1.txt"
				tassert.Errorf(t, name == expectedInternalName, "expected TAR entry '%s' at index %d, got '%s'", expectedInternalName, i, name)

				if out := findMossOut(resp.Out, originalObjName); out != nil {
					tassert.Errorf(t, out.Size == size, "expected MossOut size %d for TAR-ed file '%s', got %d", out.Size, originalObjName, size)
					tassert.Errorf(t, internalFileSizes[expectedInternalName] == int(size), "expected original size %d for TAR-ed file '%s', got %d", internalFileSizes[expectedInternalName], originalObjName, size)
				} else {
					t.Errorf("api.MossOut for '%s' not found in response", originalObjName)
				}
			} else {
				t.Errorf("Unexpected full name format: %s", originalObjName)
			}
		}
	}

	tassert.Errorf(t, len(foundEntries) == len(mossReq.In), "expected %d files in TAR, got %d", len(mossReq.In), len(foundEntries))
}

func testMossMissing(t *testing.T) {
	tlog.Logfln("Running TestMoss - missing objects...")
	const (
		bucketName  = "moss-missing-bucket"
		numExisting = 10
		numMissing  = 2
		objectSize  = 256
		fake        = "-fake.txt"
	)
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: bucketName + cos.GenTie(), Provider: apc.AIS}
		mem        = memsys.PageMM()
	)
	tlog.Logfln("Creating bucket %s...", bucketName)
	err := api.CreateBucket(baseParams, bck, nil)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tlog.Logfln("Destroying bucket %s...", bucketName)
		err := api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(t, err)
	})

	// Put a few real objects
	existingNames := make([]string, numExisting)
	for i := range numExisting {
		name := fmt.Sprintf("real_%d.txt", i)
		existingNames[i] = name
		reader, err := readers.NewRand(int64(objectSize), cos.ChecksumNone)
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams, Bck: bck, ObjName: name,
			Reader: reader, Size: uint64(objectSize), SkipVC: true,
		})
		tassert.CheckFatal(t, err)
		tlog.Logfln("Put object %s", name)
	}

	// Compose mixed input: some real, some fake
	mossIn := make([]api.MossIn, numExisting+numMissing)
	for i := range numExisting + numMissing {
		if i < numExisting {
			mossIn[i] = api.MossIn{ObjName: existingNames[i]}
		} else {
			mossIn[i] = api.MossIn{ObjName: cos.GenTie() + fake}
		}
	}
	rand.Shuffle(len(mossIn), func(i, j int) {
		mossIn[i], mossIn[j] = mossIn[j], mossIn[i]
	})

	sgl := mem.NewSGL(0)
	defer sgl.Free()

	mossReq := api.MossReq{
		In:            mossIn,
		ContinueOnErr: true,
	}
	tlog.Logfln("Calling GetBatch (with ContinueOnErr=true)")
	resp, err := api.GetBatch(baseParams, bck, &mossReq, sgl)
	tassert.CheckFatal(t, err)
	tlog.Logfln("GetBatch: xid %q, num %d", resp.UUID, len(resp.Out))

	tassert.Errorf(t, len(resp.Out) == len(mossIn), "expected %d MossOuts, got %d", len(mossIn), len(resp.Out))

	// Verify TAR stream entries
	var (
		tr         = tar.NewReader(sgl)
		namesInTar = make([]string, 0, numExisting+numMissing)
	)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		tassert.CheckFatal(t, err)
		namesInTar = append(namesInTar, hdr.Name)
	}

	expectedNames := make(map[string]struct{}, numExisting+numMissing)
	for i, mossInEntry := range mossIn {
		objName := mossInEntry.ObjName
		expectedTarName := mossReq.NameInRespArch(&bck, i)

		if strings.HasSuffix(objName, fake) {
			// Missing files go under __404__/ directory
			expectedNames[filepath.Join(api.MissingFilesDirectory, expectedTarName)] = struct{}{}
		} else {
			// Existing files use the normal naming convention
			expectedNames[expectedTarName] = struct{}{}
		}
	}

	for _, name := range namesInTar {
		if _, ok := expectedNames[name]; !ok {
			t.Errorf("unexpected name in TAR: %s", name)
		}
		delete(expectedNames, name)
	}
	for missing := range expectedNames {
		t.Errorf("missing entry in TAR: %s", missing)
	}

	// Verify MossOuts
	for _, out := range resp.Out {
		if strings.HasSuffix(out.ObjName, fake) {
			tassert.Errorf(t, out.ErrMsg != "", "expected error message for %q", out.ObjName)
			tassert.Errorf(t, out.Size == 0, "expected size 0 for %q", out.ObjName)
		} else {
			tassert.Errorf(t, out.ErrMsg == "", "unexpected error for %q: %s", out.ObjName, out.ErrMsg)
			tassert.Errorf(t, out.Size == int64(objectSize), "wrong size for %q: got %d", out.ObjName, out.Size)
		}
	}
}

func findMossOut(outSlice []api.MossOut, objName string) *api.MossOut {
	for _, out := range outSlice {
		if out.ObjName == objName {
			return &out
		}
	}
	return nil
}
