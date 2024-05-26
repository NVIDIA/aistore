// Package integration_test.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func prefixCreateFiles(t *testing.T, proxyURL string, bck cmn.Bck, cksumType string) []string {
	const (
		objCnt   = 100
		fileSize = cos.KiB
	)

	// Create specific files to test corner cases.
	var (
		extraNames = []string{"dir/obj01", "dir/obj02", "dir/obj03", "dir1/dir2/obj04", "dir1/dir2/obj05"}
		fileNames  = make([]string, 0, objCnt)
		wg         = &sync.WaitGroup{}
		errCh      = make(chan error, objCnt+len(extraNames))
	)

	for range objCnt {
		fileName := trand.String(20)
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)

		// NOTE: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader.
		r, err := readers.NewRand(fileSize, cksumType)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			tools.Put(proxyURL, bck, keyName, r, errCh)
		}()
		fileNames = append(fileNames, fileName)
	}

	for _, fName := range extraNames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fName)
		// NOTE: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader.
		r, err := readers.NewRand(fileSize, cksumType)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			tools.Put(proxyURL, bck, keyName, r, errCh)
		}()
		fileNames = append(fileNames, fName)
	}

	wg.Wait()
	tassert.SelectErr(t, errCh, "put", false)
	return fileNames
}

func prefixLookup(t *testing.T, proxyURL string, bck cmn.Bck, fileNames []string) {
	prefixLookupDefault(t, proxyURL, bck, fileNames)
	prefixLookupCornerCases(t, proxyURL, bck, fileNames)
}

func prefixLookupDefault(t *testing.T, proxyURL string, bck cmn.Bck, fileNames []string) {
	tlog.Logf("Looking up for files in alphabetic order\n")

	var (
		letters    = "abcdefghijklmnopqrstuvwxyz"
		baseParams = tools.BaseAPIParams(proxyURL)
	)
	for i := range len(letters) {
		key := letters[i : i+1]
		lookFor := fmt.Sprintf("%s/%s", prefixDir, key)
		msg := &apc.LsoMsg{Prefix: lookFor}
		objList, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		numFiles := len(objList.Entries)
		realNumFiles := numberOfFilesWithPrefix(fileNames, key)

		if numFiles == realNumFiles {
			if numFiles != 0 {
				tlog.Logf("Found %v files starting with %q\n", numFiles, key)
			}
		} else {
			t.Errorf("Expected number of files with prefix %q is %v but found %v files", key, realNumFiles, numFiles)
			tlog.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tlog.Logf("    %d[%d]. %s\n", i, id, oo.Name)
			}
		}
	}
}

func prefixLookupCornerCases(t *testing.T, proxyURL string, bck cmn.Bck, objNames []string) {
	tlog.Logln("Testing corner cases")

	var (
		on  = cos.StrKVs{"features": feat.DontOptimizeVirtualDir.String()}
		off = cos.StrKVs{"features": "0"}
	)
	if bck.IsRemoteAIS() {
		tools.SetRemAisConfig(t, on)
		t.Cleanup(func() {
			tools.SetRemAisConfig(t, off)
		})
	} else {
		tools.SetClusterConfig(t, on)
		t.Cleanup(func() {
			tools.SetClusterConfig(t, off)
		})
	}

	tests := []struct {
		title  string
		prefix string
	}{
		{"Entire list (dir)", "dir"},
		{"dir/", "dir/"},
		{"dir1", "dir1"},
		{"dir1/", "dir1/"},
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	for idx, test := range tests {
		var (
			fullPrefix = fmt.Sprintf("%s/%s", prefixDir, test.prefix)
			expCnt     int
		)
		for _, objName := range objNames {
			fullObjName := fmt.Sprintf("%s/%s", prefixDir, objName)
			if strings.HasPrefix(fullObjName, fullPrefix) {
				expCnt++
			}
		}

		tlog.Logf("%d. Prefix: %s [%s]\n", idx, test.title, fullPrefix)
		msg := &apc.LsoMsg{Prefix: fullPrefix}
		objList, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		if len(objList.Entries) != expCnt {
			t.Errorf("prefix %q: expected %d objects, got %d", fullPrefix, expCnt, len(objList.Entries))
			tlog.Logln("namely:")
			for _, en := range objList.Entries {
				tlog.Logln("    " + en.Name)
			}
			tlog.Logln("whereby the complete list:")
			for _, name := range objNames {
				if strings.HasPrefix(name, test.prefix) {
					tlog.Logln("    " + name)
				}
			}
		}
	}
}

func numberOfFilesWithPrefix(fileNames []string, namePrefix string) int {
	numFiles := 0
	for _, fileName := range fileNames {
		if strings.HasPrefix(fileName, namePrefix) {
			numFiles++
		}
	}
	return numFiles
}

func prefixCleanup(t *testing.T, proxyURL string, bck cmn.Bck, fileNames []string) {
	var (
		wg    = cos.NewLimitedWaitGroup(40, 0)
		errCh = make(chan error, len(fileNames))
	)

	for _, fileName := range fileNames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tools.Del(proxyURL, bck, keyName, nil, errCh, true)
		}()
	}
	wg.Wait()

	select {
	case e := <-errCh:
		tlog.Logf("Failed to DEL: %s\n", e)
		t.Fail()
	default:
	}
}
