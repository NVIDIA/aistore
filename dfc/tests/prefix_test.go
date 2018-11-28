// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
//
// Example run:
// 	go test -v -run=prefix -args -numfiles=50 -prefix="filter/a"
//
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

const (
	prefixDir = "filter"
)

var (
	prefixFileNumber int
)

// if the prefix flag is set via command line the test looks only for the prefix
// and checks if the number of items equals the number of files with
// the names starting with the prefix;
// otherwise, the test creates (PUT) random files and executes 'a*' through 'z*' listings.
func TestPrefix(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)

	tutils.Logf("Looking for files with prefix [%s]\n", prefix)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer destroyLocalBucket(t, proxyURL, clibucket)
	}
	prefixFileNumber = numfiles
	prefixCreateFiles(t, proxyURL)
	prefixLookup(t, proxyURL)
	prefixCleanup(t, proxyURL)
}

func numberOfFilesWithPrefix(fileNames []string, namePrefix string, commonDir string) int {
	numFiles := 0
	for _, fileName := range fileNames {
		if commonDir != "" {
			fileName = fmt.Sprintf("%s/%s", commonDir, fileName)
		}
		if strings.HasPrefix(fileName, namePrefix) {
			numFiles++
		}
	}
	return numFiles
}

func prefixCreateFiles(t *testing.T, proxyURL string) {
	src := rand.NewSource(baseseed + 1000)
	random := rand.New(src)
	fileNames = make([]string, 0, prefixFileNumber)
	errCh := make(chan error, numfiles)
	wg := &sync.WaitGroup{}

	for i := 0; i < prefixFileNumber; i++ {
		fileName := tutils.FastRandomFilename(random, fnlen)
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)

		// Note: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader
		r, err := tutils.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go tutils.PutAsync(wg, proxyURL, clibucket, keyName, r, errCh)
		fileNames = append(fileNames, fileName)
	}

	// create specific files to test corner cases
	extranames := []string{"dir/obj01", "dir/obj02", "dir/obj03", "dir1/dir2/obj04", "dir1/dir2/obj05"}
	for _, fName := range extranames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fName)
		// Note: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader
		r, err := tutils.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go tutils.PutAsync(wg, proxyURL, clibucket, keyName, r, errCh)
		fileNames = append(fileNames, fName)
	}

	wg.Wait()

	select {
	case e := <-errCh:
		tutils.Logf("Failed to PUT: %s\n", e)
		t.Fail()
	default:
	}
}

func prefixLookupOne(t *testing.T, proxyURL string) {
	tutils.Logf("Looking up for files than names start with %s\n", prefix)
	var msg = &cmn.GetMsg{GetPrefix: prefix}
	numFiles := 0
	baseParams := tutils.BaseAPIParams(proxyURL)
	objList, err := api.ListBucket(baseParams, clibucket, msg, 0)
	if err != nil {
		t.Errorf("List files with prefix failed, err = %v", err)
		return
	}

	for _, entry := range objList.Entries {
		tutils.Logf("Found object: %s\n", entry.Name)
		numFiles++
	}

	realNumFiles := numberOfFilesWithPrefix(fileNames, prefix, prefixDir)
	if realNumFiles == numFiles {
		tutils.Logf("Total files with prefix found: %v\n", numFiles)
	} else {
		t.Errorf("Expected number of files with prefix '%s' is %v but found %v files", prefix, realNumFiles, numFiles)
	}
}

func prefixLookupDefault(t *testing.T, proxyURL string) {
	tutils.Logf("Looking up for files in alphabetic order\n")

	baseParams := tutils.BaseAPIParams(proxyURL)
	letters := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < len(letters); i++ {
		key := letters[i : i+1]
		lookFor := fmt.Sprintf("%s/%s", prefixDir, key)
		var msg = &cmn.GetMsg{GetPrefix: lookFor}
		objList, err := api.ListBucket(baseParams, clibucket, msg, 0)
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		numFiles := len(objList.Entries)
		realNumFiles := numberOfFilesWithPrefix(fileNames, key, prefix)

		if numFiles == realNumFiles {
			if numFiles != 0 {
				tutils.Logf("Found %v files starting with '%s'\n", numFiles, key)
			}
		} else {
			t.Errorf("Expected number of files with prefix '%s' is %v but found %v files", key, realNumFiles, numFiles)
			tutils.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tutils.Logf("    %d[%d]. %s\n", i, id, oo.Name)
			}
		}
	}
}

func prefixLookupCornerCases(t *testing.T, proxyURL string) {
	tutils.Logf("Testing corner cases\n")

	type testProps struct {
		title    string
		prefix   string
		objCount int
	}
	tests := []testProps{
		{"Entire list (dir)", "dir", 5},
		{"dir/", "dir/", 3},
		{"dir1", "dir1", 2},
		{"dir1/", "dir1/", 2},
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for idx, test := range tests {
		p := fmt.Sprintf("%s/%s", prefixDir, test.prefix)
		tutils.Logf("%d. Prefix: %s [%s]\n", idx, test.title, p)
		var msg = &cmn.GetMsg{GetPrefix: p}
		objList, err := api.ListBucket(baseParams, clibucket, msg, 0)
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		if len(objList.Entries) != test.objCount {
			t.Errorf("Expected number of objects with prefix '%s' is %d but found %d",
				test.prefix, test.objCount, len(objList.Entries))
			tutils.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tutils.Logf("    %d[%d]. %s\n", idx, id, oo.Name)
			}
		}
	}
}

func prefixLookup(t *testing.T, proxyURL string) {
	if prefix == "" {
		prefixLookupDefault(t, proxyURL)
		prefixLookupCornerCases(t, proxyURL)
	} else {
		prefixLookupOne(t, proxyURL)
	}
}

func prefixCleanup(t *testing.T, proxyURL string) {
	errCh := make(chan error, numfiles)
	var wg = &sync.WaitGroup{}

	for _, fileName := range fileNames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)
		wg.Add(1)
		go tutils.Del(proxyURL, clibucket, keyName, wg, errCh, true)
	}
	wg.Wait()

	select {
	case e := <-errCh:
		tutils.Logf("Failed to DEL: %s\n", e)
		t.Fail()
	default:
	}
}
