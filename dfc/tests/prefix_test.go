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
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/pkg/client/readers"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
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
func Test_prefix(t *testing.T) {
	if err := client.Tcping(proxyurl); err != nil {
		tlogf("%s: %v\n", proxyurl, err)
		os.Exit(1)
	}

	fmt.Printf("Looking for files with prefix [%s]\n", prefix)
	prefixFileNumber = numfiles
	prefixCreateFiles(t)
	prefixLookup(t)
	prefixCleanup(t)
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

func prefixCreateFiles(t *testing.T) {
	src := rand.NewSource(baseseed + 1000)
	random := rand.New(src)
	fileNames = make([]string, 0, prefixFileNumber)
	errch := make(chan error, numfiles)
	wg := &sync.WaitGroup{}

	for i := 0; i < prefixFileNumber; i++ {
		fileName := client.FastRandomFilename(random, fnlen)
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)

		// Note: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader
		r, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go client.PutAsync(wg, proxyurl, r, clibucket, keyName, errch, false /* silent */)
		fileNames = append(fileNames, fileName)
	}

	// create specific files to test corner cases
	extranames := []string{"dir/obj01", "dir/obj02", "dir/obj03", "dir1/dir2/obj04", "dir1/dir2/obj05"}
	for _, fName := range extranames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fName)
		// Note: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader
		r, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go client.PutAsync(wg, proxyurl, r, clibucket, keyName, errch, false /* silent */)
		fileNames = append(fileNames, fName)
	}

	wg.Wait()

	select {
	case e := <-errch:
		fmt.Printf("Failed to PUT: %s\n", e)
		t.Fail()
	default:
	}
}

func prefixLookupOne(t *testing.T) {
	fmt.Printf("Looking up for files than names start with %s\n", prefix)
	var msg = &dfc.GetMsg{GetPrefix: prefix}
	numFiles := 0
	objList, err := client.ListBucket(proxyurl, clibucket, msg, 0)
	if testfail(err, "List files with prefix failed", nil, nil, t) {
		return
	}

	for _, entry := range objList.Entries {
		tlogf("Found object: %s\n", entry.Name)
		numFiles++
	}

	realNumFiles := numberOfFilesWithPrefix(fileNames, prefix, prefixDir)
	if realNumFiles == numFiles {
		fmt.Printf("Total files with prefix found: %v\n", numFiles)
	} else {
		t.Errorf("Expected number of files with prefix '%s' is %v but found %v files", prefix, realNumFiles, numFiles)
	}
}

func prefixLookupDefault(t *testing.T) {
	fmt.Printf("Looking up for files in alphabetic order\n")

	letters := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < len(letters); i++ {
		key := letters[i : i+1]
		lookFor := fmt.Sprintf("%s/%s", prefixDir, key)
		var msg = &dfc.GetMsg{GetPrefix: lookFor}
		objList, err := client.ListBucket(proxyurl, clibucket, msg, 0)
		if testfail(err, "List files with prefix failed", nil, nil, t) {
			return
		}

		numFiles := len(objList.Entries)
		realNumFiles := numberOfFilesWithPrefix(fileNames, key, prefix)

		if numFiles == realNumFiles {
			if numFiles != 0 {
				fmt.Printf("Found %v files starting with '%s'\n", numFiles, key)
			}
		} else {
			t.Errorf("Expected number of files with prefix '%s' is %v but found %v files", key, realNumFiles, numFiles)
			tlogf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tlogf("    %d[%d]. %s\n", i, id, oo.Name)
			}
		}
	}
}

func prefixLookupCornerCases(t *testing.T) {
	fmt.Printf("Testing corner cases\n")

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

	for idx, test := range tests {
		p := fmt.Sprintf("%s/%s", prefixDir, test.prefix)
		tlogf("%d. Prefix: %s [%s]\n", idx, test.title, p)
		var msg = &dfc.GetMsg{GetPrefix: p}
		objList, err := client.ListBucket(proxyurl, clibucket, msg, 0)
		if testfail(err, "List files with prefix failed", nil, nil, t) {
			return
		}

		if len(objList.Entries) != test.objCount {
			t.Errorf("Expected number of objects with prefix '%s' is %d but found %d",
				test.prefix, test.objCount, len(objList.Entries))
			tlogf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tlogf("    %d[%d]. %s\n", idx, id, oo.Name)
			}
		}
	}
}

func prefixLookup(t *testing.T) {
	if prefix == "" {
		prefixLookupDefault(t)
		prefixLookupCornerCases(t)
	} else {
		prefixLookupOne(t)
	}
}

func prefixCleanup(t *testing.T) {
	errch := make(chan error, numfiles)
	var wg = &sync.WaitGroup{}

	for _, fileName := range fileNames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)
		wg.Add(1)
		go client.Del(proxyurl, clibucket, keyName, wg, errch, true)
	}
	wg.Wait()

	select {
	case e := <-errch:
		fmt.Printf("Failed to DEL: %s\n", e)
		t.Fail()
	default:
	}
}
