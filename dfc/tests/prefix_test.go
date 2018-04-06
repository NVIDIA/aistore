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
	"flag"
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
	prefix           string
	prefixFileNumber int
)

func init() {
	flag.StringVar(&prefix, "prefix", "", "Object name prefix")
}

// if the prefix flag is set via command line the test looks only for the prefix
// and checks if the number of items equals the number of files with
// the names starting with the prefix;
// otherwise, the test creates (PUT) random files and executes 'a*' through 'z*' listings.
func Test_prefix(t *testing.T) {
	parse()

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
	objList, err := client.ListBucket(proxyurl, clibucket, msg)
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
		objList, err := client.ListBucket(proxyurl, clibucket, msg)
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
		}
	}
}

func prefixLookup(t *testing.T) {
	if prefix == "" {
		prefixLookupDefault(t)
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
