// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
//
// Example run:
// 	go test -v -run=rwstress -args -numfiles=10 -cycles=10 -nodel
//
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
)

const (
	rwdir    = "rwstress"
	fileSize = 1024 * 32 // file size

	// time to sleep if there is no object created yet in milliseconds
	// del time is slightly greater than get one to allow get work faster
	//   than del, so get won't miss objects because del run before get
	getSleep = 5
	delSleep = 10

	FileCreated = true
	FileExists  = true
	FileDeleted = false
	RunNormal   = false
	RunCleanUp  = true
)

type fileLock struct {
	locked bool
	exists bool
}

var (
	cycles  int
	skipdel bool

	rwstressMtx sync.Mutex
	fileLocks   []fileLock
	fileNames   []string
	buf         []byte

	numLoops int
	numFiles int
)

// default number of files is 100, default number of loops to write files is 15
// it results in 230-260 seconds long test
func init() {
	flag.BoolVar(&skipdel, "nodel", false, "Run only PUT and GET in a loop and do cleanup once at the end")
	flag.IntVar(&cycles, "cycles", 15, "Number of PUT cycles")
}

func tryLockFile(idx int) bool {
	rwstressMtx.Lock()
	defer rwstressMtx.Unlock()

	info := fileLocks[idx]
	if info.locked {
		return false
	}

	fileLocks[idx].locked = true
	return true
}

// tryLockNextAvailFile looks for an unlocked file that exists. If such file
// found it returns the id of the file and true. Returns 0 and false otherwise.
// idx is the preferred file id - a starting point to look for a file
func tryLockNextAvailFile(idx int) (int, bool) {
	rwstressMtx.Lock()
	defer rwstressMtx.Unlock()

	info := fileLocks[idx]
	if !info.locked && info.exists {
		fileLocks[idx].locked = true
		return idx, true
	}

	nextIdx := idx + 1
	for nextIdx != idx {
		if nextIdx >= len(fileNames) {
			nextIdx = 0
			continue
		}

		info = fileLocks[nextIdx]
		if !info.locked && info.exists {
			fileLocks[nextIdx].locked = true
			return nextIdx, true
		}

		nextIdx++
	}

	return 0, false
}

// tryUnlockFile unlocks the file and marks if the file exists or not
func tryUnlockFile(idx int, fileExists bool) bool {
	rwstressMtx.Lock()
	defer rwstressMtx.Unlock()

	fileLocks[idx].locked = false
	fileLocks[idx].exists = fileExists
	return true
}

// generates a list of random file names and a buffer to keep random data for filling up files
func generateRandomData(t *testing.T, seed int64, fileCount int) {
	src := rand.NewSource(seed)
	random := rand.New(src)
	fileNames = make([]string, fileCount)

	for i := 0; i < fileCount; i++ {
		fileNames[i] = fastRandomFilename(random)
	}

	buf = make([]byte, blocksize)
}

func rwPutLoop(t *testing.T, fileNames []string, taskGrp *sync.WaitGroup, doneCh chan int, buf []byte) {
	errch := make(chan error, 10)
	fileCount := len(fileNames)
	var putMissed, totalOps int

	src := rand.NewSource(time.Now().UTC().UnixNano())
	random := rand.New(src)

	for i := 0; i < numLoops; i++ {
		for i := 0; i < fileCount; i++ {
			keyname := fmt.Sprintf("%s/%s", rwdir, fileNames[i])
			fname := fmt.Sprintf("%s/%s", baseDir, keyname)

			if _, err := writeRandomData(fname, buf, int(fileSize), random); err != nil {
				fmt.Fprintf(os.Stdout, "PUT write FAIL: %v\n", err)
				t.Error(err)
				if errch != nil {
					errch <- err
				}
				return
			}
			if ok := tryLockFile(i); ok {
				put(fname, clibucket, keyname, nil, errch)
				tryUnlockFile(i, FileCreated)
				totalOps++
			} else {
				putMissed++
			}

			select {
			case e := <-errch:
				fmt.Fprintf(os.Stdout, "PUT FAIL: %s\n", e)
				t.Fail()
			default:
			}
		}
	}

	// emit signals for DEL and GET loops
	doneCh <- 1
	if !skipdel {
		doneCh <- 1
	}

	if taskGrp != nil {
		taskGrp.Done()
	}

	fmt.Fprintf(os.Stdout, "PUT total %d [missed PUT %d]\n", totalOps, putMissed)
}

func rwDelLoop(t *testing.T, fileNames []string, taskGrp *sync.WaitGroup, doneCh chan int, doCleanUp bool) {
	done := false
	var delMissed, totalOps, currIdx int
	errch := make(chan error, 10)

	for !done {
		if idx, ok := tryLockNextAvailFile(currIdx); ok {
			keyname := fmt.Sprintf("%s/%s", rwdir, fileNames[idx])
			del(clibucket, keyname, nil, errch)
			tryUnlockFile(idx, FileDeleted)

			currIdx = idx + 1
			if currIdx >= len(fileNames) {
				currIdx = 0
			}
			totalOps++
		} else {
			if doCleanUp {
				fmt.Printf("Cleanup finished\n")
				break
			}
			delMissed++
			time.Sleep(delSleep * time.Millisecond)
		}

		select {
		case <-doneCh:
			done = true
		default:
		}
	}

	if taskGrp != nil {
		taskGrp.Done()
	}

	if doCleanUp {
		fmt.Fprintf(os.Stdout, "DEL cleaned up %d files\n", totalOps)
	} else {
		fmt.Fprintf(os.Stdout, "DEL %d files [missed DEL %d]\n", totalOps, delMissed)
	}
}

func rwGetLoop(t *testing.T, fileNames []string, taskGrp *sync.WaitGroup, doneCh chan int) {
	done := false
	var currIdx, getMissed, totalOps int
	errch := make(chan error, 10)

	for !done {
		if idx, ok := tryLockNextAvailFile(currIdx); ok {
			keyname := fmt.Sprintf("%s/%s", rwdir, fileNames[idx])
			get(keyname, nil, errch, clibucket)
			tryUnlockFile(idx, FileExists)

			currIdx = idx + 1
			if currIdx >= len(fileNames) {
				currIdx = 0
			}
			totalOps++
		} else {
			getMissed++
			time.Sleep(getSleep * time.Millisecond)
		}

		select {
		case <-doneCh:
			done = true
		default:
		}
	}

	if taskGrp != nil {
		taskGrp.Done()
	}

	fmt.Fprintf(os.Stdout, "GET %d files [missed GET %d]\n", totalOps, getMissed)
}

func rwstress(t *testing.T) {
	if err := dfc.CreateDir(fmt.Sprintf("%s/%s", baseDir, rwdir)); err != nil {
		t.Fatalf("Failed to create dir %s/%s, err: %v", baseDir, rwdir, err)
	}

	fileLocks = make([]fileLock, numFiles, numFiles)

	generateRandomData(t, baseseed+10000, numFiles)
	fmt.Printf("PUT files: %v x %v times\n", len(fileNames), numLoops)

	var wg = &sync.WaitGroup{}

	doneCh := make(chan int, 2)
	wg.Add(1)
	go rwPutLoop(t, fileNames, wg, doneCh, buf)
	wg.Add(1)
	go rwGetLoop(t, fileNames, wg, doneCh)
	if skipdel {
		wg.Wait()
	} else {
		wg.Add(1)
		go rwDelLoop(t, fileNames, wg, doneCh, RunNormal)
		wg.Wait()
	}

	fmt.Fprintf(os.Stdout, "Cleaning up...\n")
	rwDelLoop(t, fileNames, nil, doneCh, RunCleanUp)

	rwstress_cleanup(t)
}

func rwstress_cleanup(t *testing.T) {
	fileDir := fmt.Sprintf("%s/%s", baseDir, rwdir)
	e := os.RemoveAll(fileDir)
	if e != nil {
		fmt.Printf("Failed to remove directory %s: %v\n", fileDir, e)
		t.Error(e)
	}
}

// The regression verions of the test should run around 30 seconds
// 25 files written 8 times takes 20-25 seconds
func regressionRWStress(t *testing.T) {
	numFiles = 25
	numLoops = 8

	rwstress(t)
}

// Test_rwstress runs delete, put, and get operations in a loop
// Since PUT is the longest operation, PUT loop runs the defined number
//    of cycles and emits a done signal at the end. Both GET and DEL run
//    endlessly until PUT loop emits the done signal
// If -nodel is on then the test runs only PUT and GET in a loop and after they
//    complete the test runs DEL loop to clean up
// If the test runs asynchronusly all three kinds of operations then after the
//    test finishes it executes extra loop to delete all files
func Test_rwstress(t *testing.T) {
	flag.Parse()
	numLoops = cycles
	numFiles = numfiles

	rwstress(t)
}
