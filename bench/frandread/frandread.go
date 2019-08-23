// Package frandread is a file-reading benchmark that makes a special effort to visit the files randomly and equally.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type (
	cliVars struct {
		fileList     string        // name of the file that contains filenames to read
		dirs         string        // comma-separated list of directories to read
		pattern      string        // filename matching wildcard; used when reading directories ind gnored with -l
		seed         int64         // random seed; current time (nanoseconds) if omitted
		maxTime      time.Duration // max time to run (run forever if both -t and -e (epochs) are not defined
		numWorkers   int           // number of concurrently reading goroutines (workers)
		numEpochs    uint          // number of "epochs" whereby each epoch entails a full random pass through all filenames
		verbose      bool          // verbose output
		superVerbose bool          // super-verbose output
		usage        bool          // print usage and exit
	}
	statsVars struct {
		sizeEpoch int64
		sizeTotal int64
	}
)

var (
	cliv  = &cliVars{}
	stats = &statsVars{}
)

func main() {
	flag.StringVar(&cliv.fileList, "l", "files.txt", "name of the file that lists filenames to read")
	flag.StringVar(&cliv.dirs, "d", "", "comma-separated list of directories to read (an alternative to list (-l) option)")
	flag.StringVar(&cliv.pattern, "p", "", "filename matching wildcard when reading directories (ignored when -l is used)")
	flag.Int64Var(&cliv.seed, "s", 0, "random seed; current time (nanoseconds) if omitted")
	flag.DurationVar(&cliv.maxTime, "t", 0, "max time to run (run forever if both -t and -e (epochs) are not defined)")
	flag.IntVar(&cliv.numWorkers, "w", 8, "number of concurrently reading goroutines (workers)")
	flag.UintVar(&cliv.numEpochs, "e", 0, "number of \"epochs\" to run whereby each epoch entails a full random pass through all filenames")
	flag.BoolVar(&cliv.verbose, "v", false, "verbose output")
	flag.BoolVar(&cliv.superVerbose, "vv", false, "super-verbose output")
	flag.BoolVar(&cliv.usage, "h", false, "print usage and exit")
	flag.Parse()

	if cliv.usage {
		flag.Usage()
		fmt.Println("Build:")
		fmt.Println("# go build -o frandread")
		fmt.Println("Examples:")
		fmt.Printf("# frandread -h\t\t\t\t\t- show usage\n")
		fmt.Printf("# frandread -d /tmp/work -v -t 10m\t\t- read from /tmp/work, run for 10 minutes\n")
		fmt.Printf("# frandread -d /tmp/work -vv -t 10m -p *.tgz\t- super-verbose and filter by tgz extension\n")
		fmt.Printf("# frandread -d /tmp/a,/tmp/work/b -e 999\t- read two directories, run for 999 epochs\n")

		os.Exit(0)
	}
	if cliv.fileList != "files.txt" && cliv.dirs != "" {
		panic("invalid command-line: -l and -d cannot be used together")
	}
	if cliv.numEpochs == 0 {
		cliv.numEpochs = math.MaxUint32
	}
	if cliv.superVerbose {
		cliv.verbose = true
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	// 1. open and read prepared list of file names
	fileNames := make([]string, 0, 1024)
	if cliv.dirs != "" {
		dirs := strings.Split(cliv.dirs, ",")
		for _, dir := range dirs {
			fileNames = filenamesFromDir(dir, fileNames)
		}
	} else {
		fileNames = filenamesFromList(fileNames)
	}

	// 2. read them all at a given concurrency
	now := time.Now()
	fmt.Printf("Starting to run: %d filenames, %d workers\n", len(fileNames), cliv.numWorkers)

	sema := make(chan struct{}, cliv.numWorkers)
	wg := &sync.WaitGroup{}
	seed := cliv.seed
	if seed == 0 {
		seed = now.UnixNano()
	}
	rnd := rand.New(rand.NewSource(seed))
	en := uint(1)
ml:
	for ; en <= cliv.numEpochs; en++ {
		perm := rnd.Perm(len(fileNames))
		started := time.Now()
		epoch(fileNames, perm, wg, sema) // do the work
		wg.Wait()
		epochWritten := atomic.LoadInt64(&stats.sizeEpoch)
		atomic.AddInt64(&stats.sizeTotal, epochWritten)
		if cliv.verbose {
			mbs := float64(epochWritten) / 1024 / 1024
			elp := time.Since(started)
			thr := float64(0)
			if elp > 0 {
				thr = mbs * float64(time.Second) / float64(elp)
			}
			fmt.Printf("Epoch #%d:\t%.2fMB/s\n", en, thr)
		}
		if cliv.maxTime != 0 {
			if time.Since(now) > cliv.maxTime {
				break
			}
		}
		select {
		case <-sigCh:
			break ml
		default:
			break
		}
	}

	mbs := float64(atomic.LoadInt64(&stats.sizeTotal)) / 1024 / 1024
	elp := time.Since(now)
	thr := float64(0)
	if elp > 0 {
		thr = mbs * float64(time.Second) / float64(elp)
	}
	fmt.Printf("\nEpochs: %d Run Time: %v Average Throughput: %.2fMB/s\n", en, elp, thr)
}

func epoch(fileNames []string, perm []int, wg *sync.WaitGroup, sema chan struct{}) {
	atomic.StoreInt64(&stats.sizeEpoch, 0)
	for _, idx := range perm {
		fname := fileNames[idx]
		wg.Add(1)
		sema <- struct{}{}
		go func(fn string) {
			file, err := os.Open(fn)
			if err != nil {
				panic(err)
			}
			written, err := io.Copy(ioutil.Discard, file) // drain the reader
			if err != nil {
				panic(err)
			}
			atomic.AddInt64(&stats.sizeEpoch, written)
			file.Close()
			if cliv.superVerbose {
				fmt.Println("\t", fn)
			}
			<-sema
			wg.Done()
		}(fname)
	}
}

func filenamesFromList(fileNames []string) []string {
	list, err := os.Open(cliv.fileList)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(list)
	for scanner.Scan() {
		fileNames = append(fileNames, scanner.Text())
	}
	list.Close()
	return fileNames
}

func filenamesFromDir(dir string, fileNames []string) []string {
	dentries, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, finfo := range dentries {
		if finfo.IsDir() {
			continue
		}
		if cliv.pattern != "" {
			if matched, _ := filepath.Match(cliv.pattern, filepath.Base(finfo.Name())); !matched {
				continue
			}
		}
		fname := filepath.Join(dir, finfo.Name())
		fileNames = append(fileNames, fname)
	}
	return fileNames
}
