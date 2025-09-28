// Package frandread is a file-reading benchmark that makes a special effort to visit the files randomly and equally.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
)

type (
	cliVars struct {
		fileList   string        // name of the file that contains filenames to read
		dirs       string        // comma-separated list of directories to read
		pattern    string        // filename matching wildcard; used when reading directories ind gnored with -l
		pctPut     int           // percentage of PUTs in the generated workload
		minSize    int           // minimum size of the object which will be created during PUT
		maxSize    int           // maximum size of the object which will be created during PUT
		seed       int64         // random seed; current time (nanoseconds) if omitted
		maxTime    time.Duration // max time to run (run forever if both -t and -e (epochs) are not defined
		numWorkers int           // number of concurrently reading goroutines (workers)
		numEpochs  uint          // number of "epochs" whereby each epoch entails a full random pass through all filenames
		verbose    bool          // super-verbose output
		usage      bool          // print usage and exit
	}
	statsVars struct {
		sizeEpoch int64
		sizeTotal int64
		timeTotal time.Duration
	}

	bench struct {
		semaCh chan struct{}
		wg     *sync.WaitGroup
		rnd    *rand.Rand

		fileNames []string
		perm      []int
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
	flag.IntVar(&cliv.pctPut, "pctput", 0, "percentage of PUTs in the generated workload")
	flag.IntVar(&cliv.minSize, "minsize", 1024, "minimum size of the object which will be created during PUT")
	flag.IntVar(&cliv.maxSize, "maxsize", 10*1024*1024, "maximum size of the object which will be created during PUT")
	flag.Int64Var(&cliv.seed, "s", 0, "random seed; current time (nanoseconds) if omitted")
	flag.DurationVar(&cliv.maxTime, "t", 0, "max time to run (run forever if both -t and -e (epochs) are not defined)")
	flag.IntVar(&cliv.numWorkers, "w", 8, "number of concurrently reading goroutines (workers)")
	flag.UintVar(&cliv.numEpochs, "e", 0, "number of \"epochs\" to run whereby each epoch entails a full random pass through all filenames")
	flag.BoolVar(&cliv.verbose, "v", false, "verbose output")
	flag.BoolVar(&cliv.usage, "h", false, "print usage and exit")
	flag.Parse()

	if cliv.usage || len(os.Args[1:]) == 0 {
		flag.Usage()
		fmt.Println("Build:")
		fmt.Println("\tgo install frandread.go")
		fmt.Println("Examples:")
		fmt.Print("\tfrandread -h\t\t\t\t\t- show usage\n")
		fmt.Print("\tfrandread -d /tmp/work -t 10m\t\t\t- read from /tmp/work, run for 10 minutes\n")
		fmt.Print("\tfrandread -d /tmp/work -v -t 10m -p *.tgz\t- filter by tgz extension\n")
		fmt.Print("\tfrandread -d /tmp/a,/tmp/work/b -e 999\t\t- read two directories, run for 999 epochs\n")
		fmt.Print("\tfrandread -d ~/smth -pctput 1\t\t\t- put files into ~/smth directory")
		fmt.Println()
		os.Exit(0)
	}
	if cliv.fileList != "files.txt" && cliv.dirs != "" {
		panic("invalid command-line: -l and -d cannot be used together")
	}
	if cliv.numEpochs == 0 {
		cliv.numEpochs = math.MaxUint32
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)

	fileNames := make([]string, 0, 1024)
	if cliv.pctPut == 0 {
		// 1. open and read prepared list of file names
		if cliv.dirs != "" {
			dirs := strings.Split(cliv.dirs, ",")
			for _, dir := range dirs {
				fileNames = fileNamesFromDir(dir, fileNames)
			}
		} else {
			fileNames = fileNamesFromList(fileNames)
		}
	} else {
		if cliv.dirs == "" {
			panic("In PUT mode one needs to specify directory to which files will be written")
		}

		for range 1024 {
			fileNames = append(fileNames, randString(10))
		}
	}

	// 2. read them all at a given concurrency
	now := time.Now()
	fmt.Printf("Starting to run: %d filenames, %d workers\n", len(fileNames), cliv.numWorkers)

	b := newBench(fileNames)

	en := uint(1)
ml:
	for ; en <= cliv.numEpochs; en++ {
		b.reset()
		started := time.Now()
		b.epoch()

		epochWritten := atomic.LoadInt64(&stats.sizeEpoch)
		stats.sizeTotal += epochWritten
		epochTime := time.Since(started)
		stats.timeTotal += epochTime

		b.cleanup()

		sthr := formatThroughput(epochWritten, epochTime)
		fmt.Printf("Epoch #%d:\t%s\n", en, sthr)

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
	elapsed := time.Since(now)
	sthr := formatThroughput(stats.sizeTotal, stats.timeTotal) // total-bytes / total-effective-time
	fmt.Println("ok", elapsed)
	fmt.Printf("%-12s%-18s%-30s\n", "Epochs", "Time", "Average Throughput")
	fmt.Printf("%-12d%-18v%-30s\n", en, stats.timeTotal, sthr)
}

func newBench(fileNames []string) *bench {
	if cliv.seed == 0 {
		cliv.seed = mono.NanoTime()
	}
	rnd := rand.New(cos.NewRandSource(uint64(cliv.seed)))
	return &bench{
		rnd:    rnd,
		semaCh: make(chan struct{}, cliv.numWorkers),
		wg:     &sync.WaitGroup{},

		fileNames: fileNames,
	}
}

func formatThroughput(bytes int64, duration time.Duration) (sthr string) {
	var (
		gbs    float64
		mbs    = float64(bytes) / 1024 / 1024
		suffix = "MiB/s"
		thr    = mbs * float64(time.Second) / float64(duration)
	)
	if duration == 0 {
		return "-"
	}
	if thr > 1024 {
		gbs = float64(bytes) / 1024 / 1024 / 1024
		suffix = "GiB/s"
		thr = gbs * float64(time.Second) / float64(duration)
	}
	sthr = fmt.Sprintf("%.3f%s", thr, suffix)
	return
}

func (b *bench) reset() {
	b.perm = b.rnd.Perm(len(b.fileNames))
}

func (b *bench) epoch() {
	atomic.StoreInt64(&stats.sizeEpoch, 0)
	for _, idx := range b.perm {
		fname := b.fileNames[idx]
		b.wg.Add(1)
		b.semaCh <- struct{}{}
		go func(fname string) {
			defer func() {
				<-b.semaCh
				b.wg.Done()
			}()

			if cliv.pctPut > 0 {
				// PUT
				f, err := os.Create(filepath.Join(cliv.dirs, fname))
				if err != nil {
					panic(err)
				}

				size := b.rnd.IntN(cliv.maxSize-cliv.minSize) + cliv.minSize
				r := io.LimitReader(&nopReadCloser{}, int64(size))
				written, err := io.Copy(f, r)
				if err != nil {
					panic(err)
				}
				atomic.AddInt64(&stats.sizeEpoch, written)

				f.Close()
			} else {
				// GET
				f, err := os.Open(fname)
				if err != nil {
					panic(err)
				}
				read, err := io.Copy(io.Discard, f) // drain the reader
				if err != nil {
					panic(err)
				}
				atomic.AddInt64(&stats.sizeEpoch, read)
				f.Close()
			}

			if cliv.verbose {
				fmt.Println("\t", fname)
			}
		}(fname)
	}

	b.wg.Wait()
}

func (b *bench) cleanup() {
	if cliv.pctPut > 0 {
		for _, fname := range b.fileNames {
			os.Remove(filepath.Join(cliv.dirs, fname))
		}
	}
}

func fileNamesFromList(fileNames []string) []string {
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

func fileNamesFromDir(dir string, fileNames []string) []string {
	dentries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, dent := range dentries {
		if dent.IsDir() || !dent.Type().IsRegular() {
			continue
		}
		if cliv.pattern != "" {
			if matched, _ := filepath.Match(cliv.pattern, filepath.Base(dent.Name())); !matched {
				continue
			}
		}
		fname := filepath.Join(dir, dent.Name())
		fileNames = append(fileNames, fname)
	}
	return fileNames
}

func randString(n int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.IntN(len(letterRunes))]
	}
	return string(b)
}

type nopReadCloser struct{}

func (*nopReadCloser) Read(p []byte) (n int, err error) { return len(p), nil }
func (*nopReadCloser) Close() error                     { return nil }
