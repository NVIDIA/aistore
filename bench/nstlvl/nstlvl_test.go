// Package nstlvl is intended to measure impact (or lack of thereof) of POSIX directory nesting on random read performance.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package nstlvl

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/trand"
)

// run with all defaults:
// $ go test -v -bench=.
//
// generate and random-read 300K 4K-size files:
// $ go test -v -bench=. -size=4096 -num=300000
//
// generate 8K files under `/ais/mpath` and run for at least 1m (to increase the number of iterations)
// $ go test -v -bench=. -size=8192 -dir=/ais/mpath -benchtime=1m
//
// print usage and exit:
// $ go test -bench=. -usage

const (
	fileNameLen = 15
	dirs        = "/aaaaaa/bbbbbb/cccccc/dddddd/eeeeee/ffffff/gggggg/hhhhhh/iiiiii/jjjjjj"
	dirNameLen  = 6 // NOTE: must reflect the length of dirs' dirs
	skipModulo  = 967
)

type benchContext struct {
	// internal
	rnd       *rand.Rand
	fileNames []string

	// command line
	level     int
	fileSize  int64
	fileCount int
	skipMod   int
	dir       string
	help      bool
}

var benchCtx benchContext

func init() {
	flag.IntVar(&benchCtx.level, "level", 2, "initial (mountpath) nesting level")
	flag.IntVar(&benchCtx.fileCount, "num", 1000, "number of files to generate (and then randomly read)")
	flag.Int64Var(&benchCtx.fileSize, "size", cos.KiB, "file/object size")
	flag.StringVar(&benchCtx.dir, "dir", "/tmp", "top directory for generated files")
	flag.BoolVar(&benchCtx.help, "usage", false, "show command-line options")
}

func (bctx *benchContext) init() {
	flag.Parse()
	if bctx.help {
		flag.Usage()
		os.Exit(0)
	}
	if bctx.fileCount < 10 {
		fmt.Printf("Error: number of files (%d) must be greater than 10 (see README for usage)\n", bctx.fileCount)
		os.Exit(2)
	}
	fmt.Printf("files: %d size: %d\n", bctx.fileCount, bctx.fileSize)
	bctx.fileNames = make([]string, 0, bctx.fileCount)
	bctx.rnd = cos.NowRand()
	bctx.skipMod = skipModulo
	if bctx.fileCount < skipModulo {
		bctx.skipMod = bctx.fileCount/2 - 1
	}
}

func BenchmarkNestedLevel(b *testing.B) {
	benchCtx.init()
	for _, extraDepth := range []int{0, 2, 4, 6} {
		nestedLvl := benchCtx.level + extraDepth

		benchCtx.createFiles(nestedLvl)
		b.Run(strconv.Itoa(nestedLvl), benchNestedLevel)
		benchCtx.removeFiles()
	}
}

func benchNestedLevel(b *testing.B) {
	for i, j, k := 0, 0, 0; i < b.N; i, j = i+1, j+benchCtx.skipMod {
		if j >= benchCtx.fileCount {
			k++
			if k >= benchCtx.skipMod {
				k = 0
			}
			j = k
		}
		fqn := benchCtx.fileNames[j]
		file, err := os.Open(fqn)
		cos.AssertNoErr(err)
		cos.DrainReader(file)
		cos.Close(file)
	}
}

func (bctx *benchContext) createFiles(lvl int) {
	var (
		reader = &io.LimitedReader{R: bctx.rnd, N: bctx.fileSize}
		buf    = make([]byte, 32*cos.KiB)
	)
	for i := 0; i < bctx.fileCount; i++ {
		fileName := bctx.dir + dirs[:lvl*(dirNameLen+1)+1] + bctx.randNestName()
		file, err := cos.CreateFile(fileName)
		cos.AssertNoErr(err)
		_, err = io.CopyBuffer(file, reader, buf)
		cos.AssertNoErr(err)
		err = file.Close()
		cos.AssertNoErr(err)

		reader.N = bctx.fileSize
		bctx.fileNames = append(bctx.fileNames, fileName)
	}

	cmd := exec.Command("sync")
	_, err := cmd.Output()
	cos.AssertNoErr(err)
	time.Sleep(time.Second)

	dropCaches()
	runtime.GC()
	time.Sleep(time.Second)
}

func (bctx *benchContext) removeFiles() {
	err := os.RemoveAll(bctx.dir + dirs[:dirNameLen+1])
	cos.AssertNoErr(err)
	bctx.fileNames = bctx.fileNames[:0]
}

func (*benchContext) randNestName() string {
	return trand.String(fileNameLen)
}
