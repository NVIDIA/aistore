/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package nstlvl

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
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
	fileNameLen = 13
	dirs        = "/a/b/c/d/e/f/g/h/i/j"
)

type benchContext struct {
	// internal
	rnd       *rand.Rand
	fileNames []string

	// command line
	level     int
	fileSize  int64
	fileCount int
	dir       string
	help      bool
}

var benchCtx benchContext

func init() {
	flag.IntVar(&benchCtx.level, "level", 2, "initial (mountpath) nesting level")
	flag.IntVar(&benchCtx.fileCount, "num", 1000, "number of files to generate (and then randomly read)")
	flag.Int64Var(&benchCtx.fileSize, "size", cmn.KiB, "file/object size")
	flag.StringVar(&benchCtx.dir, "dir", "/tmp", "top directory for generated files")
	flag.BoolVar(&benchCtx.help, "usage", false, "show command-line options")
}

func (bctx *benchContext) init() {
	flag.Parse()
	if bctx.help {
		flag.Usage()
		os.Exit(0)
	}
	fmt.Printf("files: %d size: %d\n", bctx.fileCount, bctx.fileSize)
	bctx.fileNames = make([]string, 0, bctx.fileCount)
	bctx.rnd = cmn.NowRand()
}

func BenchmarkNestedLevel(b *testing.B) {
	benchCtx.init()
	for _, extraDepth := range []int{2, 4, 6} {
		nestedLvl := benchCtx.level + extraDepth

		benchCtx.createFiles(nestedLvl)
		b.Run(strconv.Itoa(nestedLvl), benchNestedLevel)
		benchCtx.removeFiles()
	}
}

func benchNestedLevel(b *testing.B) {
	var (
		perm = benchCtx.rnd.Perm(benchCtx.fileCount)
	)

	b.ResetTimer()
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j >= benchCtx.fileCount {
			b.StopTimer()
			perm = benchCtx.rnd.Perm(benchCtx.fileCount)
			j = 0
			b.StartTimer()
		}

		fqn := benchCtx.fileNames[perm[j]]
		file, err := os.Open(fqn)
		cmn.AssertNoErr(err)
		_, err = io.Copy(ioutil.Discard, file)
		cmn.AssertNoErr(err)
		_ = file.Close()
	}
}

func (bctx *benchContext) createFiles(lvl int) {
	var (
		reader = &io.LimitedReader{R: bctx.rnd, N: bctx.fileSize}
		buf    = make([]byte, 32*cmn.KiB)
	)

	for i := 0; i < bctx.fileCount; i++ {
		fileName := bctx.dir + dirs[:lvl*2+1] + bctx.randNestName()

		file, err := cmn.CreateFile(fileName)
		cmn.AssertNoErr(err)
		_, err = io.CopyBuffer(file, reader, buf)
		cmn.AssertNoErr(err)
		err = file.Close()
		cmn.AssertNoErr(err)

		reader.N = bctx.fileSize
		bctx.fileNames = append(bctx.fileNames, fileName)
	}
}

func (bctx *benchContext) removeFiles() {
	err := os.RemoveAll(bctx.dir + dirs[:2])
	cmn.AssertNoErr(err)
	bctx.fileNames = bctx.fileNames[:0]
}

func (bctx *benchContext) randNestName() string {
	var bytes [fileNameLen]byte
	bctx.rnd.Read(bytes[:])
	for i, b := range bytes {
		bytes[i] = cmn.LetterBytes[b%byte(len(cmn.LetterBytes))]
	}
	return string(bytes[:])
}
