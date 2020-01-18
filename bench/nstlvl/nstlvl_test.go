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

var nst = struct {
	// const
	fnl int
	lll int
	ddd string
	// internal
	rnd *rand.Rand
	lst []string
	buf []byte
	prm []int
	// command line
	size  int64
	dir   string
	num   int
	level int
	help  bool
}{}

func init() {
	nst.fnl = 13 // filename length
	nst.lll = len(cmn.LetterBytes)
	nst.ddd = "/a/b/c/d/e/f/g/h/i/j"

	flag.IntVar(&nst.num, "num", 1000, "number of files to generate (and then randomly read)")
	flag.IntVar(&nst.level, "level", 2, "initial (mountpath) nesting level")
	flag.Int64Var(&nst.size, "size", cmn.KiB, "file/object size")
	flag.StringVar(&nst.dir, "dir", "/tmp", "top directory for generated files")
	flag.BoolVar(&nst.help, "usage", false, "show command-line options")

	nst.rnd = cmn.NowRand()
	nst.buf = make([]byte, 32*cmn.KiB)
}

func nstInit() {
	if nst.lst == nil {
		flag.Parse()
		if nst.help {
			flag.Usage()
			os.Exit(0)
		}
		fmt.Println("files:", nst.num, "size:", nst.size)
		nst.lst = make([]string, 0, nst.num)
		nst.prm = nst.rnd.Perm(nst.num)
	}
}

func BenchmarkNestLevel(b *testing.B) {
	nstInit()
	for _, extraDepth := range []int{2, 4, 6} {
		createFiles(nst.level+extraDepth, nst.num)
		b.Run(strconv.Itoa(nst.level+extraDepth), benchNest)
		removeFiles()
	}
}

func benchNest(b *testing.B) {
	var j int
	for i := 0; i < b.N; i++ {
		j++
		if j >= nst.num {
			b.StopTimer()                   // pause
			nst.prm = nst.rnd.Perm(nst.num) // permutate
			b.StartTimer()                  // resume
			j = 0
		}
		fqn := nst.lst[nst.prm[j]]
		file, err := os.Open(fqn)
		cmn.AssertNoErr(err)
		_, err = io.Copy(ioutil.Discard, file)
		cmn.AssertNoErr(err)
		_ = file.Close()
	}
}

func createFiles(lvl, num int) {
	reader := &io.LimitedReader{R: nst.rnd, N: nst.size}
	for i := 0; i < num; i++ {
		fn := nst.dir + nst.ddd[0:lvl*2+1] + randNestName()
		file, err := cmn.CreateFile(fn)
		cmn.AssertNoErr(err)
		_, err = io.CopyBuffer(file, reader, nst.buf)
		cmn.AssertNoErr(err)

		_ = file.Close()
		reader.N = nst.size
		nst.lst = append(nst.lst, fn)
	}
}

func removeFiles() {
	err := os.RemoveAll(nst.dir + nst.ddd[:2])
	cmn.AssertNoErr(err)
	nst.lst = nst.lst[:0]
}

func randNestName() string {
	bytes := nst.buf[:nst.fnl]
	nst.rnd.Read(bytes)
	for i, b := range bytes {
		bytes[i] = cmn.LetterBytes[b%byte(nst.lll)]
	}
	return string(bytes)
}
