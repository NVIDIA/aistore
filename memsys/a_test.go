// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

// How to run:
//
// 1) run each of the tests for 2 minutes while redirecting errors to standard error:
//
// go test -v -logtostderr=true -duration 2m
//
// 2) run a given test (name matching "No") with debug enabled:
//
// AIS_DEBUG=memsys=1 go test -v -logtostderr=true -run=No
//
// 3) run each test for 10 minutes with the permission to use up to 90% of total RAM
//
// AIS_MINMEM_PCT_TOTAL=10 AIS_DEBUG=memsys=1 go test -v -logtostderr=true -run=No -duration 10m
//
// 4) same, with both debug and verbose output enabled:
//
// AIS_MINMEM_PCT_TOTAL=10 go test -v -logtostderr=true -run=No -duration 10m -verbose true

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils"
)

var (
	duration time.Duration // test duration
	verbose  bool
)

func init() {
	var (
		d   string
		err error
	)
	flag.StringVar(&d, "duration", "30s", "test duration")
	flag.BoolVar(&verbose, "verbose", false, "verbose")
	flag.Parse()

	if duration, err = time.ParseDuration(d); err != nil {
		cmn.ExitInfof("Invalid duration %q", d)
	}
}

func Test_Sleep(t *testing.T) {
	mem := &memsys.Mem2{TimeIval: time.Second * 20, MinFree: cmn.GiB, Name: "amem", Debug: verbose}
	err := mem.Init(true /* ignore errors */)
	if err != nil {
		t.Fatal(err)
	}
	go mem.Run()

	wg := &sync.WaitGroup{}
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		ttl := time.Duration(random.Int63n(int64(time.Millisecond*100))) + time.Millisecond
		var siz, tot int64
		if i%2 == 0 {
			siz = random.Int63n(cmn.MiB*10) + cmn.KiB
		} else {
			siz = random.Int63n(cmn.KiB*100) + cmn.KiB
		}
		tot = random.Int63n(cmn.DivCeil(cmn.MiB*50, siz))*siz + cmn.KiB
		wg.Add(1)
		go memstress(mem, i, ttl, siz, tot, wg)
	}
	c := make(chan struct{}, 1)
	go printMaxRingLen(mem, c)
	for i := 0; i < 7; i++ {
		time.Sleep(duration / 8)
		mem.Free(memsys.FreeSpec{IdleDuration: 1, MinSize: cmn.MiB})
	}
	wg.Wait()
	close(c)
	mem.Stop(nil)
}

func Test_NoSleep(t *testing.T) {
	mem := &memsys.Mem2{TimeIval: time.Second * 20, MinPctTotal: 5, Name: "bmem", Debug: verbose}
	err := mem.Init(true /* ignore errors */)
	if err != nil {
		t.Fatal(err)
	}
	go mem.Run()
	go printstats(mem)

	wg := &sync.WaitGroup{}
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 500; i++ {
		siz := random.Int63n(cmn.MiB) + cmn.KiB
		tot := random.Int63n(cmn.DivCeil(cmn.MiB*10, siz))*siz + cmn.KiB
		wg.Add(1)
		go memstress(mem, i, time.Millisecond, siz, tot, wg)
	}
	c := make(chan struct{}, 1)
	go printMaxRingLen(mem, c)
	for i := 0; i < 7; i++ {
		time.Sleep(duration / 8)
		mem.Free(memsys.FreeSpec{Totally: true, ToOS: true, MinSize: cmn.MiB * 10})
	}
	wg.Wait()
	close(c)
	mem.Stop(nil)
}

func printMaxRingLen(mem *memsys.Mem2, c chan struct{}) {
	for i := 0; i < 100; i++ {
		select {
		case <-c:
			return
		case <-time.After(5 * time.Second):
			if p := mem.MemPressure(); p > memsys.MemPressureLow {
				tutils.Logf("memory pressure %s\n", memsys.MemPressureText(p))
			}
		}
	}
}

func memstress(mem *memsys.Mem2, id int, ttl time.Duration, siz, tot int64, wg *sync.WaitGroup) {
	defer wg.Done()
	sgls := make([]*memsys.SGL, 0, 128)
	x := cmn.B2S(siz, 1) + "/" + cmn.B2S(tot, 1)
	if id%100 == 0 && verbose {
		if ttl > time.Millisecond {
			tutils.Logf("%4d: %-19s ttl %v\n", id, x, ttl)
		} else {
			tutils.Logf("%4d: %-19s\n", id, x)
		}
	}
	started := time.Now()
	for {
		t := tot
		for t > 0 {
			sgls = append(sgls, mem.NewSGL(siz))
			t -= siz
		}
		time.Sleep(ttl)
		for i, sgl := range sgls {
			sgl.Free()
			sgls[i] = nil
		}
		sgls = sgls[:0]
		if time.Since(started) > duration {
			break
		}
	}
	if id%100 == 0 && verbose {
		tutils.Logf("%4d: done\n", id)
	}
}

func printstats(mem *memsys.Mem2) {
	var (
		prevStats, currStats memsys.Stats2
		req                  = memsys.ReqStats2{Wg: &sync.WaitGroup{}, Stats: &currStats}
		ravghits             = make([]float64, memsys.NumSlabs)
	)
	for {
		time.Sleep(mem.TimeIval)
		req.Wg.Add(1)
		mem.GetStats(req)
		req.Wg.Wait()
		for i := 0; i < memsys.NumSlabs; i++ {
			ftot := float64(currStats.Hits[i])
			if ftot == 0 {
				continue
			}
			if ravghits[i] == 0 {
				ravghits[i] = float64(currStats.Hits[i]) / ftot
			} else {
				x := float64(currStats.Hits[i]) / ftot
				ravghits[i] = ravghits[i]*0.4 + x*0.6
			}
		}
		str := ""
		for i := 0; i < memsys.NumSlabs; i++ {
			slab, err := mem.GetSlab2(int64(i+1) * cmn.KiB * 4)
			if err != nil {
				fmt.Println(err)
				return
			}
			if ravghits[i] < 0.0001 || ravghits[i] > 0.9999 {
				continue
			}
			str += fmt.Sprintf("%s (%.2f) ", slab.Tag(), ravghits[i])
			prevStats.Hits[i] = currStats.Hits[i]
			prevStats.Adeltas[i] = currStats.Adeltas[i]
			prevStats.Idle[i] = currStats.Idle[i]
		}
		if len(str) > 0 {
			fmt.Println(str)
		}
	}
}
