// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

//
// GET from
//

func TestGetFromArchive(t *testing.T) {
	const tmpDir = "/tmp"
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			m = ioContext{
				t:   t,
				bck: bck.Clone(),
			}
			baseParams  = tools.BaseAPIParams(m.proxyURL)
			errCh       = make(chan error, m.num)
			numArchived = 100
			randomNames = make([]string, numArchived)
			subtests    = []struct {
				ext        string // one of archive.FileExtensions
				nested     bool   // subdirs
				autodetect bool   // auto-detect by magic
				mime       bool   // specify mime type
			}{
				{
					ext: archive.ExtTar, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtTarTgz, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtZip, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtTarLz4, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtTar, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtTarTgz, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtZip, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtTarLz4, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtTar, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtTarTgz, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtZip, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtTarLz4, nested: true, autodetect: true, mime: true,
				},
			}
		)
		if testing.Short() {
			numArchived = 10
		}
		for _, test := range subtests {
			tname := fmt.Sprintf("%s/nested=%t/auto=%t/mime=%t", test.ext, test.nested, test.autodetect, test.mime)
			t.Run(tname, func(t *testing.T) {
				var (
					err      error
					fsize    = rand.Intn(10*cos.KiB) + 1
					archName = tmpDir + "/" + cos.GenTie() + test.ext
					dirs     = []string{"a", "b", "c", "a/b", "a/c", "b/c", "a/b/c", "a/c/b", "b/a/c"}
				)
				for i := 0; i < numArchived; i++ {
					j := rand.Int()
					randomNames[i] = fmt.Sprintf("%d.txt", j)
					if test.nested {
						k := j % len(dirs)
						dir := dirs[k]
						randomNames[i] = dir + "/" + randomNames[i]
					}
					if j%3 == 0 {
						randomNames[i] = "/" + randomNames[i]
					}
				}
				if test.ext == archive.ExtZip {
					err = tarch.CreateZipWithRandomFiles(archName, numArchived, fsize, randomNames)
				} else {
					err = tarch.CreateTarWithRandomFiles(
						archName,
						test.ext,
						numArchived,
						fsize,
						false,       // duplication
						nil,         // record extensions
						randomNames, // pregenerated filenames
					)
				}
				tassert.CheckFatal(t, err)
				defer os.Remove(archName)

				objname := filepath.Base(archName)
				if test.autodetect {
					objname = objname[0 : len(objname)-len(test.ext)]
				}
				reader, err := readers.NewFileReaderFromFile(archName, cos.ChecksumNone)
				tassert.CheckFatal(t, err)

				tools.Put(m.proxyURL, m.bck, objname, reader, errCh)
				tassert.SelectErr(t, errCh, "put", true)
				defer tools.Del(m.proxyURL, m.bck, objname, nil, nil, true)

				for _, randomName := range randomNames {
					var mime string
					if test.mime {
						mime = "application/x-" + test.ext[1:]
					}
					getArgs := api.GetArgs{
						Query: url.Values{
							apc.QparamArchpath: []string{randomName},
							apc.QparamArchmime: []string{mime},
						},
					}
					oah, err := api.GetObject(baseParams, m.bck, objname, &getArgs)
					tlog.Logf("%s?%s=%s(%dB)\n", m.bck.Cname(objname), apc.QparamArchpath, randomName, oah.Size())
					tassert.CheckFatal(t, err)
				}
			})
		}
	})
}

// PUT/create
func TestCreateMultiObjArch(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		testMobjArch(t, bck)
	})
}

func testMobjArch(t *testing.T, bck *meta.Bck) {
	var (
		numPuts = 100
		m       = ioContext{
			t:       t,
			bck:     bck.Clone(),
			num:     numPuts,
			prefix:  "archive/",
			ordered: true,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		numArchs   = 15
		numInArch  = cos.Min(m.num/2, 7)
		fmtRange   = "%s{%d..%d}"
		subtests   = []struct {
			ext            string // one of archive.FileExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
			abrt           bool
			apnd           bool
		}{
			{
				ext: archive.ExtTar, list: true,
			},
			{
				ext: archive.ExtTar, list: false, inclSrcBckName: true,
			},
			{
				ext: archive.ExtTar, list: false,
			},
			{
				ext: archive.ExtTar, list: true, apnd: true,
			},
			{
				ext: archive.ExtTarLz4, list: true,
			},
		}
		subtestsLong = []struct {
			ext            string // one of archive.FileExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
			abrt           bool
			apnd           bool
		}{
			{
				ext: archive.ExtTgz, list: true,
			},
			{
				ext: archive.ExtTgz, list: false, inclSrcBckName: true,
			},
			{
				ext: archive.ExtTgz, list: true, inclSrcBckName: true, apnd: true,
			},
			{
				ext: archive.ExtTgz, list: false, apnd: true,
			},
			{
				ext: archive.ExtZip, list: true,
			},
			{
				ext: archive.ExtZip, list: false, inclSrcBckName: true,
			},
			{
				ext: archive.ExtZip, list: true,
			},
			{
				ext: archive.ExtTarLz4, list: false,
			},
		}
	)
	if testing.Short() {
		numArchs = 2
	} else { // test-long
		subtests = append(subtests, subtestsLong...)
	}
	for _, test := range subtests {
		var (
			abrt        string
			listOrRange = "list"
		)
		if !test.list {
			listOrRange = "range"
		}
		tm := mono.NanoTime()
		if tm&0x3 == 0 && !test.apnd {
			test.abrt = true
			abrt = "/abort"
		}
		tname := fmt.Sprintf("%s/%s%s", test.ext, listOrRange, abrt)
		if test.inclSrcBckName {
			tname += "/incl-src=" + m.bck.Name
		}
		if test.apnd {
			tname += "/append"
		}
		t.Run(tname, func(t *testing.T) {
			if m.bck.IsRemote() {
				m.num = numPuts >> 1
			}
			m.initWithCleanup()
			m.fileSize = cos.MinU64(m.fileSize+m.fileSize/3, 32*cos.KiB)
			m.puts()
			if m.bck.IsRemote() {
				defer m.del(-1)
			}
			bckTo := cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
			tools.CreateBucketWithCleanup(t, proxyURL, bckTo, nil)

			if test.list {
				for i := 0; i < numArchs; i++ {
					archName := fmt.Sprintf("test_lst_%02d%s", i, test.ext)
					list := make([]string, 0, numInArch)
					for j := 0; j < numInArch; j++ {
						list = append(list, m.objNames[rand.Intn(m.num)])
					}
					go func(archName string, list []string, i int) {
						msg := cmn.ArchiveMsg{
							ToBck:      bckTo,
							ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
						}
						msg.ListRange.ObjNames = list
						msg.InclSrcBname = test.inclSrcBckName

						xids, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
						tlog.Logf("[%s] %2d: arch list %d objects %s => %s\n", xids, i, len(list), m.bck, bckTo)
					}(archName, list, i)
				}
			} else {
				for i := 0; i < numArchs; i++ {
					archName := fmt.Sprintf("test_rng_%02d%s", i, test.ext)
					start := rand.Intn(m.num - numInArch)
					go func(archName string, start, i int) {
						msg := cmn.ArchiveMsg{
							ToBck:      bckTo,
							ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
						}
						msg.ListRange.Template = fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
						msg.InclSrcBname = test.inclSrcBckName

						xids, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
						tlog.Logf("[%s] %2d: arch range %s %s => %s\n",
							xids, i, msg.ListRange.Template, m.bck, bckTo)
					}(archName, start, i)
				}
			}

			flt := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
			if test.abrt {
				time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)
				tlog.Logln("Aborting...")
				api.AbortXaction(baseParams, flt)
			}

			var lstToAppend *cmn.LsoResult
			for ii := 0; ii < 2; ii++ {
				// TODO: calibrate x-archive reporting 'idle'
				// (too early in presence of multiple concurrent requests)
				time.Sleep(3 * time.Second)
				api.WaitForXactionIdle(baseParams, flt)

				tlog.Logf("List %s\n", bckTo)
				msg := &apc.LsoMsg{Prefix: "test_"}
				msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
				objList, err := api.ListObjects(baseParams, bckTo, msg, 0)
				tassert.CheckFatal(t, err)
				for _, en := range objList.Entries {
					tlog.Logf("%s: %dB\n", en.Name, en.Size)
				}
				num := len(objList.Entries)
				if num < numArchs && ii == 0 {
					tlog.Logf("Warning: expected %d, have %d - retrying...\n", numArchs, num)
					time.Sleep(7 * time.Second) // TODO: ditto
					continue
				}
				tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)
				lstToAppend = objList
				break
			}

			msg := &apc.LsoMsg{Prefix: "test_"}
			msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			msg.SetFlag(apc.LsArchDir)
			objList, err := api.ListObjects(baseParams, bckTo, msg, 0)
			tassert.CheckFatal(t, err)
			num := len(objList.Entries)
			expectedNum := numArchs + numArchs*numInArch

			tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)

			// multi-object APPEND
			if test.apnd {
				for _, e := range lstToAppend.Entries {
					start := rand.Intn(m.num - numInArch)
					go func(archName string, start int) {
						msg := cmn.ArchiveMsg{
							ToBck:      bckTo,
							ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
						}
						msg.ListRange.Template = fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
						msg.InclSrcBname = test.inclSrcBckName

						msg.AppendToExisting = true // here

						xids, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
						tlog.Logf("[%s] APPEND %s/%s => %s/%s\n",
							xids, m.bck, msg.ListRange.Template, bckTo, archName)
					}(e.Name, start)
				}

				time.Sleep(10 * time.Second)
				flt := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
				api.WaitForXactionIdle(baseParams, flt)
			}

			var (
				objName string
				mime    = "application/x-" + test.ext[1:]
			)
			for _, en := range objList.Entries {
				if !en.IsInsideArch() {
					objName = en.Name
					continue
				}
				if rand.Intn(3) > 0 {
					continue
				}

				getArgs := api.GetArgs{
					Query: url.Values{
						apc.QparamArchpath: []string{en.Name},
						apc.QparamArchmime: []string{mime},
					},
				}
				oah, err := api.GetObject(baseParams, bckTo, objName, &getArgs)
				if err != nil {
					t.Errorf("%s?%s=%s(%dB): %v", bckTo.Cname(objName), apc.QparamArchpath, en.Name, oah.Size(), err)
				}
			}
		})
	}
}

// exercises `api.CreateArchMultiObj` followed by api.AppendToArch(local rand-reader)
func TestAppendToArch(t *testing.T) {
	var (
		bckFrom = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		bckTo   = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		m       = ioContext{
			t:       t,
			bck:     bckFrom,
			num:     10,
			prefix:  "archive/",
			ordered: true,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		numArchs   = m.num
		numAdd     = m.num
		numInArch  = cos.Min(m.num/2, 7)
		objPattern = "test_lst_%04d%s"
		archPath   = "extra/newfile%04d"
		subtests   = []struct {
			ext   string // one of archive.FileExtensions (same as: supported arch formats)
			multi bool   // false - append a single file, true - append a list of objects
		}{
			{
				ext: archive.ExtTar, multi: false,
			},
			{
				ext: archive.ExtTar, multi: true,
			},
			{
				ext: archive.ExtTgz, multi: false,
			},
			{
				ext: archive.ExtTgz, multi: true,
			},
			{
				ext: archive.ExtTarLz4, multi: false,
			},
		}
		subtestsLong = []struct {
			ext   string // one of archive.FileExtensions (same as: supported arch formats)
			multi bool   // false - append a single file, true - append a list of objects
		}{
			{
				ext: archive.ExtZip, multi: false,
			},
			{
				ext: archive.ExtZip, multi: true,
			},
			{
				ext: archive.ExtTarLz4, multi: true,
			},
		}
	)
	if !testing.Short() { // test-long, and see one other Skip below
		subtests = append(subtests, subtestsLong...)
	}
	for _, test := range subtests {
		tname := fmt.Sprintf("%s/multi=%t", test.ext, test.multi)
		t.Run(tname, func(t *testing.T) {
			tools.CreateBucketWithCleanup(t, proxyURL, bckFrom, nil)
			tools.CreateBucketWithCleanup(t, proxyURL, bckTo, nil)
			m.initWithCleanup()
			m.fileSize = cos.MinU64(m.fileSize+m.fileSize/3, 32*cos.KiB)
			m.puts()

			if testing.Short() && test.ext != archive.ExtTar {
				// skip all multi-object appends
				if test.multi {
					tools.ShortSkipf(t)
				}
				// reduce
				numArchs = 2
				numAdd = 3
			}

			for i := 0; i < numArchs; i++ {
				archName := fmt.Sprintf(objPattern, i, test.ext)
				list := make([]string, 0, numInArch)
				for j := 0; j < numInArch; j++ {
					list = append(list, m.objNames[rand.Intn(m.num)])
				}
				go func(archName string, list []string) {
					msg := cmn.ArchiveMsg{
						ToBck:      bckTo,
						ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
					}
					msg.ListRange.ObjNames = list

					_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
					tassert.CheckFatal(t, err)
				}(archName, list)
			}

			wargs := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
			api.WaitForXactionIdle(baseParams, wargs)

			lsmsg := &apc.LsoMsg{Prefix: "test_lst"}
			lsmsg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			objList, err := api.ListObjects(baseParams, bckTo, lsmsg, 0)
			tassert.CheckFatal(t, err)
			num := len(objList.Entries)
			tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)

			for i := 0; i < numArchs; i++ {
				archName := fmt.Sprintf(objPattern, i, test.ext)
				if test.multi {
					tlog.Logf("APPEND multi-obj %s => %s/%s\n", bckFrom, bckTo, archName)
					list := make([]string, 0, numAdd)
					for j := 0; j < numAdd; j++ {
						list = append(list, m.objNames[rand.Intn(m.num)])
					}
					msg := cmn.ArchiveMsg{
						ToBck:      bckTo,
						ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
					}
					msg.AppendToExisting = true
					msg.ListRange.ObjNames = list
					go func() {
						_, err = api.CreateArchMultiObj(baseParams, bckFrom, msg)
						tassert.CheckError(t, err)
					}()
				} else {
					for j := 0; j < numAdd; j++ {
						reader, _ := readers.NewRandReader(fileSize, cos.ChecksumNone)
						putArgs := api.PutArgs{
							BaseParams: baseParams,
							Bck:        bckTo,
							ObjName:    archName,
							Reader:     reader,
							Size:       fileSize,
						}
						archpath := fmt.Sprintf(archPath, j) + cos.GenTie()
						appendArchArgs := api.AppendToArchArgs{
							PutArgs:  putArgs,
							ArchPath: archpath,
						}
						tlog.Logf("APPEND local rand => %s/%s/%s\n", bckTo, archName, archpath)
						err = api.AppendToArch(appendArchArgs)
						tassert.CheckError(t, err)
					}
				}
			}
			if test.multi {
				wargs := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
				api.WaitForXactionIdle(baseParams, wargs)
			}

			lsmsg.SetFlag(apc.LsArchDir)
			objList, err = api.ListObjects(baseParams, bckTo, lsmsg, 0)
			tassert.CheckError(t, err)
			num = len(objList.Entries)
			expectedNum := numArchs + numArchs*(numInArch+numAdd)

			tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)
		})
	}
}
