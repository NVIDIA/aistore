// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/archive"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

//
// GET from
//

func TestGetFromArchive(t *testing.T) {
	const tmpDir = "/tmp"
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			m = ioContext{
				t:   t,
				bck: bck.Clone(),
			}
			baseParams  = tools.BaseAPIParams(m.proxyURL)
			errCh       = make(chan error, m.num)
			numArchived = 10
			randomNames = make([]string, numArchived)
			subtests    = []struct {
				ext        string // one of cos.ArchExtensions
				nested     bool   // subdirs
				autodetect bool   // auto-detect by magic
				mime       bool   // specify mime type
			}{
				{
					ext: cos.ExtTar, nested: false, autodetect: false, mime: false,
				},
				{
					ext: cos.ExtTarTgz, nested: false, autodetect: false, mime: false,
				},
				{
					ext: cos.ExtZip, nested: false, autodetect: false, mime: false,
				},
				{
					ext: cos.ExtTar, nested: true, autodetect: true, mime: false,
				},
				{
					ext: cos.ExtTarTgz, nested: true, autodetect: true, mime: false,
				},
				{
					ext: cos.ExtZip, nested: true, autodetect: true, mime: false,
				},
				{
					ext: cos.ExtTar, nested: true, autodetect: true, mime: true,
				},
				{
					ext: cos.ExtTarTgz, nested: true, autodetect: true, mime: true,
				},
				{
					ext: cos.ExtZip, nested: true, autodetect: true, mime: true,
				},
			}
		)
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
				if test.ext == cos.ExtZip {
					err = archive.CreateZipWithRandomFiles(archName, numArchived, fsize, randomNames)
				} else {
					err = archive.CreateTarWithRandomFiles(
						archName,
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
					getOptions := api.GetObjectInput{
						Query: url.Values{
							apc.QparamArchpath: []string{randomName},
							apc.QparamArchmime: []string{mime},
						},
					}
					n, err := api.GetObject(baseParams, m.bck, objname, getOptions)
					tlog.Logf("%s/%s?%s=%s(%dB)\n", m.bck.Name, objname, apc.QparamArchpath, randomName, n)
					tassert.CheckFatal(t, err)
				}
			})
		}
	})
}

// PUT/create
func TestCreateMultiObjArch(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		testMobjArch(t, bck)
	})
}

func testMobjArch(t *testing.T, bck *cluster.Bck) {
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
			ext            string // one of cos.ArchExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
			abrt           bool
		}{
			{
				ext: cos.ExtTar, list: true,
			},
			{
				ext: cos.ExtTar, list: false, inclSrcBckName: true,
			},
			{
				ext: cos.ExtTar, list: false,
			},
			{
				ext: cos.ExtMsgpack, list: true,
			},
		}
		subtestsLong = []struct {
			ext            string // one of cos.ArchExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
			abrt           bool
		}{
			{
				ext: cos.ExtTgz, list: true,
			},
			{
				ext: cos.ExtTgz, list: false, inclSrcBckName: true,
			},
			{
				ext: cos.ExtZip, list: true,
			},
			{
				ext: cos.ExtZip, list: false, inclSrcBckName: true,
			},
			{
				ext: cos.ExtZip, list: true,
			},
			{
				ext: cos.ExtMsgpack, list: false,
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
		if tm&0x3 == 0 {
			test.abrt = true
			abrt = "/abort"
		}
		tname := fmt.Sprintf("%s/%s%s", test.ext, listOrRange, abrt)
		if test.inclSrcBckName {
			tname += "/incl-src=" + m.bck.Name
		}
		t.Run(tname, func(t *testing.T) {
			if m.bck.IsRemote() {
				m.num = numPuts >> 1
			}
			m.initWithCleanup()
			m.puts()
			if m.bck.IsRemote() {
				defer m.del(-1)
			}
			toBck := cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
			tools.CreateBucketWithCleanup(t, proxyURL, toBck, nil)

			if test.list {
				tlog.Logf("Archive %d lists %s => %s\n", numArchs, m.bck, toBck)
				for i := 0; i < numArchs; i++ {
					archName := fmt.Sprintf("test_lst_%02d%s", i, test.ext)
					list := make([]string, 0, numInArch)
					if test.ext == cos.ExtMsgpack {
						// m.b. unique for msgpack
						start := rand.Intn(m.num - numInArch)
						for j := start; j < start+numInArch; j++ {
							list = append(list, m.objNames[j])
						}
					} else {
						for j := 0; j < numInArch; j++ {
							list = append(list, m.objNames[rand.Intn(m.num)])
						}
					}
					go func(archName string, list []string) {
						msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName}
						msg.SelectObjsMsg.ObjNames = list
						msg.InclSrcBname = test.inclSrcBckName

						_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
					}(archName, list)
				}
			} else {
				tlog.Logf("Archive %d ranges %s => %s\n", numArchs, m.bck, toBck)
				for i := 0; i < numArchs; i++ {
					archName := fmt.Sprintf("test_rng_%02d%s", i, test.ext)
					start := rand.Intn(m.num - numInArch)
					go func(archName string, start int) {
						msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName}
						msg.SelectObjsMsg.Template = fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
						msg.InclSrcBname = test.inclSrcBckName

						_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
					}(archName, start)
				}
			}

			flt := api.XactReqArgs{Kind: apc.ActArchive, Bck: m.bck}
			if test.abrt {
				time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)
				tlog.Logln("Aborting...")
				api.AbortXaction(baseParams, flt)
			}

			api.WaitForXactionIdle(baseParams, flt)

			tlog.Logf("List %s\n", toBck)
			msg := &apc.LsoMsg{Prefix: "test_"}
			msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			objList, err := api.ListObjects(baseParams, toBck, msg, 0)
			tassert.CheckFatal(t, err)
			for _, entry := range objList.Entries {
				tlog.Logf("%s: %dB\n", entry.Name, entry.Size)
			}
			num := len(objList.Entries)
			tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)

			msg.SetFlag(apc.LsArchDir)
			objList, err = api.ListObjects(baseParams, toBck, msg, 0)
			tassert.CheckFatal(t, err)
			num = len(objList.Entries)
			expectedNum := numArchs + numArchs*numInArch

			if test.ext == cos.ExtMsgpack { // TODO -- FIXME: remove
				if num != expectedNum {
					tlog.Logf("expected %d, have %d\n", expectedNum, num)
				}
			} else {
				tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)
			}

			var (
				objName string
				mime    = "application/x-" + test.ext[1:]
			)
			for _, entry := range objList.Entries {
				if !entry.IsInsideArch() {
					objName = entry.Name
					continue
				}
				if rand.Intn(3) > 0 {
					continue
				}

				getOptions := api.GetObjectInput{
					Query: url.Values{
						apc.QparamArchpath: []string{entry.Name},
						apc.QparamArchmime: []string{mime},
					},
				}
				n, err := api.GetObject(baseParams, toBck, objName, getOptions)
				if err != nil {
					t.Errorf("%s/%s?%s=%s(%dB): %v", toBck.Name, objName, apc.QparamArchpath, entry.Name, n, err)
				}
			}
		})
	}
}

func TestAppendToArch(t *testing.T) {
	var (
		fromBck = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		toBck   = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		m       = ioContext{
			t:       t,
			bck:     fromBck,
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
			ext   string // one of cos.ArchExtensions (same as: supported arch formats)
			multi bool   // false - append a single file, true - append a list of objects
		}{
			{
				ext: cos.ExtTar, multi: false,
			},
			{
				ext: cos.ExtTar, multi: true,
			},
		}
	)
	for _, test := range subtests {
		tname := fmt.Sprintf("%s/multi=%t", test.ext, test.multi)
		t.Run(tname, func(t *testing.T) {
			tools.CreateBucketWithCleanup(t, proxyURL, fromBck, nil)
			tools.CreateBucketWithCleanup(t, proxyURL, toBck, nil)
			m.initWithCleanup()
			m.puts()

			if testing.Short() {
				if test.multi {
					tools.ShortSkipf(t)
				}
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
					msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName}
					msg.SelectObjsMsg.ObjNames = list

					_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
					tassert.CheckFatal(t, err)
				}(archName, list)
			}

			wargs := api.XactReqArgs{Kind: apc.ActArchive, Bck: m.bck}
			api.WaitForXactionIdle(baseParams, wargs)

			lsmsg := &apc.LsoMsg{Prefix: "test_lst"}
			lsmsg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			objList, err := api.ListObjects(baseParams, toBck, lsmsg, 0)
			tassert.CheckFatal(t, err)
			num := len(objList.Entries)
			tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)

			for i := 0; i < numArchs; i++ {
				archName := fmt.Sprintf(objPattern, i, test.ext)
				if test.multi {
					list := make([]string, 0, numAdd)
					for j := 0; j < numAdd; j++ {
						list = append(list, m.objNames[rand.Intn(m.num)])
					}
					msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName, AllowAppendToExisting: true}
					msg.SelectObjsMsg.ObjNames = list
					go func() {
						_, err = api.CreateArchMultiObj(baseParams, fromBck, msg)
						tassert.CheckError(t, err)
					}()
				} else {
					for j := 0; j < numAdd; j++ {
						reader, _ := readers.NewRandReader(fileSize, cos.ChecksumNone)
						putArgs := api.PutObjectArgs{
							BaseParams: baseParams,
							Bck:        toBck,
							Object:     archName,
							Reader:     reader,
							Size:       fileSize,
						}
						appendArchArgs := api.AppendToArchArgs{
							PutObjectArgs: putArgs,
							ArchPath:      fmt.Sprintf(archPath, j),
						}
						err = api.AppendToArch(appendArchArgs)
						tassert.CheckError(t, err)
					}
				}
			}
			if test.multi {
				wargs := api.XactReqArgs{Kind: apc.ActArchive, Bck: m.bck}
				api.WaitForXactionIdle(baseParams, wargs)
			}

			lsmsg.SetFlag(apc.LsArchDir)
			objList, err = api.ListObjects(baseParams, toBck, lsmsg, 0)
			tassert.CheckError(t, err)
			num = len(objList.Entries)
			expectedNum := numArchs + numArchs*(numInArch+numAdd)
			tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)
		})
	}
}
