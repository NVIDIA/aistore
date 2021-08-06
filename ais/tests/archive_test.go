// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/archive"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
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
				bck: bck.Bck,
			}
			baseParams  = tutils.BaseAPIParams(m.proxyURL)
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

				tutils.Put(m.proxyURL, m.bck, objname, reader, errCh)
				tassert.SelectErr(t, errCh, "put", true)
				defer tutils.Del(m.proxyURL, m.bck, objname, nil, nil, true)

				for _, randomName := range randomNames {
					var mime string
					if test.mime {
						mime = "application/x-" + test.ext[1:]
					}
					getOptions := api.GetObjectInput{
						Query: url.Values{
							cmn.URLParamArchpath: []string{randomName},
							cmn.URLParamArchmime: []string{mime},
						},
					}
					n, err := api.GetObject(baseParams, m.bck, objname, getOptions)
					tlog.Logf("%s/%s?%s=%s(%dB)\n", m.bck.Name, objname, cmn.URLParamArchpath, randomName, n)
					tassert.CheckFatal(t, err)
				}
			})
		}
	})
}

// PUT/create
func TestCreateArchMultiObj(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		_createArchMultiObj(t, bck)
	})
}

func _createArchMultiObj(t *testing.T, bck *cluster.Bck) {
	var (
		m = ioContext{
			t:       t,
			bck:     bck.Bck,
			num:     75,
			prefix:  "archive/",
			ordered: true,
		}
		toBck      = cmn.Bck{Name: cos.RandString(10), Provider: cmn.ProviderAIS}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		numArchs   = 15
		numInArch  = cos.Min(m.num/2, 7)
		numPuts    = m.num
		fmtRange   = "%s{%d..%d}"
		subtests   = []struct {
			ext            string // one of cos.ArchExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
		}{
			{
				ext: cos.ExtTar, list: true,
			},
			{
				ext: cos.ExtTar, list: false, inclSrcBckName: true,
			},
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
		}
	)
	if testing.Short() {
		numArchs = 2
	}
	for _, test := range subtests {
		listOrRange := "list"
		if !test.list {
			listOrRange = "range"
		}
		tname := fmt.Sprintf("%s/%s", test.ext, listOrRange)
		if test.inclSrcBckName {
			tname += "//" + m.bck.Name
		}
		t.Run(tname, func(t *testing.T) {
			m.init()
			m.puts()
			if m.bck.IsRemote() {
				defer m.del()
			}
			if !toBck.Equal(m.bck) && toBck.IsAIS() {
				tutils.CreateFreshBucket(t, proxyURL, toBck, nil)
			}

			if test.list {
				for i := 0; i < numArchs; i++ {
					archName := fmt.Sprintf("test_lst_%02d%s", i, test.ext)
					list := make([]string, 0, numInArch)
					for j := 0; j < numInArch; j++ {
						list = append(list, m.objNames[rand.Intn(numPuts)])
					}
					go func(archName string, list []string) {
						msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName}
						msg.ListRangeMsg.ObjNames = list
						msg.InclBckName = test.inclSrcBckName

						_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
					}(archName, list)
				}
			} else {
				for i := 0; i < numArchs; i++ {
					archName := fmt.Sprintf("test_rng_%02d%s", i, test.ext)
					start := rand.Intn(numPuts - numInArch)
					go func(archName string, start int) {
						msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName}
						msg.ListRangeMsg.Template = fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
						msg.InclBckName = test.inclSrcBckName

						_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
						tassert.CheckFatal(t, err)
					}(archName, start)
				}
			}

			wargs := api.XactReqArgs{Kind: cmn.ActArchive, Bck: m.bck}
			api.WaitForXactionIdle(baseParams, wargs)

			tlog.Logf("List %q\n", toBck)
			msg := &cmn.SelectMsg{Prefix: "test_"}
			msg.AddProps(cmn.GetPropsName, cmn.GetPropsSize)
			objList, err := api.ListObjects(baseParams, toBck, msg, 0)
			tassert.CheckFatal(t, err)
			for _, entry := range objList.Entries {
				tlog.Logf("%s: %dB\n", entry.Name, entry.Size)
			}
			num := len(objList.Entries)
			tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)

			msg.Flags |= cmn.SelectArchDir
			objList, err = api.ListObjects(baseParams, toBck, msg, 0)
			tassert.CheckFatal(t, err)
			num = len(objList.Entries)
			expectedNum := numArchs + numArchs*numInArch
			tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)

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
						cmn.URLParamArchpath: []string{entry.Name},
						cmn.URLParamArchmime: []string{mime},
					},
				}
				n, err := api.GetObject(baseParams, toBck, objName, getOptions)
				if err != nil {
					t.Errorf("%s/%s?%s=%s(%dB): %v", toBck.Name, objName, cmn.URLParamArchpath, entry.Name, n, err)
				}
			}
		})
	}
}

func TestArchiveAppend(t *testing.T) {
	var (
		fromBck = cmn.Bck{Name: cos.RandString(10), Provider: cmn.ProviderAIS}
		toBck   = cmn.Bck{Name: cos.RandString(10), Provider: cmn.ProviderAIS}
		m       = ioContext{
			t:       t,
			bck:     fromBck,
			num:     10,
			prefix:  "archive/",
			ordered: true,
		}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		numArchs   = 10
		numAdd     = 10
		numInArch  = cos.Min(m.num/2, 7)
		numPuts    = m.num
		objPattern = "test_lst_%04d%s"
		archPath   = "extra/newfile%04d"
		subtests   = []struct {
			ext  string // one of cos.ArchExtensions (same as: supported arch formats)
			list bool
		}{
			{
				ext: cos.ExtTar, list: true,
			},
		}
	)
	if testing.Short() {
		numArchs = 1
		numAdd = 2
	}
	for _, test := range subtests {
		tname := test.ext
		t.Run(tname, func(t *testing.T) {
			tutils.CreateFreshBucket(t, proxyURL, fromBck, nil)
			tutils.CreateFreshBucket(t, proxyURL, toBck, nil)
			m.init()
			m.puts()

			for i := 0; i < numArchs; i++ {
				archName := fmt.Sprintf(objPattern, i, test.ext)
				list := make([]string, 0, numInArch)
				for j := 0; j < numInArch; j++ {
					list = append(list, m.objNames[rand.Intn(numPuts)])
				}
				go func(archName string, list []string) {
					msg := cmn.ArchiveMsg{ToBck: toBck, ArchName: archName}
					msg.ListRangeMsg.ObjNames = list

					_, err := api.CreateArchMultiObj(baseParams, m.bck, msg)
					tassert.CheckFatal(t, err)
				}(archName, list)
			}

			wargs := api.XactReqArgs{Kind: cmn.ActArchive, Bck: m.bck}
			api.WaitForXactionIdle(baseParams, wargs)

			selmsg := &cmn.SelectMsg{Prefix: "test_lst"}
			selmsg.AddProps(cmn.GetPropsName, cmn.GetPropsSize)
			objList, err := api.ListObjects(baseParams, toBck, selmsg, 0)
			tassert.CheckFatal(t, err)
			num := len(objList.Entries)
			tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)

			for i := 0; i < numArchs; i++ {
				for j := 0; j < numAdd; j++ {
					archName := fmt.Sprintf(objPattern, i, test.ext)
					reader, _ := readers.NewRandReader(fileSize, cos.ChecksumNone)
					putArgs := api.PutObjectArgs{
						BaseParams: baseParams,
						Bck:        toBck,
						Object:     archName,
						Reader:     reader,
						Size:       fileSize,
					}
					appendArcArgs := api.AppendObjectArchArgs{
						PutObjectArgs: putArgs,
						ArchPath:      fmt.Sprintf(archPath, j),
					}
					err = api.AppendObjectArch(appendArcArgs)
					tassert.CheckError(t, err)
				}
			}

			selmsg.Flags |= cmn.SelectArchDir
			objList, err = api.ListObjects(baseParams, toBck, selmsg, 0)
			tassert.CheckError(t, err)
			num = len(objList.Entries)
			expectedNum := numArchs + numArchs*(numInArch+numAdd)
			tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)
		})
	}
}
