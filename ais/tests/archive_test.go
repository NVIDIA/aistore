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
	"time"

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
func TestArchiveListRange(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		_testArchiveListRange(t, bck)
	})
}

func _testArchiveListRange(t *testing.T, bck *cluster.Bck) {
	var (
		m = ioContext{
			t:       t,
			bck:     bck.Bck,
			num:     100,
			prefix:  "archive/",
			ordered: true,
		}
		toBck      = cmn.Bck{Name: cos.RandString(10), Provider: cmn.ProviderAIS}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		numArchs   = 20
		numInArch  = cos.Min(m.num/2, 7)
		numPuts    = m.num
		fmtRange   = "%s{%d..%d}"
	)
	m.init()
	if testing.Short() {
		numArchs = 2
	}
	m.puts()
	if m.bck.IsRemote() {
		defer m.del()
	}
	if !toBck.Equal(m.bck) && toBck.IsAIS() {
		tutils.CreateFreshBucket(t, proxyURL, toBck, nil)
	}

	// archive bucket => same bucket in parallel
	for i := 0; i < numArchs; i++ {
		go func(i int) {
			tarName := fmt.Sprintf("test_lst_%02d.tar", i)
			list := make([]string, 0, numInArch)
			for j := 0; j < numInArch; j++ {
				list = append(list, m.objNames[rand.Intn(numPuts)])
			}
			_, err := api.ArchiveList(baseParams, m.bck, toBck, tarName, list)
			tassert.CheckFatal(t, err)
		}(i)
	}
	for i := 0; i < numArchs; i++ {
		go func(i int) {
			tarName := fmt.Sprintf("test_rng_%02d.tar", i)
			start := rand.Intn(numPuts - numInArch)
			rng := fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
			_, err := api.ArchiveRange(baseParams, m.bck, toBck, tarName, rng)
			tassert.CheckFatal(t, err)
		}(i)
	}

	wargs := api.XactReqArgs{Kind: cmn.ActArchive, Bck: m.bck}
	api.WaitForXactionIdle(baseParams, wargs)

	time.Sleep(3 * time.Second) // TODO -- FIXME: remove
	tlog.Logf("List %q\n", toBck)
	msg := &cmn.SelectMsg{Prefix: "test_"}
	msg.AddProps(cmn.GetPropsName, cmn.GetPropsSize)
	objList, err := api.ListObjects(baseParams, toBck, msg, 0)
	tassert.CheckFatal(t, err)
	for _, entry := range objList.Entries {
		tlog.Logf("%s: %dB\n", entry.Name, entry.Size)
	}
	tassert.Errorf(t, len(objList.Entries) == numArchs*2, "expected %d, have %d", numArchs*2, len(objList.Entries))
}
