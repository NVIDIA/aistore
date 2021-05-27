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

func TestGetFromArchive(t *testing.T) {
	const tmpDir = "/tmp"
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			m = ioContext{
				t:   t,
				bck: bck.Bck,
			}
			baseParams  = tutils.BaseAPIParams(m.proxyURL)
			tarName     = fmt.Sprintf("%s/%s", tmpDir, bck.Name) + cos.ExtTar
			errCh       = make(chan error, m.num)
			numArchived = 5
			randomNames = make([]string, numArchived)
		)
		for i := 0; i < numArchived; i++ {
			randomNames[i] = fmt.Sprintf("%d.txt", rand.Int())
		}
		err := archive.CreateTarWithRandomFiles(
			tarName,              // tar name
			numArchived,          // num archived files
			rand.Intn(cos.KiB)+1, // archived file size
			false,                // duplication
			nil,                  // record exts
			randomNames,          // caller-generated filenames
		)
		tassert.CheckFatal(t, err)
		defer os.Remove(tarName)

		objname := filepath.Base(tarName)

		reader, err := readers.NewFileReaderFromFile(tarName, cos.ChecksumNone)
		tassert.CheckFatal(t, err)

		tutils.Put(m.proxyURL, m.bck, objname, reader, errCh)

		for _, randomName := range randomNames {
			getOptions := api.GetObjectInput{
				Query: url.Values{cmn.URLParamArchpath: []string{randomName}},
			}
			n, err := api.GetObject(baseParams, m.bck, objname, getOptions)
			tlog.Logf("%s/%s?%s=%s(%dB)\n", m.bck.Name, objname, cmn.URLParamArchpath, randomName, n)
			tassert.CheckFatal(t, err)
		}
	})
}
