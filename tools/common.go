// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

var DefaultWaitRetry = WaitRetryOpts{MaxRetries: 3, Interval: time.Second * 3}

func WaitForCondition(condition func() bool, opts WaitRetryOpts) error {
	var (
		i             int
		retries       = opts.MaxRetries
		retryInterval = opts.Interval
	)
	for {
		if success := condition(); success {
			return nil
		}
		time.Sleep(retryInterval)
		i++
		if i >= retries {
			return fmt.Errorf("max retries (%d) exceeded", retries)
		}
	}
}

// Generates an object name that hashes to a different target than `baseName`.
func GenerateNotConflictingObjectName(baseName, newNamePrefix string, bck cmn.Bck, smap *meta.Smap) string {
	// Init digests - HrwTarget() requires it
	smap.InitDigests()

	newName := newNamePrefix

	cbck := meta.CloneBck(&bck)
	baseNameHrw, e1 := smap.HrwName2T(cbck.MakeUname(baseName))
	newNameHrw, e2 := smap.HrwName2T(cbck.MakeUname(newName))
	cos.Assert(e1 == nil && e2 == nil)

	for i := 0; baseNameHrw == newNameHrw; i++ {
		newName = newNamePrefix + strconv.Itoa(i)
		newNameHrw, e1 = smap.HrwName2T(cbck.MakeUname(newName))
		cos.AssertNoErr(e1)
	}
	return newName
}

func GenerateNonexistentBucketName(prefix string, bp api.BaseParams) (string, error) {
	for range 100 {
		bck := cmn.Bck{
			Name:     prefix + trand.String(8),
			Provider: apc.AIS,
		}
		_, err := api.HeadBucket(bp, bck, true /* don't add to cluster MD */)
		if err == nil {
			continue
		}
		errHTTP, ok := err.(*cmn.ErrHTTP)
		if !ok {
			return "",
				fmt.Errorf("error generating bucket name: expected error of type *cmn.ErrHTTP, but got: %T", err)
		}
		if errHTTP.Status == http.StatusNotFound {
			return bck.Name, nil
		}

		return "", fmt.Errorf("error generating bucket name: unexpected HEAD request error: %v", err)
	}

	return "", errors.New("error generating bucket name: too many tries gave no result")
}

func BucketsContain(bcks cmn.Bcks, qbck cmn.QueryBcks) bool {
	for i := range bcks {
		bck := bcks[i]
		if qbck.Equal(&bck) || qbck.Contains(&bck) {
			return true
		}
	}
	return false
}

func BucketExists(tb testing.TB, proxyURL string, bck cmn.Bck) (bool, error) {
	if bck.IsQuery() {
		if tb == nil {
			return false, fmt.Errorf("expecting a named bucket, got %q", bck.String())
		}
		tassert.CheckFatal(tb, fmt.Errorf("expecting a named bucket, got %q", bck.String()))
	}
	bp := api.BaseParams{Client: gctx.Client, URL: proxyURL, Token: LoggedUserToken}
	_, err := api.HeadBucket(bp, bck, true /*dontAddRemote*/)
	if err == nil {
		return true, nil
	}
	errHTTP, ok := err.(*cmn.ErrHTTP)
	if !ok {
		err = fmt.Errorf("expected an error of the type *cmn.ErrHTTP, got %v(%T)", err, err)
		if tb == nil {
			return false, err
		}
		tassert.CheckFatal(tb, err)
	}
	if errHTTP.Status != http.StatusNotFound {
		err = fmt.Errorf("tools.BucketExists: expected status 404 (NotFound), got [%s]", errHTTP.Message)
		if tb != nil {
			tassert.CheckFatal(tb, err)
		} else {
			cos.Exitf("%v", err)
		}
	}
	return false, err
}

func isRemoteAndPresentBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	if !bck.IsRemote() {
		return false
	}
	exists, err := BucketExists(tb, proxyURL, bck)
	tassert.CheckFatal(tb, err)
	return exists
}

func PutObjRR(bp api.BaseParams, bck cmn.Bck, objName string, objSize int64, cksumType string) error {
	reader, err := readers.NewRand(objSize, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutArgs{
		BaseParams: bp,
		Bck:        bck,
		ObjName:    objName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	_, err = api.PutObject(&putArgs)
	return err
}

func PutRR(tb testing.TB, bp api.BaseParams, objSize int64, cksumType string,
	bck cmn.Bck, dir string, objCount int) []string {
	objNames := make([]string, objCount)
	for i := range objCount {
		fn := trand.String(20)
		objName := filepath.Join(dir, fn)
		objNames[i] = objName
		// FIXME: Separate RandReader per object created inside PutObjRR to workaround
		// https://github.com/golang/go/issues/30597
		err := PutObjRR(bp, bck, objName, objSize, cksumType)
		tassert.CheckFatal(tb, err)
	}

	return objNames
}

func isClusterK8s() (isK8s bool, err error) {
	// NOTE: The test suite doesn't have to be deployed on K8s, the cluster has to be.
	_, err = api.ETLList(BaseAPIParams(GetPrimaryURL()))
	isK8s = err == nil
	// HACK: Check based on error message. Unfortunately, there is no relevant HTTP code.
	if err != nil && strings.Contains(err.Error(), "requires Kubernetes") {
		err = nil
	}
	return
}

func isClusterLocal() (isLocal bool, err error) {
	var (
		primaryURL = GetPrimaryURL()
		smap       *meta.Smap
		bp         = BaseAPIParams(primaryURL)
		config     *cmn.Config
		fileData   []byte
	)
	if smap, err = api.GetClusterMap(bp); err != nil {
		return
	}
	if config, err = api.GetDaemonConfig(bp, smap.Primary); err != nil {
		return
	}
	fileData, err = os.ReadFile(filepath.Join(config.ConfigDir, fname.ProxyID))
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}

	isLocal = strings.TrimSpace(string(fileData)) == smap.Primary.ID()
	return
}
