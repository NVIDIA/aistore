// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

// Generates an object name that hashes to a different target than `baseName`.
func GenerateNotConflictingObjectName(baseName, newNamePrefix string, bck cmn.Bck, smap *cluster.Smap) string {
	// Init digests - HrwTarget() requires it
	smap.InitDigests()

	newName := newNamePrefix

	cbck := cluster.CloneBck(&bck)
	baseNameHrw, _ := cluster.HrwTarget(cbck.MakeUname(baseName), smap)
	newNameHrw, _ := cluster.HrwTarget(cbck.MakeUname(newName), smap)

	for i := 0; baseNameHrw == newNameHrw; i++ {
		newName = newNamePrefix + strconv.Itoa(i)
		newNameHrw, _ = cluster.HrwTarget(cbck.MakeUname(newName), smap)
	}
	return newName
}

func GenerateNonexistentBucketName(prefix string, baseParams api.BaseParams) (string, error) {
	for i := 0; i < 100; i++ {
		bck := cmn.Bck{
			Name:     prefix + trand.String(8),
			Provider: apc.AIS,
		}
		_, err := api.HeadBucket(baseParams, bck, true /* don't add to cluster MD */)
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

func BucketExists(tb testing.TB, proxyURL string, bck cmn.Bck) (bool, error) {
	if bck.IsQuery() {
		if tb == nil {
			return false, fmt.Errorf("expecting a named bucket, got %q", bck)
		}
		tassert.CheckFatal(tb, fmt.Errorf("expecting a named bucket, got %q", bck))
	}
	baseParams := api.BaseParams{Client: gctx.Client, URL: proxyURL, Token: LoggedUserToken}
	_, err := api.HeadBucket(baseParams, bck, true /*dontAddRemote*/)
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

func isRemoteBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	if !bck.IsRemote() {
		return false
	}
	exists, err := BucketExists(tb, proxyURL, bck)
	tassert.CheckFatal(tb, err)
	return exists
}

func isCloudBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	if !bck.IsCloud() {
		return false
	}
	exists, err := BucketExists(tb, proxyURL, bck)
	tassert.CheckFatal(tb, err)
	return exists
}

func PutObjRR(baseParams api.BaseParams, bck cmn.Bck, objName string, objSize int64, cksumType string) error {
	reader, err := readers.NewRandReader(objSize, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     objName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	return api.PutObject(putArgs)
}

func PutRR(tb testing.TB, baseParams api.BaseParams, objSize int64, cksumType string,
	bck cmn.Bck, dir string, objCount int) []string {
	objNames := make([]string, objCount)
	for i := 0; i < objCount; i++ {
		fname := trand.String(20)
		objName := filepath.Join(dir, fname)
		objNames[i] = objName
		// FIXME: Separate RandReader per object created inside PutObjRR to workaround
		// https://github.com/golang/go/issues/30597
		err := PutObjRR(baseParams, bck, objName, objSize, cksumType)
		tassert.CheckFatal(tb, err)
	}

	return objNames
}

func NewClientWithProxy(proxyURL string) *http.Client {
	transport := cmn.NewTransport(transportArgs)
	prxURL, _ := url.Parse(proxyURL)
	transport.Proxy = http.ProxyURL(prxURL)
	return &http.Client{
		Transport: transport,
		Timeout:   transportArgs.Timeout,
	}
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
		smap       *cluster.Smap
		baseParams = BaseAPIParams(primaryURL)
		config     *cmn.Config
		fileData   []byte
	)
	if smap, err = api.GetClusterMap(baseParams); err != nil {
		return
	}
	if config, err = api.GetDaemonConfig(baseParams, smap.Primary); err != nil {
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
