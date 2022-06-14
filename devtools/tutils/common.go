// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

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
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
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
			Name:     prefix + cos.RandString(8),
			Provider: apc.ProviderAIS,
		}
		_, err := api.HeadBucket(baseParams, bck)
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

func BckExists(proxyURL string, bck cmn.Bck) (bool, error) {
	baseParams := api.BaseParams{Client: gctx.Client, URL: proxyURL}
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(bck))
	if err != nil {
		return false, err
	}
	return bcks.Contains(cmn.QueryBcks(bck)), nil
}

func isRemoteBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	if !bck.IsRemote() {
		return false
	}
	exists, err := BckExists(proxyURL, bck)
	tassert.CheckFatal(tb, err)
	return exists
}

func isCloudBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	if !bck.IsCloud() {
		return false
	}
	exists, err := BckExists(proxyURL, bck)
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
		fname := cos.RandString(20)
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
	fileData, err = os.ReadFile(filepath.Join(config.ConfigDir, cmn.ProxyIDFname))
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}

	isLocal = strings.TrimSpace(string(fileData)) == smap.Primary.ID()
	return
}
