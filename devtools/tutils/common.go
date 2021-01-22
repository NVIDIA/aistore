// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils/readers"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

type SkipTestArgs struct {
	Bck                   cmn.Bck
	MinTargets            int
	MinMountpaths         int
	RequiresRemoteCluster bool
	RequiresAuth          bool
	Long                  bool
	RemoteBck             bool
	K8s                   bool
}

func prependTime(msg string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000000"), msg)
}

func Logln(msg string) {
	if testing.Verbose() {
		fmt.Fprintln(os.Stdout, prependTime(msg))
	}
}

func Logf(f string, a ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stdout, prependTime(f), a...)
	}
}

func LogfCond(cond bool, f string, a ...interface{}) {
	if cond {
		Logf(f, a...)
	}
}

// Generates an object name that hashes to a different target than `baseName`.
func GenerateNotConflictingObjectName(baseName, newNamePrefix string, bck cmn.Bck, smap *cluster.Smap) string {
	// Init digests - HrwTarget() requires it
	smap.InitDigests()

	newName := newNamePrefix

	cbck := cluster.NewBckEmbed(bck)
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
			Name:     prefix + cmn.RandString(8),
			Provider: cmn.ProviderAIS,
		}
		_, err := api.HeadBucket(baseParams, bck)
		if err == nil {
			continue
		}
		errHTTP, ok := err.(*cmn.HTTPError)
		if !ok {
			return "", fmt.Errorf("error generating bucket name: expected error of type *cmn.HTTPError, but got: %T", err)
		}
		if errHTTP.Status == http.StatusNotFound {
			return bck.Name, nil
		}

		return "", fmt.Errorf("error generating bucket name: unexpected HEAD request error: %v", err)
	}

	return "", errors.New("error generating bucket name: too many tries gave no result")
}

func CheckSkip(tb testing.TB, args SkipTestArgs) {
	var smap *cluster.Smap
	if args.RequiresRemoteCluster && RemoteCluster.UUID == "" {
		tb.Skipf("%s requires remote cluster", tb.Name())
	}
	if args.RequiresAuth && AuthToken == "" {
		tb.Skipf("%s requires authentication token", tb.Name())
	}
	if args.Long && testing.Short() {
		tb.Skipf("skipping %s in short mode", tb.Name())
	}
	if args.RemoteBck {
		proxyURL := GetPrimaryURL()
		if !isRemoteBucket(tb, proxyURL, args.Bck) {
			tb.Skipf("%s requires a remote bucket", tb.Name())
		}
	}
	if args.K8s {
		// NOTE: The test suite doesn't have to be deployed on K8s, the cluster has to be.
		_, err := api.ETLList(BaseAPIParams(GetPrimaryURL()))
		// HACK: Check based on error message. Unfortunately, there is no relevant HTTP code.
		if err != nil {
			if strings.Contains(err.Error(), "requires Kubernetes") {
				tb.Skipf("%s requires Kubernetes", tb.Name())
			} else {
				tb.Fatalf("Unrecognized error upon checking K8s deployment; err: %v", err)
			}
		}
	}

	if args.MinTargets > 0 || args.MinMountpaths > 0 {
		smap = GetClusterMap(tb, GetPrimaryURL())
	}
	if args.MinTargets > 0 {
		if smap.CountTargets() < args.MinTargets {
			tb.Skipf("%s requires at least %d targets (have %d)",
				tb.Name(), args.MinTargets, smap.CountTargets())
		}
	}
	if args.MinMountpaths > 0 {
		targets := smap.Tmap.ActiveNodes()
		proxyURL := GetPrimaryURL()
		baseParams := BaseAPIParams(proxyURL)
		mpList, _ := api.GetMountpaths(baseParams, targets[0])
		if l := len(mpList.Available); l < args.MinMountpaths {
			tb.Skipf("%s requires at least %d mountpaths (have %d)", tb.Name(), args.MinMountpaths, l)
		}
	}
}

func isRemoteBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	if !bck.IsRemote() {
		return false
	}
	baseParams := BaseAPIParams(proxyURL)
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(bck))
	tassert.CheckFatal(tb, err)
	return bcks.Contains(cmn.QueryBcks(bck))
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
		fname := cmn.RandString(20)
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
