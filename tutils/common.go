// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func prependTime(msg string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000000"), msg)
}

func Logln(msg string) {
	if testing.Verbose() {
		fmt.Fprintln(os.Stdout, prependTime(msg))
	}
}

func Logf(msg string, args ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stdout, prependTime(msg), args...)
	}
}

// Generates strong random string or fallbacks to weak if error occurred
// during generation.
func GenRandomString(fnLen int) string {
	bytes := make([]byte, fnLen)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = cmn.LetterBytes[b%byte(len(cmn.LetterBytes))]
	}
	return string(bytes)
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
			Name:     prefix + GenRandomString(8),
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

type SkipTestArgs struct {
	RequiresRemote bool
	RequiresAuth   bool
	Long           bool
	Cloud          bool
	Bck            cmn.Bck
}

func CheckSkip(tb testing.TB, args SkipTestArgs) {
	if args.RequiresRemote && RemoteCluster.UUID == "" {
		tb.Skip("test requires a remote cluster")
	}
	if args.RequiresAuth && AuthToken == "" {
		tb.Skip("test requires auth token")
	}
	if args.Long && testing.Short() {
		tb.Skip("skipping test in short mode")
	}
	if args.Cloud {
		proxyURL := GetPrimaryURL()
		if !IsCloudBucket(tb, proxyURL, args.Bck) {
			tb.Skip("test requires a cloud bucket")
		}
	}
}

func IsCloudBucket(tb testing.TB, proxyURL string, bck cmn.Bck) bool {
	bck.Provider = cmn.AnyCloud
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

func PutRR(tb testing.TB, baseParams api.BaseParams, objSize int64, cksumType string, bck cmn.Bck, dir string, objCount, fnlen int) []string {
	var (
		objNames = make([]string, objCount)
	)
	for i := 0; i < objCount; i++ {
		fname := GenRandomString(fnlen)
		objName := filepath.Join(dir, fname)
		objNames[i] = objName
		// FIXME: Separate RandReader per object created inside PutObjRR to workaround https://github.com/golang/go/issues/30597
		err := PutObjRR(baseParams, bck, objName, objSize, cksumType)
		tassert.CheckFatal(tb, err)
	}

	return objNames
}
