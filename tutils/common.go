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
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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
	Long           bool
	Cloud          bool
	Bck            cmn.Bck
}

func CheckSkip(t *testing.T, args SkipTestArgs) {
	if args.RequiresRemote && RemoteCluster.UUID == "" {
		t.Skip("test requires a remote cluster")
	}
	if args.Long && testing.Short() {
		t.Skip("skipping test in short mode")
	}
	if args.Cloud {
		proxyURL := GetPrimaryURL()
		if !IsCloudBucket(t, proxyURL, args.Bck) {
			t.Skip("test requires a cloud bucket")
		}
	}
}

func IsCloudBucket(t *testing.T, proxyURL string, bck cmn.Bck) bool {
	bck.Provider = cmn.AnyCloud
	baseParams := BaseAPIParams(proxyURL)
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(bck))
	tassert.CheckFatal(t, err)
	return bcks.Contains(cmn.QueryBcks(bck))
}
