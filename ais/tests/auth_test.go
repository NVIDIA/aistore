// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"errors"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

func createBaseParams() (unAuth, auth api.BaseParams) {
	unAuth = tutils.BaseAPIParams()
	auth = tutils.BaseAPIParams()
	auth.Token = tutils.AuthToken
	return
}

func expectUnauthorized(t *testing.T, err error) {
	tassert.Fatalf(t, err != nil, "expected unauthorized error")
	var httpErr *cmn.ErrHTTP
	tassert.Fatalf(t, errors.As(err, &httpErr), "expected cmn.ErrHTTP")
	tassert.Fatalf(
		t, httpErr.Status == http.StatusUnauthorized,
		"expected status unauthorized, got: %d", httpErr.Status,
	)
}

func TestAuthObj(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiresAuth: true})
	var (
		unAuthBP, authBP = createBaseParams()
		bck              = cmn.Bck{
			Name: cos.RandString(10),
		}
	)
	err := api.CreateBucket(authBP, bck, nil)
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to create %s\n", authBP.Token[:16], bck.StringEx())
	defer func() {
		err := api.DestroyBucket(authBP, bck)
		tassert.CheckFatal(t, err)
		tlog.Logf("bucket %s destroyed\n", bck.StringEx())
	}()

	r, _ := readers.NewRandReader(fileSize, cos.ChecksumNone)
	objName := cos.RandString(10)
	err = api.PutObject(api.PutObjectArgs{
		BaseParams: unAuthBP,
		Bck:        bck,
		Reader:     r,
		Size:       fileSize,
		Object:     objName,
	})
	expectUnauthorized(t, err)

	r, _ = readers.NewRandReader(fileSize, cos.ChecksumNone)
	err = api.PutObject(api.PutObjectArgs{
		BaseParams: authBP,
		Bck:        bck,
		Reader:     r,
		Size:       fileSize,
		Object:     objName,
	})
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to PUT %s/%s\n", authBP.Token[:16], bck.StringEx(), objName)
}

func TestAuthBck(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiresAuth: true})
	var (
		unAuthBP, authBP = createBaseParams()
		bck              = cmn.Bck{
			Name: cos.RandString(10),
		}
	)
	err := api.CreateBucket(unAuthBP, bck, nil)
	expectUnauthorized(t, err)

	err = api.CreateBucket(authBP, bck, nil)
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to create %s\n", authBP.Token[:16], bck.StringEx())

	p, err := api.HeadBucket(authBP, bck)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, p.Provider == apc.ProviderAIS, "expected provider %q, got %q", apc.ProviderAIS, p.Provider)

	defer func() {
		err := api.DestroyBucket(authBP, bck)
		tassert.CheckFatal(t, err)
		tlog.Logf("%s destroyed\n", bck.StringEx())
	}()

	err = api.DestroyBucket(unAuthBP, bck)
	expectUnauthorized(t, err)
}
