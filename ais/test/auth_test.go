// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func createBaseParams() (unAuth, auth api.BaseParams) {
	unAuth = tools.BaseAPIParams()
	unAuth.Token = ""
	auth = tools.BaseAPIParams()
	return
}

func expectUnauthorized(t *testing.T, err error) {
	tassert.Fatalf(t, err != nil, "expected unauthorized error")
	var herr *cmn.ErrHTTP
	tassert.Fatalf(t, errors.As(err, &herr), "expected cmn.ErrHTTP, got %v", err)
	tassert.Fatalf(
		t, herr.Status == http.StatusUnauthorized,
		"expected status unauthorized, got: %d", herr.Status,
	)
}

func TestAuthObj(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiresAuth: true})
	var (
		unAuthBP, authBP = createBaseParams()
		bck              = cmn.Bck{Name: trand.String(10)}
	)
	err := api.CreateBucket(authBP, bck, nil)
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to create %s\n", authBP.Token[:16], bck.String())
	defer func() {
		err := api.DestroyBucket(authBP, bck)
		tassert.CheckFatal(t, err)
		tlog.Logf("bucket %s destroyed\n", bck.String())
	}()

	r, _ := readers.NewRand(fileSize, cos.ChecksumNone)
	objName := trand.String(10)
	_, err = api.PutObject(api.PutArgs{
		BaseParams: unAuthBP,
		Bck:        bck,
		Reader:     r,
		Size:       fileSize,
		ObjName:    objName,
	})
	expectUnauthorized(t, err)

	r, _ = readers.NewRand(fileSize, cos.ChecksumNone)
	_, err = api.PutObject(api.PutArgs{
		BaseParams: authBP,
		Bck:        bck,
		Reader:     r,
		Size:       fileSize,
		ObjName:    objName,
	})
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to PUT %s\n", authBP.Token[:16], bck.Cname(objName))
}

func TestAuthBck(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiresAuth: true})
	var (
		unAuthBP, authBP = createBaseParams()
		bck              = cmn.Bck{Name: trand.String(10)}
	)
	err := api.CreateBucket(unAuthBP, bck, nil)
	expectUnauthorized(t, err)

	err = api.CreateBucket(authBP, bck, nil)
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to create %s\n", authBP.Token[:16], bck.String())

	p, err := api.HeadBucket(authBP, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, p.Provider == apc.AIS, "expected provider %q, got %q", apc.AIS, p.Provider)

	defer func() {
		err := api.DestroyBucket(authBP, bck)
		tassert.CheckFatal(t, err)
		tlog.Logf("%s destroyed\n", bck.String())
	}()

	err = api.DestroyBucket(unAuthBP, bck)
	expectUnauthorized(t, err)
}
