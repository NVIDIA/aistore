// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRemoteCopyPutResponses(t *testing.T) {
	for _, operation := range []string{"rename", "promote"} {
		t.Run(operation, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				status  int
				body    string
				message string
				s3Code  string
			}{
				{name: "ok", status: http.StatusOK},
				{name: "redirect", status: http.StatusTemporaryRedirect, message: "Temporary Redirect"},
				{name: "forbidden", status: http.StatusForbidden, body: "access denied", message: "access denied"},
				{name: "empty-error", status: http.StatusInsufficientStorage, message: "failed to execute PUT request"},
				{name: "aws-access-denied", status: http.StatusForbidden,
					body:    `{"status":403,"message":"aws-error[AccessDenied: Access Denied]"}`,
					message: "aws-error[AccessDenied: Access Denied]", s3Code: s3.ErrCodeAccessDenied},
			} {
				t.Run(test.name, func(t *testing.T) {
					data := []byte("keep the source")
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(test.status)
						io.WriteString(w, test.body)
					}))
					defer srv.Close()
					peer := setCopyPutTestPeer(t, srv)

					lom := core.AllocLOM("remote-copy-put-" + operation)
					defer core.FreeLOM(lom)
					tassert.CheckFatal(t, lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)))
					destination := lom.ObjName
					var (
						source string
						err    error
					)
					if operation == "rename" {
						params := &core.PutParams{
							Reader:  cos.NewByteReader(data),
							WorkTag: fs.WorkfilePut,
							Atime:   time.Now(),
							Size:    int64(len(data)),
							OWT:     cmn.OwtPut,
						}
						tassert.CheckFatal(t, mockTarget.PutObject(lom, params))
						defer func() {
							lom.Lock(true)
							defer lom.Unlock(true)
							loadErr := lom.Load(false /*cache it*/, true /*locked*/)
							if cos.IsNotExist(loadErr) {
								return
							}
							tassert.CheckFatal(t, loadErr)
							tassert.CheckFatal(t, lom.RemoveObj())
						}()
						source = lom.FQN
						destination += "-renamed"
						err = mockTarget.objMv(lom, &apc.ActMsg{Name: destination})
					} else {
						source = filepath.Join(t.TempDir(), "source")
						tassert.CheckFatal(t, os.WriteFile(source, data, cos.PermRWR))
						params := &core.PromoteParams{
							Bck:    lom.Bck(),
							Config: cmn.GCO.Get(),
							PromoteArgs: apc.PromoteArgs{
								SrcFQN:       source,
								ObjName:      lom.ObjName,
								OverwriteDst: true,
								DeleteSrc:    true,
							},
						}
						_, err = mockTarget._promote(params, lom)
					}

					remaining, readErr := os.ReadFile(source)
					if test.status < http.StatusMultipleChoices {
						tassert.CheckFatal(t, err)
						tassert.Errorf(t, os.IsNotExist(readErr), "successful %s must remove the source, got %v", operation, readErr)
						return
					}
					tassert.Fatalf(t, err != nil, "expected destination status %d to fail %s", test.status, operation)
					herr := cmn.AsErrHTTP(err)
					tassert.Fatalf(t, herr != nil, "expected an HTTP error, got %v", err)
					tassert.Errorf(t, herr.Status == test.status, "expected HTTP status %d, got %d", test.status, herr.Status)
					tassert.Errorf(t, strings.Contains(herr.Message, test.message), "expected error %q, got %q", test.message, herr.Message)
					want := " (" + mockTarget.String() + ": coi.put " + lom.Bck().Cname(destination) + " " + peer.String() + ")"
					tassert.Errorf(t, strings.HasSuffix(herr.Message, want), "expected error suffix %q, got %q", want, herr.Message)
					if test.s3Code != "" {
						w := httptest.NewRecorder()
						r := httptest.NewRequest(http.MethodPut, "/s3/"+testBucket+"/"+destination, http.NoBody)
						s3.WriteErr(w, r, s3.ErrInfo{Err: err})
						var out s3.Error
						tassert.CheckFatal(t, xml.Unmarshal(w.Body.Bytes(), &out))
						tassert.Errorf(t, out.Code == test.s3Code, "expected S3 code %q, got %q", test.s3Code, out.Code)
					}
					tassert.Errorf(t, readErr == nil && string(remaining) == string(data), "failed %s must preserve source bytes, got %q: %v", operation, remaining, readErr)
				})
			}
		})
	}
}

func setCopyPutTestPeer(t *testing.T, srv *httptest.Server) *meta.Snode {
	t.Helper()
	previousClient := g.client.data
	previousSmap := mockTarget.owner.smap.get()
	previousConfig := cmn.GCO.Get()
	t.Cleanup(func() {
		g.client.data = previousClient
		mockTarget.owner.smap.put(previousSmap)
		cmn.GCO.Put(previousConfig)
	})
	g.client.data = srv.Client()
	config := cmn.GCO.BeginUpdate()
	config.Timeout.SendFile = cos.Duration(5 * time.Second)
	cmn.GCO.CommitUpdate(config)

	peer := &meta.Snode{}
	peer.Init("t-copy-put", apc.Target, nil)
	peer.DataNet.URL = srv.URL
	primary := &meta.Snode{}
	primary.Init("p-copy-put", apc.Proxy, nil)
	smap := newSmap()
	smap.Tmap[peer.ID()] = peer
	smap.Pmap[primary.ID()] = primary
	smap.Primary = primary
	smap.Version = 1
	mockTarget.owner.smap.put(smap)
	return peer
}
