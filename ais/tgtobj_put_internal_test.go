// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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
			for _, status := range []int{http.StatusOK, http.StatusForbidden} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					data := []byte("keep the source")
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						uploaded, err := io.ReadAll(r.Body)
						if err != nil || !bytes.Equal(uploaded, data) {
							http.Error(w, "unexpected upload", http.StatusInternalServerError)
							return
						}
						w.WriteHeader(status)
					}))
					defer srv.Close()
					setCopyPutTestPeer(t, srv)

					lom := core.AllocLOM("remote-copy-put-" + operation)
					defer core.FreeLOM(lom)
					tassert.CheckFatal(t, lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)))
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
						err = mockTarget.objMv(lom, &apc.ActMsg{Name: lom.ObjName + "-renamed"})
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
					if status == http.StatusOK {
						tassert.CheckFatal(t, err)
						tassert.Errorf(t, os.IsNotExist(readErr), "successful %s must remove the source, got %v", operation, readErr)
						return
					}
					tassert.Fatalf(t, err != nil, "expected destination status %d to fail %s", status, operation)
					herr := cmn.AsErrHTTP(err)
					tassert.Fatalf(t, herr != nil, "expected an HTTP error, got %v", err)
					tassert.Errorf(t, herr.Status == status, "expected HTTP status %d, got %d", status, herr.Status)
					tassert.Errorf(t, readErr == nil && bytes.Equal(remaining, data), "failed %s must preserve source bytes, got %q: %v", operation, remaining, readErr)
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
