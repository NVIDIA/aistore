// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/certloader"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

//
// API endpoint: load TLS cert and use insecure intra-cluster client to rotate
//

func (h *htrun) daeLoadX509(w http.ResponseWriter, r *http.Request) {
	if err := certloader.Load(); err != nil {
		h.writeErr(w, r, err)
	}
}

// one-off intra-cluster client to skip server cert verification
// (do NOT reuse)
func _x509Client() *http.Client {
	transport := g.client.control.Transport.(*http.Transport).Clone()
	if transport.TLSClientConfig == nil {
		debug.Assert(false, "transport.TLSClientConfig nil (not https?)")
		return g.client.control
	}
	transport.TLSClientConfig.InsecureSkipVerify = true
	return &http.Client{
		Transport: transport,
		Timeout:   g.client.control.Timeout,
	}
}

// compare with generic htrun.call()
// (must be consistent, intra-cluster header- and otherwise)
func (p *proxy) _x509call(si *meta.Snode, smap *smapX, client *http.Client) error {
	reqArgs := cmn.HreqArgs{
		Method: http.MethodPut,
		Path:   apc.URLPathDaeX509.S,
		Base:   si.URL(cmn.NetIntraControl),
	}
	req, errR := reqArgs.Req()
	if errR != nil {
		return errR
	}
	req.Header.Set(apc.HdrSenderIsPrimary, "true")
	req.Header.Set(apc.HdrSenderSmapVer, smap.vstr)
	req.Header.Set(apc.HdrSenderID, p.SID())
	req.Header.Set(apc.HdrSenderName, p.si.Name())
	req.Header.Set(cos.HdrUserAgent, apc.HdrUA)

	resp, err := client.Do(req)
	if err == nil {
		if code := resp.StatusCode; code >= http.StatusBadRequest {
			err = &cmn.ErrHTTP{Message: http.StatusText(code), Status: code}
		}
	}
	if resp != nil && resp.Body != nil {
		cos.DrainReader(resp.Body)
		resp.Body.Close()
	}
	return err
}

// Note:
// - sequential; may take seconds in a large cluster
// - best-effort: if the first remote reload fails, abort immediately
// - otherwise keep going
func (p *proxy) cluLoadX509(w http.ResponseWriter, r *http.Request) {
	// 1. self
	if err := certloader.Load(); err != nil {
		p.writeErr(w, r, err)
		return
	}

	// 2. insecure client
	client := _x509Client()
	defer client.CloseIdleConnections()

	// 3. call one at a time
	var (
		smap = p.owner.smap.get()
		err  error
	)
	debug.Assert(smap.IsPrimary(p.si))

	var first = true
	for _, nodeMap := range []meta.NodeMap{smap.Pmap, smap.Tmap} {
		for _, si := range nodeMap {
			if si.ID() == p.SID() {
				continue
			}
			if err = p._x509call(si, smap, client); err != nil {
				if first {
					p.writeErr(w, r, err) // something must be very wrong
					return
				}
				nlog.Errorf("node %s: x509 reload: %v", si, err)
			}
			first = false
		}
	}
	if err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) callLoadX509(w http.ResponseWriter, r *http.Request, node *meta.Snode, smap *smapX) {
	client := _x509Client()
	defer client.CloseIdleConnections()

	if err := p._x509call(node, smap, client); err != nil {
		p.writeErr(w, r, err)
	}
}
