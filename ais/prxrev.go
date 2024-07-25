// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

type (
	reverseProxy struct {
		cloud   *httputil.ReverseProxy // unmodified GET requests => storage.googleapis.com
		nodes   sync.Map               // map [SID => reverse proxy instance]
		removed struct {
			m  meta.NodeMap // map [SID => self-disabled node]
			mu sync.Mutex
		}
		primary struct {
			rp  *httputil.ReverseProxy
			url string
			mu  sync.Mutex
		}
	}
	singleRProxy struct {
		rp *httputil.ReverseProxy
		u  *url.URL
	}
)

// forward control plane request to the current primary proxy
// return: forf (forwarded or failed) where forf = true means exactly that: forwarded or failed
func (p *proxy) forwardCP(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, s string, origBody ...[]byte) (forf bool) {
	var (
		body []byte
		smap = p.owner.smap.get()
	)
	if !smap.isValid() {
		errmsg := fmt.Sprintf("%s must be starting up: cannot execute", p.si)
		if msg != nil {
			p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s %s: %s", errmsg, msg.Action, s)
		} else {
			p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s %q", errmsg, s)
		}
		return true
	}
	if p.settingNewPrimary.Load() {
		p.writeErrStatusf(w, r, http.StatusServiceUnavailable,
			"%s is in transition, cannot process the request", p.si)
		return true
	}
	if smap.isPrimary(p.si) {
		return
	}
	// We must **not** send any request body when doing HEAD request.
	// Otherwise, the request can be rejected and terminated.
	if r.Method != http.MethodHead {
		if len(origBody) > 0 && len(origBody[0]) > 0 {
			body = origBody[0]
		} else if msg != nil {
			body = cos.MustMarshal(msg)
		}
	}
	primary := &p.rproxy.primary
	primary.mu.Lock()
	if primary.url != smap.Primary.PubNet.URL {
		primary.url = smap.Primary.PubNet.URL
		uparsed, err := url.Parse(smap.Primary.PubNet.URL)
		cos.AssertNoErr(err)
		config := cmn.GCO.Get()
		primary.rp = httputil.NewSingleHostReverseProxy(uparsed)
		primary.rp.Transport = rpTransport(config)
		primary.rp.ErrorHandler = p.rpErrHandler
	}
	primary.mu.Unlock()
	if len(body) > 0 {
		debug.AssertFunc(func() bool {
			l, _ := io.Copy(io.Discard, r.Body)
			return l == 0
		})

		r.Body = io.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // Directly setting `Content-Length` header.
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		pname := smap.Primary.StringEx()
		if msg != nil {
			nlog.Infof("%s: forwarding \"%s:%s\" to the primary %s", p, msg.Action, s, pname)
		} else {
			nlog.Infof("%s: forwarding %q to the primary %s", p, s, pname)
		}
	}
	primary.rp.ServeHTTP(w, r)
	return true
}

func rpTransport(config *cmn.Config) *http.Transport {
	var (
		err       error
		transport = cmn.NewTransport(cmn.TransportArgs{Timeout: config.Client.Timeout.D()})
	)
	if config.Net.HTTP.UseHTTPS {
		transport.TLSClientConfig, err = cmn.NewTLS(config.Net.HTTP.ToTLS())
		if err != nil {
			cos.ExitLog(err)
		}
	}
	return transport
}

// Based on default error handler `defaultErrorHandler` in `httputil/reverseproxy.go`.
func (p *proxy) rpErrHandler(w http.ResponseWriter, r *http.Request, err error) {
	var (
		smap = p.owner.smap.get()
		si   = smap.PubNet2Node(r.URL.Host) // assuming pub
		dst  = r.URL.Host
	)
	if si != nil {
		dst = "node " + si.StringEx()
	}
	if cos.IsErrConnectionRefused(err) {
		nlog.Errorf("%s: %s is unreachable (%s %s)", p, dst, r.Method, r.URL.Path)
	} else {
		nlog.Errorf("%s rproxy to %s (%s %s): %v", p, dst, r.Method, r.URL.Path, err)
	}
	w.WriteHeader(http.StatusBadGateway)
}

func (p *proxy) reverseNodeRequest(w http.ResponseWriter, r *http.Request, si *meta.Snode) {
	parsedURL, err := url.Parse(si.URL(cmn.NetPublic))
	debug.AssertNoErr(err)
	p.reverseRequest(w, r, si.ID(), parsedURL)
}

// usage:
// 1. primary => node in the cluster
// 2. primary => remais
func (p *proxy) reverseRequest(w http.ResponseWriter, r *http.Request, nodeID string, parsedURL *url.URL) {
	rproxy := p.rproxy.loadOrStore(nodeID, parsedURL, p.rpErrHandler)
	rproxy.ServeHTTP(w, r)
}

func (p *proxy) reverseRemAis(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, bck *cmn.Bck, query url.Values) (err error) {
	var (
		backend     = cmn.BackendConfAIS{}
		aliasOrUUID = bck.Ns.UUID
		config      = cmn.GCO.Get()
		v           = config.Backend.Get(apc.AIS)
	)
	if v == nil {
		p.writeErrMsg(w, r, "no remote ais clusters attached")
		return err
	}

	cos.MustMorphMarshal(v, &backend)
	urls, exists := backend[aliasOrUUID]
	if !exists {
		var refreshed bool
		if p.remais.Ver == 0 {
			p._remais(&config.ClusterConfig, true)
			refreshed = true
		}
	ml:
		p.remais.mu.RLock()
		for _, remais := range p.remais.A {
			if remais.Alias == aliasOrUUID || remais.UUID == aliasOrUUID {
				urls = []string{remais.URL}
				exists = true
				break
			}
		}
		p.remais.mu.RUnlock()
		if !exists && !refreshed {
			p._remais(&config.ClusterConfig, true)
			refreshed = true
			goto ml
		}
	}
	if !exists {
		err = cos.NewErrNotFound(p, "remote UUID/alias "+aliasOrUUID)
		p.writeErr(w, r, err)
		return err
	}

	debug.Assert(len(urls) > 0)
	u, err := url.Parse(urls[0])
	if err != nil {
		p.writeErr(w, r, err)
		return err
	}
	if msg != nil {
		body := cos.MustMarshal(msg)
		r.ContentLength = int64(len(body))
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	bck.Ns.UUID = ""
	query = cmn.DelBckFromQuery(query)
	query = bck.AddToQuery(query)
	r.URL.RawQuery = query.Encode()
	p.reverseRequest(w, r, aliasOrUUID, u)
	return nil
}

//////////////////
// reverseProxy //
//////////////////

func (rp *reverseProxy) init() {
	rp.cloud = &httputil.ReverseProxy{
		Director:  func(_ *http.Request) {},
		Transport: rpTransport(cmn.GCO.Get()),
	}
}

func (rp *reverseProxy) loadOrStore(uuid string, u *url.URL,
	errHdlr func(w http.ResponseWriter, r *http.Request, err error)) *httputil.ReverseProxy {
	revProxyIf, exists := rp.nodes.Load(uuid)
	if exists {
		shrp := revProxyIf.(*singleRProxy)
		if shrp.u.Host == u.Host {
			return shrp.rp
		}
	}
	rproxy := httputil.NewSingleHostReverseProxy(u)
	rproxy.Transport = rpTransport(cmn.GCO.Get())
	rproxy.ErrorHandler = errHdlr

	// NOTE: races are rare probably happen only when storing an entry for the first time or when URL changes.
	// Also, races don't impact the correctness as we always have latest entry for `uuid`, `URL` pair (see: L3917).
	rp.nodes.Store(uuid, &singleRProxy{rproxy, u})
	return rproxy
}
