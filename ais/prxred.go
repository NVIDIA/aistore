// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

var (
	qPool = sync.Pool{
		New: func() any { v := make(url.Values, 5); return v },
	}
)

func qAlloc() url.Values { return qPool.Get().(url.Values) }
func qFree(v url.Values) { clear(v); qPool.Put(v) }

func (p *proxy) redurl(r *http.Request, si *meta.Snode, smapVer, now int64, netIntra, netPub string) (out string) {
	var (
		nodeURL string // dst node
	)
	netPub = cos.Left(netPub, cmn.NetPublic)
	if p.si.LocalNet == nil {
		nodeURL = si.URL(netPub)
	} else {
		var (
			remote = r.RemoteAddr
			local  bool
		)
		if colon := strings.Index(remote, ":"); colon != -1 {
			remote = remote[:colon]
		}
		if ip := net.ParseIP(remote); ip != nil {
			local = p.si.LocalNet.Contains(ip)
		}
		if local {
			nodeURL = si.URL(netIntra)
		} else {
			nodeURL = si.URL(netPub)
		}
	}

	var (
		sign    *signer
		enabled = cmn.Rom.CSKEnabled()
		special = cmn.HasSpecialSymbols(r.URL.Path)
	)
	switch {
	case enabled:
		sb := sbAlloc()
		defer sbFree(sb)
		size := len(nodeURL) + len(r.URL.Path) + len(r.URL.RawQuery) + cskURLOverhead // !special size; not optimizing
		sb.Reset(size, true)
		sign = &signer{
			r:       r,
			h:       &p.htrun,
			sb:      sb,
			smapVer: smapVer,
			nonce:   p.owner.csk.nonce.Add(1),
		}
		sign.compute(p.SID())
		if !special {
			// fast signing path
			out = sign.buildURL(nodeURL, now)
			break
		}
		fallthrough
	case !enabled && special:
		scheme, host, q := _preparse(nodeURL, r)
		raw := p.qencode(q, now, sign)
		u := url.URL{
			Scheme:   scheme,
			Host:     host,
			Path:     r.URL.Path,
			RawQuery: raw,
		}
		out = u.String()
	default:
		// fast path
		q := qAlloc()
		raw := p.qencode(q, now, nil)
		qFree(q)
		if r.URL.RawQuery != "" {
			return nodeURL + r.URL.Path + "?" + r.URL.RawQuery + "&" + raw
		}
		out = nodeURL + r.URL.Path + "?" + raw
	}

	// in the future, we may need to parse nodeURL and use both scheme and host
	return out
}

func _preparse(nodeURL string, r *http.Request) (scheme, host string, q url.Values) {
	scheme = "http"
	if strings.HasPrefix(nodeURL, "https://") {
		scheme = "https"
		host = strings.TrimPrefix(nodeURL, "https://")
	} else {
		host = strings.TrimPrefix(nodeURL, "http://")
	}
	return scheme, host, r.URL.Query()
}

// http-redirect(with-json-message)
func (p *proxy) redirectAction(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string, msg *apc.ActMsg) {
	started := time.Now()
	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(msg.Action, bck.Cname(objName), "=>", si.StringEx())
	}

	redurl := p.redurl(r, si, smap.Version, started.UnixNano(), cmn.NetIntraControl, "")
	http.Redirect(w, r, redurl, http.StatusTemporaryRedirect)
}
