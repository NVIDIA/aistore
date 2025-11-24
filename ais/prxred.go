// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
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
		New: func() any { v := make(url.Values, 4); return v },
	}
)

func qAlloc() url.Values { return qPool.Get().(url.Values) }
func qFree(v url.Values) { clear(v); qPool.Put(v) }

func (p *proxy) redurl(r *http.Request, si *meta.Snode, smapVer, now int64, netIntra, netPub string) string {
	var nodeURL string
	netPub = cos.Left(netPub, cmn.NetPublic)

	if p.si.LocalNet == nil {
		nodeURL = si.URL(netPub)
	} else {
		var local bool
		remote := r.RemoteAddr
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

	// fast path
	if !cmn.HasSpecialSymbols(r.URL.Path) {
		q := qAlloc()
		s := p.qencode(q, smapVer, now)
		qFree(q)
		if r.URL.RawQuery != "" {
			return nodeURL + r.URL.Path + "?" + r.URL.RawQuery + "&" + s
		}
		return nodeURL + r.URL.Path + "?" + s
	}

	// slow path
	// it is conceivable that at some future point we may need to url.Parse nodeURL
	// and then use both scheme and host from the parsed result; not now though (NOTE)
	var (
		scheme = "http"
		host   string
	)
	if strings.HasPrefix(nodeURL, "https://") {
		scheme = "https"
		host = strings.TrimPrefix(nodeURL, "https://")
	} else {
		host = strings.TrimPrefix(nodeURL, "http://")
	}
	q := r.URL.Query()
	s := p.qencode(q, smapVer, now)
	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     r.URL.Path,
		RawQuery: s,
	}
	return u.String()
}

// populate redirect-specific query parameters (PID, timestamp, Smap version)
// and return encoded query string
func (p *proxy) qencode(q url.Values, smapVer, now int64) string {
	q.Set(apc.QparamPID, p.SID())
	q.Set(apc.QparamSmapVer, strconv.FormatInt(smapVer, 16))
	q.Set(apc.QparamUnixTime, unixNano2S(now))
	return q.Encode()
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
