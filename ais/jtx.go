// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// jtx (Job, Task, eXtended action) takes care of ownership of these entities.
// It maps an entity (uuid) to its status (metadata). All of this is stored in
// table that is distributed across the other proxies. When entity is created
// the owner is assigned. The proxy maintains its own (owned) entities as well
// as entities from other proxies (so it knows the owner and knows where the
// request should be redirected). All notifications and handling around this
// entity is done exclusively by the owner. Other proxies must redirect any
// request that they do not own, to the owner.
//
// TODO:
//  * (easy) Broadcasting the table is done at every `add` - we should do that periodically.
//  * (hard) Broadcasting a table could be done via some gossip algorithm.
//  * (easy) We should also broadcast the finished time (if set) so other proxies do not
//    need to redirect if they see that the entity/entry has finished.
//  * (easy/medium) Add unit tests.
//  * (hard) Add integration stress tests - proxy dies, target dies etc.
//  * (medium) Extend support for multiple owners.
//  * (medium/hard) Decide if we should be owner or maybe we should assign other proxy.

const (
	// How long we should wait to make another check to see if the entry exist.
	// The entry could be on its way and was just added therefore we don't need
	// to do a broadcast to other proxies to find it out (see: `redirectToOwner`).
	reCheckInterval = time.Second
)

type (
	jtx struct {
		p       *proxyrunner
		mtx     sync.RWMutex
		entries map[string]*jtxEntry
	}

	jtxEntry struct {
		owners cmn.StringSet // DaemonID of owner Snodes
	}
)

var (
	_ json.Marshaler   = &jtx{}
	_ json.Unmarshaler = &jtx{}
)

func (e *jtxEntry) isOwner(daemonID string) bool {
	return e.owners.Contains(daemonID)
}

func newJTX(p *proxyrunner) *jtx {
	v := &jtx{
		p:       p,
		entries: make(map[string]*jtxEntry),
	}
	p.GetSowner().Listeners().Reg(v)
	return v
}

func (o *jtx) addEntry(uuid string, nl notifListener) {
	cmn.Assert(uuid != "")
	cmn.Assert(!o.p.notifs.isOwner(uuid))
	nl.setOwned(true)
	o.p.notifs.add(uuid, nl)

	// TODO: broadcast periodically, not on every entry. Remove addEntry method when this is complete.
	o.broadcastTable()
}

// PRE-CONDITION: Must be under `o.mtx.Lock()`.
func (o *jtx) removeEntry(uuid string) {
	delete(o.entries, uuid)
}

func (o *jtx) entry(uuid string) (entry *jtxEntry, exists bool) {
	o.mtx.RLock()
	entry, exists = o.entries[uuid]
	o.mtx.RUnlock()
	return
}

func (o *jtx) ListenSmapChanged() {
	smap := o.p.GetSowner().Get()
	o.mtx.Lock()
	for uuid, entry := range o.entries {
		// Remove entry if the proxy is no longer part of the cluster.
		// TODO: check all owners.
		if smap.GetProxy(entry.owners.Keys()[0]) == nil {
			o.removeEntry(uuid)
			continue
		}
	}
	o.mtx.Unlock()
}

func (o *jtx) String() string { return "jtx" }

func (o *jtx) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte('"')
	buf.WriteString(o.p.Snode().DaemonID)
	buf.WriteByte(',')
	o.p.notifs.forEach(func(uuid string, nl notifListener) {
		// TODO: Also include status info RUNNING|FINISHED|ABORTED...
		buf.WriteString(uuid)
		buf.WriteByte(',')
	}, isOwned)

	buf.WriteByte('"')
	return buf.Bytes(), nil
}

func (o *jtx) UnmarshalJSON(b []byte) error {
	b = bytes.Trim(b, `",`)
	parts := bytes.Split(b, []byte{','})
	if len(b) == 0 || len(parts) == 0 {
		return errors.New("not enough parts")
	}

	daemonID := string(parts[0])
	if fromProxy := o.p.GetSowner().Get().GetProxy(daemonID); fromProxy == nil {
		return fmt.Errorf("proxy %q not exist", parts[0])
	}
	o.mtx.Lock()
	// 1. Remove entries which are owned by `fromProxy`.
	for uuid, entry := range o.entries {
		if entry.isOwner(daemonID) {
			delete(entry.owners, daemonID)
			if len(entry.owners) == 0 {
				o.removeEntry(uuid)
			}
		}
	}

	// 2. Populate with entries which are owned by `fromProxy`.
	for _, b := range parts[1:] {
		debug.Assert(len(b) > 0)
		uuid := string(b)

		// Note: should have only one owner for now
		cmn.Assert(!o.p.notifs.isOwner(uuid))
		o.entries[uuid] = &jtxEntry{
			owners: cmn.NewStringSet(daemonID),
		}
	}
	o.mtx.Unlock()
	return nil
}

// HTTP STUFF

// verb /v1/jtx
func (o *jtx) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		uuid := r.URL.Query().Get(cmn.URLParamUUID)
		if o.p.notifs.isOwner(uuid) {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case http.MethodPut:
		if err := jsp.Decode(r.Body, o, jsp.CCSign(), "jtx"); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
	default:
		cmn.Assert(false)
	}
}

func (o *jtx) broadcastTable() {
	var (
		path  = cmn.URLPath(cmn.Version, cmn.Jtx)
		table = jsp.EncodeBuf(o, jsp.CCSign())
		_     = o.p.callProxies(http.MethodPut, path, table)
	)
}

func (o *jtx) redirectToOwner(w http.ResponseWriter, r *http.Request, uuid string, msg interface{}) (redirected bool) {
	cmn.Assert(uuid != "")
	if o.p.notifs.isOwner(uuid) {
		return
	}

	if msg != nil {
		body := cmn.MustMarshal(msg)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}

	reCheck := true
Check:
	entry, exists := o.entry(uuid)
	if !exists {
		// If we don't see the entry then maybe the new table is on the way in
		// the network and we just need to give it some time to arrive.
		if reCheck {
			time.Sleep(reCheckInterval)
			reCheck = false
			goto Check
		}

		// Owner not in the table, find out...
		var (
			path    = cmn.URLPath(cmn.Version, cmn.Jtx)
			query   = url.Values{cmn.URLParamUUID: []string{uuid}}
			results = o.p.callProxies(http.MethodGet, path, nil, query)
		)
		for result := range results {
			if result.status == http.StatusNotFound {
				continue
			}
			if result.err != nil {
				glog.Error(result.err)
				continue
			}
			if result.status == http.StatusOK {
				o.p.reverseNodeRequest(w, r, result.si)
				return true
			}
		}
		o.p.invalmsghdlrf(w, r, "%q not found", uuid)
		return true
	}

	// Pick random owner and forward the request.
	owner := entry.owners.Keys()[rand.Intn(len(entry.owners))]
	ownerNode := o.p.GetSowner().Get().GetNode(owner)
	o.p.reverseNodeRequest(w, r, ownerNode)
	return true
}

func (o *jtx) checkEntry(w http.ResponseWriter, r *http.Request, uuid string) (nl notifListener, ok bool) {
	nl, exists := o.p.notifs.entry(uuid)
	if !exists {
		o.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found", uuid)
		return
	}
	if nl.finished() {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		o.p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished", uuid)
		return
	}
	return nl, true
}
