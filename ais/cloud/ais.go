// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	aisCloudRetries = 1                // number of "connection refused" retries
	aisCloudPrefix  = "remote cluster" // log
)

type (
	remAisClust struct {
		url  string
		smap *cluster.Smap
		m    *AisCloudProvider
	}
	AisCloudProvider struct {
		t      cluster.Target
		mu     *sync.RWMutex
		remote map[string]*remAisClust // by UUID:  1 to 1
		alias  map[string]string       // by alias: many to 1 UUID
	}
)

var (
	_ cluster.CloudProvider = &AisCloudProvider{}
)

// TODO - FIXME: review/refactor try{}
// TODO: house-keep refreshing remote Smap
// TODO: utilize m.remote[uuid].smap to load balance and retry disconnects

func NewAIS(t cluster.Target) *AisCloudProvider {
	return &AisCloudProvider{
		t:      t,
		mu:     &sync.RWMutex{},
		remote: make(map[string]*remAisClust),
		alias:  make(map[string]string),
	}
}

func (r *remAisClust) String() string {
	var aliases []string
	for alias, uuid := range r.m.alias {
		if uuid == r.smap.UUID {
			aliases = append(aliases, alias)
		}
	}
	return fmt.Sprintf("%s: %s => (%q, %v, %s)", aisCloudPrefix, r.url, aliases, r.smap.UUID, r.smap)
}

// NOTE: this and the next method are part of the of the *extended* AIS cloud API
//       in addition to the basic GetObj, et al.

// apply new or updated (attach, detach) cmn.CloudConfAIS configuration
func (m *AisCloudProvider) Apply(v interface{}, action string) error {
	var (
		cfg             = cmn.GCO.Get()
		clusterConf, ok = v.(cmn.CloudConfAIS)
	)
	if !ok {
		return fmt.Errorf("invalid ais cloud config (%+v, %T)", v, v)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// detach
	if action == cmn.ActDetach {
		for alias, uuid := range m.alias {
			if _, ok := clusterConf[alias]; !ok {
				if _, ok = clusterConf[uuid]; !ok {
					delete(m.alias, alias)
					delete(m.remote, uuid)
				}
			}
		}
		return nil
	}
	// init and attach
	for alias, clusterURLs := range clusterConf {
		var remAis = &remAisClust{}
		if offline, err := remAis.init(alias, clusterURLs, cfg); err != nil { // and check connectivity
			if offline {
				continue
			}
			return err
		}
		if err := m.add(remAis, alias); err != nil {
			return err
		}
	}
	return nil
}

func (m *AisCloudProvider) GetInfo(clusterConf cmn.CloudConfAIS) (cia cmn.CloudInfoAIS) {
	var (
		cfg    = cmn.GCO.Get()
		client = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout})
	)
	cia = make(cmn.CloudInfoAIS, len(m.remote))
	m.mu.RLock()
	defer m.mu.RUnlock()
	for uuid, remAis := range m.remote {
		var (
			aliases []string
			info    = &cmn.RemoteAISInfo{}
		)
		info.URL = remAis.url
		for a, u := range m.alias {
			if uuid == u {
				aliases = append(aliases, a)
			}
		}
		if len(aliases) == 1 {
			info.Alias = aliases[0]
		} else if len(aliases) > 1 {
			info.Alias = fmt.Sprintf("%v", aliases)
		}
		// online?
		if smap, err := api.GetClusterMap(api.BaseParams{Client: client, URL: remAis.url}); err == nil {
			if smap.UUID != uuid {
				glog.Errorf("%s: unexpected (or changed) uuid %q", remAis, smap.UUID)
				continue
			}
			info.Online = true
			if smap.Version < remAis.smap.Version {
				glog.Errorf("%s: detected older Smap %s - proceeding to override anyway", remAis, smap)
			}
			remAis.smap = smap
		}
		info.Primary = remAis.smap.ProxySI.String()
		info.Smap = remAis.smap.Version
		info.Targets = int32(remAis.smap.CountTargets())
		cia[uuid] = info
	}
	// defunct
	for alias, clusterURLs := range clusterConf {
		if _, ok := m.alias[alias]; !ok {
			if _, ok = m.remote[alias]; !ok {
				info := &cmn.RemoteAISInfo{}
				info.URL = fmt.Sprintf("%v", clusterURLs)
				cia[alias] = info
			}
		}
	}
	return
}

func (r *remAisClust) init(alias string, confURLs []string, cfg *cmn.Config) (offline bool, err error) {
	var (
		url           string
		remSmap, smap *cluster.Smap
		client        = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout})
	)
	for _, u := range confURLs {
		if smap, err = api.GetClusterMap(api.BaseParams{Client: client, URL: u}); err != nil {
			glog.Warningf("%s: failing to reach %q via %s: %v", aisCloudPrefix, alias, u, err)
			continue
		}
		if remSmap == nil {
			remSmap, url = smap, u
			continue
		}
		if remSmap.UUID != smap.UUID {
			err = fmt.Errorf("%q(%v) references two different clusters: uuid=%q vs uuid=%q",
				alias, confURLs, remSmap.UUID, smap.UUID)
			return
		}
		if remSmap.Version < smap.Version {
			remSmap, url = smap, u
		}
	}
	if remSmap == nil {
		err = fmt.Errorf("%s: failed to reach %q via any/all of the configured URLs %v",
			aisCloudPrefix, alias, confURLs)
		offline = true
		return
	}
	r.smap, r.url = remSmap, url
	return
}

// NOTE: supporting remote attachments both by alias and by UUID interchangeably,
//       with mappings: 1(uuid) to 1(cluster) and 1(alias) to 1(cluster)
func (m *AisCloudProvider) add(newAis *remAisClust, newAlias string) (err error) {
	if remAis, ok := m.remote[newAlias]; ok {
		return fmt.Errorf("cannot attach %s: alias %q is already in use as uuid for %s",
			newAlias, newAlias, remAis)
	}
	newAis.m = m
	tag := "added"
	if newAlias == newAis.smap.UUID {
		// not an alias
		goto ad
	}
	// existing
	if remAis, ok := m.remote[newAis.smap.UUID]; ok {
		// can re-alias existing remote cluster
		for alias, uuid := range m.alias {
			if uuid == newAis.smap.UUID {
				delete(m.alias, alias)
			}
		}
		m.alias[newAlias] = newAis.smap.UUID // alias
		if newAis.url != remAis.url {
			glog.Warningf("%s: different new URL %s - overriding", remAis, newAis)
		}
		if newAis.smap.Version < remAis.smap.Version {
			glog.Errorf("%s: detected older Smap %s - proceeding to override anyway", remAis, newAis)
		}
		tag = "updated"
		goto ad
	}
	if uuid, ok := m.alias[newAlias]; ok {
		remAis, ok := m.remote[uuid]
		if !ok {
			delete(m.alias, newAlias)
		} else {
			return fmt.Errorf("cannot attach %s: alias %q is already in use for %s", newAis, newAlias, remAis)
		}
	}
	m.alias[newAlias] = newAis.smap.UUID
ad:
	m.remote[newAis.smap.UUID] = newAis
	glog.Infof("%s %s", newAis, tag)
	return
}

////////////////////////////////
// cluster.CloudProvider APIs //
////////////////////////////////

func (m *AisCloudProvider) newBaseParams(uuid string) (bp api.BaseParams, err error) {
	cfg := cmn.GCO.Get()
	m.mu.RLock()
	remAis, ok := m.remote[uuid]
	if !ok {
		// double take (see "for user convenience" above)
		var orig = uuid
		if uuid, ok = m.alias[uuid /* alias? */]; !ok {
			m.mu.RUnlock()
			err = fmt.Errorf("%s: unknown uuid (or alias?) %q", aisCloudPrefix, orig)
			return
		}
		remAis, ok = m.remote[uuid]
		cmn.Assert(ok)
	}
	m.mu.RUnlock()
	bp = api.BaseParams{Client: cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout}), URL: remAis.url}
	return
}

func (m *AisCloudProvider) ListObjects(ctx context.Context, bck cmn.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	var (
		bp api.BaseParams
	)
	cmn.Assert(bck.Provider == cmn.ProviderAIS)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	err = m.try(func() error {
		bck.Ns.UUID = ""
		bckList, err = api.ListObjects(bp, bck, msg, 0)
		return err
	})
	err, errCode = extractErrCode(err)
	return bckList, err, errCode
}

func (m *AisCloudProvider) HeadBucket(ctx context.Context, bck cmn.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	cmn.Assert(bck.Provider == cmn.ProviderAIS)
	var (
		bp api.BaseParams
	)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	err = m.try(func() error {
		bck.Ns.UUID = "" // (sic! here and elsewhere)
		p, err := api.HeadBucket(bp, bck)
		if err != nil {
			return err
		}
		bckProps = make(cmn.SimpleKVs)
		cmn.IterFields(p, func(uniqueTag string, field cmn.IterField) (e error, b bool) {
			bckProps[uniqueTag] = fmt.Sprintf("%v", field.Value())
			return nil, false
		})
		return nil
	})
	err, errCode = extractErrCode(err)
	return bckProps, err, errCode
}

func (m *AisCloudProvider) ListBuckets(ctx context.Context) (buckets cmn.BucketNames, err error, errCode int) {
	for uuid := range m.remote {
		var (
			bp, _  = m.newBaseParams(uuid)
			selbck = cmn.Bck{Provider: cmn.ProviderAIS}
		)
		er := m.try(func() (err error) {
			var rembcks cmn.BucketNames
			rembcks, err = api.ListBuckets(bp, selbck)
			if err == nil {
				for i, bck := range rembcks {
					bck.Ns.UUID = uuid
					rembcks[i] = bck
				}
				buckets = append(buckets, rembcks...)
			} else {
				glog.Errorf("list-bucket(uuid=%s): %v", uuid, err)
			}
			return
		})
		if er != nil {
			err = er
		}
	}
	err, errCode = extractErrCode(err)
	return
}

func (m *AisCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	var (
		bp  api.BaseParams
		bck = lom.Bck().Bck
	)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	err = m.try(func() error {
		bck.Ns.UUID = ""
		p, err := api.HeadObject(bp, bck, lom.ObjName)
		cmn.IterFields(p, func(uniqueTag string, field cmn.IterField) (e error, b bool) {
			objMeta[uniqueTag] = fmt.Sprintf("%v", field.Value())
			return nil, false
		})
		return err
	})
	err, errCode = extractErrCode(err)
	return objMeta, err, errCode
}

func (m *AisCloudProvider) GetObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
	var (
		bp  api.BaseParams
		bck = lom.Bck().Bck
	)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	err = m.try(func() error {
		var (
			r, w  = io.Pipe()
			errCh = make(chan error, 1)
		)
		go func() {
			bck.Ns.UUID = ""
			goi := api.GetObjectInput{
				Writer: w,
			}
			_, err = api.GetObject(bp, bck, lom.ObjName, goi)
			w.CloseWithError(err)
			errCh <- err
		}()

		err := m.t.PutObject(cluster.PutObjectParams{
			LOM:          lom,
			Reader:       r,
			RecvType:     cluster.ColdGet,
			WorkFQN:      workFQN,
			WithFinalize: false,
		})
		r.CloseWithError(err)
		if err != nil {
			return err
		}
		if err := <-errCh; err != nil {
			return err
		}
		return nil
	})
	return extractErrCode(err)
}

func (m *AisCloudProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	var (
		bp  api.BaseParams
		bck = lom.Bck().Bck
	)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	fh, ok := r.(*cmn.FileHandle)
	cmn.Assert(ok) // http redirect requires Open()
	err = m.try(func() error {
		bck.Ns.UUID = ""
		cksumValue := lom.Cksum().Value()
		args := api.PutObjectArgs{
			BaseParams: bp,
			Bck:        bck,
			Object:     lom.ObjName,
			Hash:       cksumValue,
			Reader:     fh,
			Size:       uint64(lom.Size()),
		}
		return api.PutObject(args)
	})
	err, errCode = extractErrCode(err)
	return lom.Version(), err, errCode
}

func (m *AisCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	var (
		bp  api.BaseParams
		bck = lom.Bck().Bck
	)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	err = m.try(func() error {
		bck.Ns.UUID = ""
		return api.DeleteObject(bp, bck, lom.ObjName)
	})
	return extractErrCode(err)
}

func (m *AisCloudProvider) try(f func() error) (err error) {
	err = f()
	if err == nil {
		return nil
	}
	timeout := cmn.GCO.Get().Timeout.CplaneOperation
	for i := 0; i < aisCloudRetries; i++ {
		err = f()
		if err == nil {
			break
		}
		if !cmn.IsErrConnectionRefused(err) && !cmn.IsErrConnectionReset(err) {
			return err
		}
		time.Sleep(timeout)
	}
	return
}

func extractErrCode(e error) (error, int) {
	if e == nil {
		return nil, http.StatusOK
	}
	httpErr := &cmn.HTTPError{}
	if errors.As(e, &httpErr) {
		return httpErr, httpErr.Status
	}
	return e, http.StatusInternalServerError
}
