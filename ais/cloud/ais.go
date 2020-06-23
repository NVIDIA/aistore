// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

		uuid string
		bp   api.BaseParams
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

// At the same time a cluster may have registered both types of remote AIS
// clusters(HTTP and HTTPS). So, the method must use both kind of clients and
// select the correct one at the moment it sends a request.
func (m *AisCloudProvider) GetInfo(clusterConf cmn.CloudConfAIS) (cia cmn.CloudInfoAIS) {
	var (
		cfg         = cmn.GCO.Get()
		httpClient  = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout})
		httpsClient = cmn.NewClient(cmn.TransportArgs{
			Timeout:    cfg.Client.Timeout,
			UseHTTPS:   true,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
	)
	cia = make(cmn.CloudInfoAIS, len(m.remote))
	m.mu.RLock()
	defer m.mu.RUnlock()
	for uuid, remAis := range m.remote {
		var (
			aliases []string
			info    = &cmn.RemoteAISInfo{}
		)
		client := httpClient
		if cmn.IsHTTPS(remAis.url) {
			client = httpsClient
		}
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

// A list of remote AIS URLs can contains both HTTP and HTTPS links at the
// same time. So, the method must use both kind of clients and select the
// correct one at the moment it sends a request. First successful request
// saves the good client for the future usage.
func (r *remAisClust) init(alias string, confURLs []string, cfg *cmn.Config) (offline bool, err error) {
	var (
		url           string
		remSmap, smap *cluster.Smap
		httpClient    = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout})
		httpsClient   = cmn.NewClient(cmn.TransportArgs{
			Timeout:    cfg.Client.Timeout,
			UseHTTPS:   true,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
	)
	for _, u := range confURLs {
		client := httpClient
		if cmn.IsHTTPS(u) {
			client = httpsClient
		}
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
	if cmn.IsHTTPS(url) {
		r.bp = api.BaseParams{Client: httpsClient, URL: url}
	} else {
		r.bp = api.BaseParams{Client: httpClient, URL: url}
	}
	r.uuid = remSmap.UUID
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

func (m *AisCloudProvider) Provider() string {
	return cmn.ProviderAIS
}

func (m *AisCloudProvider) remoteCluster(uuid string) (*remAisClust, error) {
	m.mu.RLock()
	remAis, ok := m.remote[uuid]
	if !ok {
		// double take (see "for user convenience" above)
		var orig = uuid
		if uuid, ok = m.alias[uuid /* alias? */]; !ok {
			m.mu.RUnlock()
			return nil, fmt.Errorf("%s: unknown uuid (or alias) %q", aisCloudPrefix, orig)
		}
		remAis, ok = m.remote[uuid]
		cmn.Assert(ok)
	}
	m.mu.RUnlock()
	return remAis, nil
}

func (m *AisCloudProvider) ListObjects(ctx context.Context, remoteBck *cluster.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	cmn.Assert(remoteBck.Provider == cmn.ProviderAIS)

	aisCluster, err := m.remoteCluster(remoteBck.Ns.UUID)
	if err != nil {
		return nil, err, errCode
	}
	err = m.try(remoteBck.Bck, func(bck cmn.Bck) error {
		bckList, err = api.ListObjects(aisCluster.bp, bck, msg, 0)
		return err
	})
	err, errCode = extractErrCode(err)
	return bckList, err, errCode
}

func (m *AisCloudProvider) HeadBucket(ctx context.Context, remoteBck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	cmn.Assert(remoteBck.Provider == cmn.ProviderAIS)

	aisCluster, err := m.remoteCluster(remoteBck.Ns.UUID)
	if err != nil {
		return nil, err, errCode
	}
	err = m.try(remoteBck.Bck, func(bck cmn.Bck) error {
		p, err := api.HeadBucket(aisCluster.bp, bck)
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

func (m *AisCloudProvider) listBucketsCluster(uuid string, query cmn.QueryBcks) (buckets cmn.BucketNames, err error) {
	var (
		aisCluster, _ = m.remoteCluster(uuid)
		remoteQuery   = cmn.QueryBcks{Provider: cmn.ProviderAIS, Ns: cmn.Ns{Name: query.Ns.Name}}
	)
	err = m.try(cmn.Bck{}, func(_ cmn.Bck) (err error) {
		buckets, err = api.ListBuckets(aisCluster.bp, remoteQuery)
		if err != nil {
			glog.Errorf("list-bucket(uuid=%s): %v", uuid, err)
			return err
		}
		for i, bck := range buckets {
			bck.Ns.UUID = uuid // if `uuid` is alias we need to preserve it
			buckets[i] = bck
		}
		return err
	})
	return
}

func (m *AisCloudProvider) ListBuckets(ctx context.Context, query cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	if !query.Ns.IsAnyRemote() {
		buckets, err = m.listBucketsCluster(query.Ns.UUID, query)
	} else {
		for uuid := range m.remote {
			remoteBcks, tryErr := m.listBucketsCluster(uuid, query)
			buckets = append(buckets, remoteBcks...)
			if tryErr != nil {
				err = tryErr
			}
		}
	}
	err, errCode = extractErrCode(err)
	return
}

func (m *AisCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	var (
		remoteBck = lom.Bck().Bck
	)
	aisCluster, err := m.remoteCluster(remoteBck.Ns.UUID)
	if err != nil {
		return nil, err, errCode
	}
	err = m.try(remoteBck, func(bck cmn.Bck) error {
		p, err := api.HeadObject(aisCluster.bp, bck, lom.ObjName)
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
		remoteBck = lom.Bck().Bck
	)
	aisCluster, err := m.remoteCluster(remoteBck.Ns.UUID)
	if err != nil {
		return err, errCode
	}
	err = m.try(remoteBck, func(bck cmn.Bck) error {
		var (
			r, w  = io.Pipe()
			errCh = make(chan error, 1)
		)
		go func() {
			goi := api.GetObjectInput{
				Writer: w,
			}
			_, err = api.GetObject(aisCluster.bp, bck, lom.ObjName, goi)
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
		remoteBck = lom.Bck().Bck
	)

	aisCluster, err := m.remoteCluster(remoteBck.Ns.UUID)
	if err != nil {
		return "", err, errCode
	}
	fh, ok := r.(*cmn.FileHandle) // `PutObject` closes file handle.
	cmn.Assert(ok)                // HTTP redirect requires Open().
	err = m.try(remoteBck, func(bck cmn.Bck) error {
		args := api.PutObjectArgs{
			BaseParams: aisCluster.bp,
			Bck:        bck,
			Object:     lom.ObjName,
			Cksum:      lom.Cksum(),
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
		remoteBck = lom.Bck().Bck
	)
	aisCluster, err := m.remoteCluster(remoteBck.Ns.UUID)
	if err != nil {
		return err, errCode
	}
	err = m.try(remoteBck, func(bck cmn.Bck) error {
		return api.DeleteObject(aisCluster.bp, bck, lom.ObjName)
	})
	return extractErrCode(err)
}

func (m *AisCloudProvider) try(remoteBck cmn.Bck, f func(bck cmn.Bck) error) (err error) {
	remoteBck.Ns.UUID = ""
	for i := 0; i < aisCloudRetries+1; i++ {
		err = f(remoteBck)
		if err == nil || !(cmn.IsErrConnectionRefused(err) || cmn.IsErrConnectionReset(err)) {
			break
		}
		time.Sleep(cmn.GCO.Get().Timeout.CplaneOperation)
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
