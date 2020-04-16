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
	// Number of retries when operation to remote cluster fails with "connection refused".
	aisCloudRetries = 1
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

// TODO: refresh remote Smap every so often
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
	return fmt.Sprintf("remote cluster: %s => (%q, %v, %s)", r.url, aliases, r.smap.UUID, r.smap)
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

// cluster uuid -> [ urls, aliases, other info ]
// TODO: check reachability - ping them all
func (m *AisCloudProvider) GetInfo(clusterConf cmn.CloudConfAIS) (info cmn.CloudInfoAIS) {
	info = make(cmn.CloudInfoAIS, len(m.remote))
	m.mu.RLock()
	defer m.mu.RUnlock()
	for uuid, remAis := range m.remote {
		var (
			aliases []string
			value   = make([]string, 3)
		)
		value[0] = remAis.url
		for a, u := range m.alias {
			if uuid == u {
				aliases = append(aliases, a)
			}
		}
		if len(aliases) == 1 {
			value[1] = aliases[0]
		} else if len(aliases) > 1 {
			value[1] = fmt.Sprintf("%v", aliases)
		}
		value[2] = fmt.Sprintf("%s(storage nodes: %d)", remAis.smap, remAis.smap.CountTargets())
		info[uuid] = value
	}
	// offline
	for alias, clusterURLs := range clusterConf {
		if _, ok := m.alias[alias]; !ok {
			if _, ok = m.remote[alias]; !ok {
				value := make([]string, 3)
				if len(clusterURLs) == 1 {
					value[0] = "<" + clusterURLs[0] + ">"
				} else if len(clusterURLs) > 1 {
					value[0] = fmt.Sprintf("<%v>", clusterURLs)
				}
				value[2] = "<offline>"
				info[alias] = value
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
			glog.Warningf("remote cluster %q via %s: %v", alias, u, err)
			continue
		}
		if remSmap == nil {
			remSmap, url = smap, u
			continue
		}
		if remSmap.UUID != smap.UUID {
			err = fmt.Errorf("%q(%v) references two different remote clusters UUIDs=[%s, %s]",
				alias, confURLs, remSmap.UUID, smap.UUID)
			return
		}
		if remSmap.Version < smap.Version {
			remSmap, url = smap, u
		}
	}
	if remSmap == nil {
		err = fmt.Errorf("failed to reach remote cluster %q via any/all of the configured URLs %v",
			alias, confURLs)
		offline = true
		return
	}
	r.smap, r.url = remSmap, url
	return
}

// for user convenience we are supporting the capability to attach remote cluster
// both by (alias => URL) and (UUID => URL) interchangeably
// with 1 to 1 for UUID mapping, and
// many(aliases) to 1 (cluster) - for aliases
func (m *AisCloudProvider) add(newAis *remAisClust, newAlias string) (err error) {
	newAis.m = m
	tag := "added"
	if newAlias == newAis.smap.UUID {
		// not an alias
		goto ad
	}
	if remAis, ok := m.remote[newAis.smap.UUID]; ok {
		m.alias[newAlias] = newAis.smap.UUID            // another alias
		if newAis.smap.Version >= remAis.smap.Version { // override
			if newAis.url != remAis.url {
				glog.Errorf("Warning: different URLs %s vs %s(new) - overriding...", remAis, newAis)
			} else {
				glog.Infof("%s vs %s(new) - updating...", remAis, newAis)
			}
			tag = "updated"
			goto ad
		} else {
			return fmt.Errorf("attempt to downgrade %s with %s", remAis, newAis)
		}
	}
	if uuid, ok := m.alias[newAlias]; ok {
		remAis, ok := m.remote[uuid]
		cmn.Assert(ok)
		return fmt.Errorf("alias %q is already in use for %s", newAlias, remAis)
	}
	m.alias[newAlias] = newAis.smap.UUID
ad:
	m.remote[newAis.smap.UUID] = newAis
	glog.Infof("%s %s", newAis, tag)
	return
}

func (m *AisCloudProvider) newBaseParams(uuid string) (bp api.BaseParams, err error) {
	var (
		cfg        = cmn.GCO.Get()
		remAis, ok = m.remote[uuid]
	)
	if !ok {
		// double take (see "for user convenience" above)
		var orig = uuid
		if uuid, ok = m.alias[uuid /* alias? */]; !ok {
			err = errors.New("remote ais: unknown UUID/alias " + orig)
			return
		}
		remAis, ok = m.remote[uuid]
		cmn.Assert(ok)
	}
	bp = api.BaseParams{Client: cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout}), URL: remAis.url}
	return
}

////////////////////////////////
// cluster.CloudProvider APIs //
////////////////////////////////

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
