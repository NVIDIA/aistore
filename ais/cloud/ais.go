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
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	// Number of retries when operation to remote cluster fails with "connection refused".
	aisCloudRetries = 3
)

type (
	remAisClust struct {
		url  string
		smap *cluster.Smap
		m    *aisCloudProvider
	}
	aisCloudProvider struct {
		t      cluster.Target
		remote map[string]*remAisClust // by UUID:  1 to 1
		alias  map[string]string       // by alias: many to 1 UUID
	}
)

var (
	_ cluster.CloudProvider = &aisCloudProvider{}
)

// TODO - FIXME: review/refactor try{}

// TODO: refresh remote Smap every so often
// TODO: utilize m.remote[uuid].smap

func NewAIS(t cluster.Target) cluster.CloudProvider { return &aisCloudProvider{t: t} }

func (r *remAisClust) String() string {
	var aliases []string
	for alias, uuid := range r.m.alias {
		if uuid == r.smap.UUID {
			aliases = append(aliases, alias)
		}
	}
	return fmt.Sprintf("remote cluster: %s => (%q, %v, %s)", r.url, aliases, r.smap.UUID, r.smap)
}

func (m *aisCloudProvider) Configure(v interface{}) error {
	var (
		cfg             = cmn.GCO.Get()
		clusterConf, ok = v.(cmn.CloudConfAIS)
	)
	if !ok {
		return fmt.Errorf("invalid ais cloud config (%+v, %T)", v, v)
	}
	m.remote = make(map[string]*remAisClust, len(clusterConf))
	m.alias = make(map[string]string, len(clusterConf))
	for alias, clusterURLs := range clusterConf {
		var remAis = &remAisClust{}
		if err := remAis.init(alias, clusterURLs, cfg); err != nil {
			return err
		}
		if err := m.add(remAis, alias); err != nil {
			return err
		}
	}
	return nil
}

func (r *remAisClust) init(alias string, confURLs []string, cfg *cmn.Config) (err error) {
	var (
		url           string
		remSmap, smap *cluster.Smap
		client        = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout})
	)
	for _, u := range confURLs {
		if smap, err = api.GetClusterMap(api.BaseParams{Client: client, URL: u}); err != nil {
			glog.Errorf("remote cluster %q via %s: %v", alias, u, err)
			continue
		}
		if remSmap == nil {
			remSmap, url = smap, u
			continue
		}
		if remSmap.UUID != smap.UUID {
			return fmt.Errorf("%q(%v) references two different remote clusters UUIDs=[%s, %s]",
				alias, confURLs, remSmap.UUID, smap.UUID)
		}
		if remSmap.Version < smap.Version {
			remSmap, url = smap, u
		}
	}
	if remSmap == nil {
		return fmt.Errorf("failed to reach remote cluster %q via any/all of the configured URLs %v",
			alias, confURLs)
	}
	r.smap, r.url = remSmap, url
	return nil
}

// for user convenience, supporting both (alias, URL) and (UUID, URL) attachments
// - with 1 to 1 for UUID mapping and
// - many(aliases) to 1 (cluster) for aliases
func (m *aisCloudProvider) add(newAis *remAisClust, newAlias string) (err error) {
	newAis.m = m
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
				glog.Infof("%s vs %s(new) - overriding...", remAis, newAis)
			}
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
	glog.Infof("%s added", newAis)
	return
}

func (m *aisCloudProvider) newBaseParams(uuid string) (bp api.BaseParams, err error) {
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

func (m *aisCloudProvider) ListObjects(ctx context.Context, bck cmn.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	var (
		bp api.BaseParams
	)
	cmn.Assert(bck.Provider == cmn.ProviderAIS)
	if bp, err = m.newBaseParams(bck.Ns.UUID); err != nil {
		return
	}
	err = m.try(func() error {
		bckList, err = api.ListObjects(bp, bck, msg, 0)
		return err
	})
	err, errCode = extractErrCode(err)
	return bckList, err, errCode
}

func (m *aisCloudProvider) HeadBucket(ctx context.Context, bck cmn.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
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

func (m *aisCloudProvider) ListBuckets(ctx context.Context) (buckets cmn.BucketNames, err error, errCode int) {
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

func (m *aisCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
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

func (m *aisCloudProvider) GetObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
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

func (m *aisCloudProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
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

func (m *aisCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
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

func (m *aisCloudProvider) try(f func() error) (err error) {
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
