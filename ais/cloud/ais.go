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
	"io/ioutil"
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
		alias string
		url   string
		smap  *cluster.Smap
	}
	aisCloudProvider struct {
		t      cluster.Target
		remote map[string]*remAisClust
	}
)

var (
	_ cluster.CloudProvider = &aisCloudProvider{}
)

// TODO -- FIXME: reload remote Smap every so often OR (***) get notified

func NewAIS(t cluster.Target) cluster.CloudProvider { return &aisCloudProvider{t: t} }

func (m *aisCloudProvider) Configure(v interface{}) error {
	var (
		cfg             = cmn.GCO.Get()
		clusterConf, ok = v.(cmn.CloudConfAIS)
	)
	if !ok {
		return fmt.Errorf("invalid ais cloud config (%+v, %T)", v, v)
	}
	for alias, clusterURLs := range clusterConf {
		var remote = &remAisClust{}
		if err := remote.init(alias, clusterURLs, cfg); err != nil {
			return err
		}
		m.remote[remote.smap.UUID] = remote
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
			glog.Errorf("remote AIS cluster %q via %s: %v", alias, u, err)
			continue
		}
		if remSmap == nil {
			remSmap, url = smap, u
			continue
		}
		if remSmap.UUID != smap.UUID {
			return fmt.Errorf("remote AIS %q(%v) references two different clusters UUIDs=[%s, %s]",
				alias, confURLs, remSmap.UUID, smap.UUID)
		}
		if remSmap.Version < smap.Version {
			remSmap, url = smap, u
		}
	}
	if remSmap == nil {
		return fmt.Errorf("failed to reach remote AIS cluster %q via any/all of the configured URLs %v",
			alias, confURLs)
	}
	r.alias, r.smap, r.url = alias, remSmap, url
	return nil
}

func (m *aisCloudProvider) newBaseParams(uuid string) api.BaseParams {
	var (
		cfg        = cmn.GCO.Get()
		remAis, ok = m.remote[uuid]
	)
	cmn.AssertMsg(ok, "ais cloud: unknown UUID "+uuid)
	return api.BaseParams{Client: cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout}), URL: remAis.url}
}

func (m *aisCloudProvider) ListObjects(ctx context.Context, bck cmn.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	cmn.Assert(bck.Provider == cmn.ProviderAIS)
	err = m.try(func() error {
		bp := m.newBaseParams(bck.Ns.UUID)
		bckList, err = api.ListObjects(bp, bck, msg, 0)
		return err
	})
	err, errCode = extractErrCode(err)
	return bckList, err, errCode
}

func (m *aisCloudProvider) HeadBucket(ctx context.Context, bck cmn.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	bp := m.newBaseParams(bck.Ns.UUID)
	cmn.Assert(bck.Provider == cmn.ProviderAIS)
	err = m.try(func() error {
		p, err := api.HeadBucket(bp, bck)
		bckProps = make(cmn.SimpleKVs)
		cmn.IterFields(p, func(uniqueTag string, field cmn.IterField) (e error, b bool) {
			bckProps[uniqueTag] = fmt.Sprintf("%v", field.Value())
			return nil, false
		})
		return err
	})
	err, errCode = extractErrCode(err)
	return bckProps, err, errCode
}

func (m *aisCloudProvider) ListBuckets(ctx context.Context) (buckets cmn.BucketNames, err error, errCode int) {
	var (
		bck = cmn.Bck{Provider: cmn.ProviderAIS}
	)
	for uuid := range m.remote {
		bp := m.newBaseParams(uuid)
		er := m.try(func() (err error) {
			var names cmn.BucketNames
			names, err = api.ListBuckets(bp, bck)
			if err == nil {
				buckets = append(buckets, names...)
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
		bck = cmn.Bck{Name: lom.BckName(), Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		bp  = m.newBaseParams(bck.Ns.UUID)
	)
	err = m.try(func() error {
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
	bck := cmn.Bck{Name: lom.BckName(), Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
	err = m.try(func() error {
		var (
			r, w  = io.Pipe()
			errCh = make(chan error, 1)
		)

		go func() {
			bp := m.newBaseParams(bck.Ns.UUID)
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
		bck = cmn.Bck{Name: lom.BckName(), Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		bp  = m.newBaseParams(bck.Ns.UUID)
	)
	err = m.try(func() error {
		cksumValue := lom.Cksum().Value()
		args := api.PutObjectArgs{
			BaseParams: bp,
			Bck:        bck,
			Object:     lom.ObjName,
			Hash:       cksumValue,
			Reader:     cmn.NopOpener(ioutil.NopCloser(r)),
			Size:       uint64(lom.Size()),
		}
		return api.PutObject(args)
	})
	err, errCode = extractErrCode(err)
	return lom.Version(), err, errCode
}

func (m *aisCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	var (
		bck = cmn.Bck{Name: lom.BckName(), Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		bp  = m.newBaseParams(bck.Ns.UUID)
	)
	err = m.try(func() error {
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
	//
	// TODO -- FIXME: utilize m.remote[uuid].smap
	//
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
