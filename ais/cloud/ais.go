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

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	// Number of retries when operation to remote cluster fails with "connection refused".
	aisCloudRetries = 3
)

type (
	aisCloudProvider struct {
		t           cluster.Target
		clusterConf cmn.CloudConfAIS

		contactURL string   // selected URL to which currently we should contact
		urls       []string // URLs provider by the user to which we should contact
		failedURLs []string // URLs that failed to contact
	}
)

var (
	_ cluster.CloudProvider = &aisCloudProvider{}
)

func NewAIS(t cluster.Target, clusterConf cmn.CloudConfAIS) (cluster.CloudProvider, error) {
	var (
		found      bool
		url        string
		failedURLs []string
		urls       []string
		cfg        = cmn.GCO.Get()
	)

	for _, clusterURLs := range clusterConf {
		urls = clusterURLs
		break
	}

	for _, url = range urls {
		smap, err := api.GetClusterMap(api.BaseParams{
			Client: cmn.NewClient(cmn.TransportArgs{
				Timeout: cfg.Client.TimeoutLong,
			}),
			URL: url,
		})
		if err != nil {
			failedURLs = append(failedURLs, url)
			continue
		}

		found = true
		// Extend contact URLs with proxies of different cluster.
		for _, node := range smap.Pmap {
			urls = append(urls, node.URL(cmn.NetworkPublic))
		}
		break
	}

	if !found {
		return nil, fmt.Errorf("failed to contact any of the provided URLs: %v", urls)
	}

	return &aisCloudProvider{
		t:           t,
		clusterConf: clusterConf,
		contactURL:  url,
		urls:        urls,
		failedURLs:  failedURLs,
	}, nil
}

func (m *aisCloudProvider) newBaseParams() api.BaseParams {
	cfg := cmn.GCO.Get()
	return api.BaseParams{
		Client: cmn.NewClient(cmn.TransportArgs{
			Timeout: cfg.Client.TimeoutLong,
		}),
		URL: m.contactURL,
	}
}

func (m *aisCloudProvider) ListObjects(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	err = m.try(func() error {
		bp := m.newBaseParams()
		bckList, err = api.ListObjects(bp, cmn.Bck{Name: bucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}, msg, 0)
		return err
	})
	err, errCode = extractErrCode(err)
	return bckList, err, errCode
}

func (m *aisCloudProvider) HeadBucket(ctx context.Context, bucket string) (bckProps cmn.SimpleKVs, err error, errCode int) {
	err = m.try(func() error {
		bp := m.newBaseParams()

		p, err := api.HeadBucket(bp, cmn.Bck{Name: bucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
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
	err = m.try(func() error {
		bp := m.newBaseParams()
		names, err := api.ListBuckets(bp, cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
		if err != nil {
			return err
		}
		buckets = names
		return err
	})
	err, errCode = extractErrCode(err)
	return buckets, err, errCode
}

func (m *aisCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	err = m.try(func() error {
		var (
			bp  = m.newBaseParams()
			bck = cmn.Bck{
				Name:     lom.BckName(),
				Provider: cmn.ProviderAIS,
				Ns:       cmn.NsGlobal,
			}
		)
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
	err = m.try(func() error {
		var (
			r, w  = io.Pipe()
			errCh = make(chan error, 1)
			bck   = cmn.Bck{
				Name:     lom.BckName(),
				Provider: cmn.ProviderAIS,
				Ns:       cmn.NsGlobal,
			}
		)

		go func() {
			bp := m.newBaseParams()
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
	err = m.try(func() error {
		cksumValue := lom.Cksum().Value()
		args := api.PutObjectArgs{
			BaseParams: m.newBaseParams(),
			Bck:        cmn.Bck{Name: lom.BckName(), Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
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
	bp := m.newBaseParams()
	err = m.try(func() error {
		bck := cmn.Bck{Name: lom.BckName(), Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		return api.DeleteObject(bp, bck, lom.ObjName)
	})
	return extractErrCode(err)
}

func (m *aisCloudProvider) try(f func() error) error {
	err := f()
	if err == nil {
		return nil
	}

	timeout := cmn.GCO.Get().Timeout.CplaneOperation
	for i := 0; i < aisCloudRetries; i++ {
		err = f()
		if err == nil || cmn.IsErrConnectionReset(err) {
			break
		}
		if !cmn.IsErrConnectionRefused(err) && !cmn.IsErrConnectionReset(err) {
			return err
		}
		// In case connection refused we should try again after a short break.
		if cmn.IsErrConnectionRefused(err) {
			time.Sleep(timeout)
		}
	}

	m.failedURLs = append(m.failedURLs, m.contactURL)

	cfg := cmn.GCO.Get()
	for _, url := range m.urls {
		if cmn.StringInSlice(url, m.failedURLs) {
			continue
		}

		smap, err := api.GetClusterMap(api.BaseParams{
			Client: cmn.NewClient(cmn.TransportArgs{
				Timeout: cfg.Client.TimeoutLong,
			}),
			URL: url,
		})
		if err != nil {
			m.failedURLs = append(m.failedURLs, url)
			continue
		}

		// Extend contact URLs with proxies of different cluster.
		for _, node := range smap.Pmap {
			nodeURL := node.URL(cmn.NetworkPublic)
			if cmn.StringInSlice(nodeURL, m.failedURLs) || cmn.StringInSlice(nodeURL, m.urls) {
				continue
			}
			m.urls = append(m.urls, nodeURL)
		}
		m.contactURL = url
		break
	}

	return f()
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
