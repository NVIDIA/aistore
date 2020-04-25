// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

var (
	errInvalidTarget = errors.New("invalid target")
)

// buildDlObjs returns list of objects that must be downloaded by target.
func buildDlObjs(t cluster.Target, bck *cluster.Bck, objects cmn.SimpleKVs) ([]dlObj, error) {
	var (
		smap = t.GetSowner().Get()
		sid  = t.Snode().ID()
	)

	objs := make([]dlObj, 0, len(objects))
	for name, link := range objects {
		job, err := jobForObject(smap, sid, bck, name, link)
		if err != nil {
			if err == errInvalidTarget {
				continue
			}
			return nil, err
		}
		objs = append(objs, job)
	}
	return objs, nil
}

func jobForObject(smap *cluster.Smap, sid string, bck *cluster.Bck, objName, link string) (dlObj, error) {
	objName, err := normalizeObjName(objName)
	if err != nil {
		return dlObj{}, err
	}
	// Make sure that link contains protocol (absence of protocol can result in errors).
	link = cmn.PrependProtocol(link)

	si, err := cluster.HrwTarget(bck.MakeUname(objName), smap)
	if err != nil {
		return dlObj{}, err
	}
	if si.ID() != sid {
		return dlObj{}, errInvalidTarget
	}

	return dlObj{
		objName:   objName,
		link:      link,
		fromCloud: bck.IsCloud(cmn.AnyCloud),
	}, nil
}

// Removes everything that goes after '?', eg. "?query=key..." so it will not
// be part of final object name.
func normalizeObjName(objName string) (string, error) {
	u, err := url.Parse(objName)
	if err != nil {
		return "", nil
	}

	if u.Path == "" {
		return objName, nil
	}

	return url.PathUnescape(u.Path)
}

func ParseStartDownloadRequest(ctx context.Context, r *http.Request, id string, t cluster.Target) (DlJob, error) {
	var (
		// link -> objName
		objects cmn.SimpleKVs
		query   = r.URL.Query()

		payload        = &DlBase{}
		singlePayload  = &DlSingleBody{}
		rangePayload   = &DlRangeBody{}
		multiPayload   = &DlMultiBody{}
		cloudPayload   = &DlCloudBody{}
		objectsPayload interface{}

		description string
	)

	payload.InitWithQuery(query)

	singlePayload.InitWithQuery(query)
	rangePayload.InitWithQuery(query)
	multiPayload.InitWithQuery(query)
	cloudPayload.InitWithQuery(query)

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	if err = singlePayload.Validate(); err == nil {
		if objects, err = singlePayload.ExtractPayload(); err != nil {
			return nil, err
		}
		description = singlePayload.Describe()
	} else if err = rangePayload.Validate(); err == nil {
		// NOTE: size of objects to be downloaded by a target will be unknown
		// So proxy won't be able to sum sizes from all targets when calculating total size
		// This should be taken care of somehow, as total is easy to know from range template anyway
		var pt cmn.ParsedTemplate
		pt, err = cmn.ParseBashTemplate(rangePayload.Template)
		if err != nil {
			return nil, err
		}
		description = rangePayload.Describe()
		bck := cluster.NewBckEmbed(rangePayload.Bck)
		baseJob := newBaseDlJob(id, bck, rangePayload.Timeout, description)
		return newRangeDlJob(t, baseJob, pt, rangePayload.Subdir)
	} else if err = multiPayload.Validate(b); err == nil {
		if err := jsoniter.Unmarshal(b, &objectsPayload); err != nil {
			return nil, err
		}

		if objects, err = multiPayload.ExtractPayload(objectsPayload); err != nil {
			return nil, err
		}
		description = multiPayload.Describe()
	} else if err = cloudPayload.Validate(); err == nil {
		bck := cluster.NewBckEmbed(cloudPayload.Bck)
		if err := bck.Init(t.GetBowner(), t.Snode()); err != nil {
			return nil, err
		}
		if !bck.IsCloud() {
			return nil, errors.New("bucket download requires a cloud bucket")
		}

		baseJob := newBaseDlJob(id, bck, cloudPayload.Timeout, payload.Description)
		return newCloudBucketDlJob(ctx, t, baseJob, cloudPayload.Prefix, cloudPayload.Suffix)
	} else {
		return nil, errors.New("input does not match any of the supported formats (single, range, multi, cloud)")
	}

	if payload.Description == "" {
		payload.Description = description
	}

	bck := cluster.NewBckEmbed(payload.Bck)
	if err = bck.Init(t.GetBowner(), t.Snode()); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok {
			return nil, err
		}
	}
	if !bck.IsAIS() {
		return nil, errors.New("regular download requires ais bucket")
	}
	objs, err := buildDlObjs(t, bck, objects)
	if err != nil {
		return nil, err
	}

	baseJob := newBaseDlJob(id, bck, payload.Timeout, payload.Description)
	return newSliceDlJob(baseJob, objs), nil
}

func headLink(link string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), headReqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return resp, nil
}

func compareObjects(obj dlObj, lom *cluster.LOM) (equal bool, err error) {
	resp, err := headLink(obj.link)
	if err != nil {
		return false, err
	}
	if resp.ContentLength != 0 && resp.ContentLength != lom.Size() {
		return false, nil
	}
	cksum := getCksum(obj.link, resp)
	if cksum != nil {
		computedCksum, err := lom.ComputeCksum(cksum.Type())
		if err != nil || !cmn.EqCksum(cksum, computedCksum) {
			return false, err
		}
	}
	// Cannot prove that the objects are different so assume they are equal.
	return true, nil
}
