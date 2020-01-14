// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

// BuildDownloaderInput takes payload, extracted from user's request and returnes DlBody
// which contains metadata of objects supposed to be downloaded by target t
func BuildDownloaderInput(t cluster.Target, id string, bck *cluster.Bck,
	payload *cmn.DlBase, objects cmn.SimpleKVs) (*cmn.DlBody, error) {
	var (
		err    error
		dlBody = &cmn.DlBody{ID: id}
	)
	dlBody.Bucket = bck.Name
	dlBody.Provider = bck.Provider
	dlBody.Timeout = payload.Timeout
	dlBody.Description = payload.Description

	dlBody.Objs, err = GetTargetDlObjs(t, objects, bck)
	return dlBody, err
}

func GetTargetDlObjs(t cluster.Target, objects cmn.SimpleKVs, bck *cluster.Bck) ([]cmn.DlObj, error) {
	// Filter out objects that will be handled by other targets
	dlObjs := make([]cmn.DlObj, 0, len(objects))
	smap := t.GetSowner().Get()
	for objName, link := range objects {
		// Make sure that objName doesn't contain "?query=smth" suffix.
		objName, err := cmn.NormalizeObjName(objName)
		if err != nil {
			return nil, err
		}
		// Make sure that link contains protocol (absence of protocol can result in errors).
		link = cmn.PrependProtocol(link)

		si, err := cluster.HrwTarget(bck.MakeUname(objName), smap)
		if err != nil {
			return nil, err
		}
		if si.ID() != t.Snode().ID() {
			continue
		}

		dlObjs = append(dlObjs, cmn.DlObj{
			Objname:   objName,
			Link:      link,
			FromCloud: cmn.IsProviderCloud(bck.Provider),
		})
	}

	return dlObjs, nil
}
