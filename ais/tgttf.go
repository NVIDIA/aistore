// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tar2tf"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

// verb /v1/tar2tf
func (t *targetrunner) tar2tfHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httptar2tfget(w, r)
	default:
		t.invalmsghdlr(w, r, r.Method+" operation not supported", 400)
	}
}

// /v1/tar2tf/
func (t *targetrunner) httptar2tfget(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()

		bck      *cluster.Bck
		err      error
		apiItems []string
	)
	apiItems, err = t.checkRESTItems(w, r, 2, true, cmn.Version, cmn.Tar2Tf)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if len(apiItems) < 2 {
		t.invalmsghdlr(w, r, fmt.Sprintf("expected at least 2 api items, got %v", apiItems))
		return
	}

	bucket := apiItems[1]
	bck, err = newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	switch apiItems[0] {
	case cmn.Start:
		var (
			job    *tar2tf.SamplesStreamJob
			jobMsg = tar2tf.SamplesStreamJobMsg{}
		)
		err = jsoniter.NewDecoder(r.Body).Decode(&jobMsg)
		if err == nil {
			job, err = jobMsg.ToSamplesStreamJob(r, w)
		}
		if err == nil {
			if len(job.Selections) != 2 {
				err = fmt.Errorf("expected selections to have length 2")
			}
		}
		if err == nil {
			_, err = xaction.Registry.NewTar2TfXact(job, t, bck)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		job.Wg.Wait()
	case cmn.GetTargetObjects:
		var (
			pt          cmn.ParsedTemplate
			template    string
			objectNames []string
		)
		if len(apiItems) < 3 {
			err = fmt.Errorf("expected at least 3 api items for get target objects")
		}
		if err == nil {
			template = apiItems[2]
			pt, err = cmn.ParseBashTemplate(template)
		}
		if err == nil {
			objectNames = make([]string, 0, cmn.Min(int(pt.Count()), 1000))
			err = cluster.HrwIterMatchingObjects(t, bck, pt, func(lom *cluster.LOM) error {
				objectNames = append(objectNames, lom.ObjName)
				return nil
			})
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		t.writeJSON(w, r, objectNames, cmn.GetTargetObjects)
	default:
		s := fmt.Sprintf("Invalid route /tar2tf/%s", apiItems[0])
		t.invalmsghdlr(w, r, s)
	}
}

func transformTarToTFRecord(goi *getObjInfo, r *cmn.HTTPRange) (written int64, err error) {
	var (
		b []byte
		n int

		start, length int64
	)

	if r != nil {
		start, length = r.Start, r.Length
	} else {
		length, err = tar2tf.Cache.GetSize(goi.lom)
		if err != nil {
			return 0, nil
		}
		start = 0
	}
	b, err = tar2tf.Cache.Get(goi.lom, start, length)
	if err != nil {
		return 0, err
	}

	n, err = goi.w.Write(b)
	if int64(n) != length {
		return 0, fmt.Errorf("written %d, expected to write %d", n, length)
	}

	return int64(n), err
}
