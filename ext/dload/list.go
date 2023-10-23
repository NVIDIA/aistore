// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import "regexp"

func ListJobs(regex *regexp.Regexp, onlyActive bool) (any, int, error) {
	var (
		respMap map[string]Job
		jobs    []*dljob
		req     = &request{action: actList, regex: regex, onlyActive: onlyActive}
	)
	if g.dlStore != nil {
		jobs = g.dlStore.getList(req)
	}
	if len(jobs) == 0 {
		req.okRsp(respMap)
		goto ex
	}
	respMap = make(map[string]Job, len(jobs))
	for _, dljob := range jobs {
		respMap[dljob.id] = dljob.clone()
	}
	req.okRsp(respMap)
ex:
	rsp := req.response
	return rsp.value, rsp.statusCode, rsp.err
}
