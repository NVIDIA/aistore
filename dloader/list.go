// Package dloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dloader

import "regexp"

func ListJobs(regex *regexp.Regexp) (any, int, error) {
	var (
		respMap map[string]Job
		records []*downloadJobInfo
		req     = &request{action: actList, regex: regex}
	)
	if dlStore != nil {
		records = dlStore.getList(req.regex)
	}
	if len(records) == 0 {
		req.okRsp(respMap)
		goto ex
	}
	respMap = make(map[string]Job, len(records))
	for _, r := range records {
		respMap[r.ID] = r.ToDlJobInfo()
	}
	req.okRsp(respMap)
ex:
	rsp := req.response
	return rsp.value, rsp.statusCode, rsp.err
}
