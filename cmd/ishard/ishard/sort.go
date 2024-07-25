// Package ishard provides utility for shard the initial dataset and integration with dsort
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ishard

import (
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/dsort"
)

const DsortDefaultTimeout = 6 * time.Minute

// Invoke dsort to perform the configured sorting algorithm and output to a new bucket with the suffix `-sorted`
func (is *ISharder) sort(shardNames []string) (string, error) {
	spec := dsort.RequestSpec{
		InputBck:        is.cfg.DstBck,
		InputExtension:  is.cfg.Ext,
		InputFormat:     apc.ListRange{ObjNames: shardNames},
		OutputFormat:    is.cfg.IshardConfig.ShardTemplate,
		OutputShardSize: strconv.FormatInt(is.cfg.MaxShardSize, 10),
		OutputExtension: is.cfg.Ext,
		Description:     "Dsort after ishard",
		OutputBck: cmn.Bck{
			Name:     is.cfg.DstBck.Name + "-sorted",
			Provider: is.cfg.DstBck.Provider,
		},
		Algorithm: is.cfg.Algorithm,
	}

	nlog.Infoln("dsort started...")
	return api.StartDsort(is.baseParams, &spec)
}

// Helper function to wait for the dsort job with a given UUID and timeout
func (is *ISharder) waitSort(dsortManagerUUID string, timeout *time.Duration) error {
	nlog.Infof("waiting for dsort[%s]\n", dsortManagerUUID)

	waitTimeout := DsortDefaultTimeout
	if timeout != nil {
		waitTimeout = *timeout
	}
	deadline := time.Now().Add(waitTimeout)

	for time.Now().Before(deadline) {
		all, err := api.MetricsDsort(is.baseParams, dsortManagerUUID)
		if err != nil {
			return err
		}

		allAborted := true
		allFinished := true
		for _, jmetrics := range all {
			m := jmetrics.Metrics
			allAborted = allAborted && m.Aborted.Load()
			allFinished = allFinished &&
				!m.Aborted.Load() &&
				m.Extraction.Finished &&
				m.Sorting.Finished &&
				m.Creation.Finished
		}
		if allAborted {
			return fmt.Errorf("dsort abort")
		}
		if allFinished {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("dsort timeout")
}
