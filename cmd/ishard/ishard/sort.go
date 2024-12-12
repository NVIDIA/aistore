// Package ishard provides utility for shard the initial dataset and integration with dsort
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ishard

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/tools/readers"
)

const DsortDefaultTimeout = 6 * time.Minute

// Invoke dsort to perform the configured sorting algorithm and output to a new bucket with the suffix `-sorted`
func (is *ISharder) sort(shardNames []string) (string, error) {
	if is.cfg.ShardSize.IsCount {
		return "", errors.New("dsort currently doesn't support count-based output shard size")
	}
	spec := dsort.RequestSpec{
		InputBck:        is.cfg.DstBck,
		InputExtension:  is.cfg.Ext,
		InputFormat:     apc.ListRange{ObjNames: shardNames},
		OutputFormat:    is.cfg.IshardConfig.ShardTemplate,
		OutputShardSize: strconv.FormatInt(is.cfg.ShardSize.Size, 10),
		OutputExtension: is.cfg.Ext,
		Description:     "Dsort after ishard",
		OutputBck: cmn.Bck{
			Name:     is.cfg.DstBck.Name + "-sorted",
			Provider: is.cfg.DstBck.Provider,
		},
		Algorithm: is.cfg.Algorithm,
	}

	// upload EKM order file if specified
	if is.cfg.EKMFlag.IsSet {
		orderFileName := is.cfg.EKMFlag.Path
		if orderFileName == "" {
			orderFileName = "order_file.json"
		}
		args := api.PutArgs{
			BaseParams: is.baseParams,
			Bck:        is.cfg.DstBck,
			ObjName:    orderFileName,
			Reader:     readers.NewBytes(is.cfg.JSONBytes),
		}
		if _, err := api.PutObject(&args); err != nil {
			return "", fmt.Errorf("error uploading order file: %w", err)
		}

		spec.EKMFileURL = fmt.Sprintf(
			"%s/%s/%s/%s/%s?%s=%s",
			is.cfg.URL, apc.Version, apc.Objects, is.cfg.DstBck.Name, orderFileName,
			apc.QparamProvider, is.cfg.DstBck.Provider,
		)

		spec.Config.EKMMissingKey = cmn.AbortReaction
	}

	fmt.Println("dsort started...")
	return api.StartDsort(is.baseParams, &spec)
}

// Helper function to wait for the dsort job with a given UUID and timeout
func (is *ISharder) waitSort(dsortManagerUUID string, timeout *time.Duration) error {
	fmt.Printf("waiting for dsort[%s]\n", dsortManagerUUID)

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
			return errors.New("dsort abort")
		}
		if allFinished {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return errors.New("dsort timeout")
}
