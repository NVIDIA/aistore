// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/etl"
)

func ETLRunningContainers(t *testing.T, params api.BaseParams) etl.InfoList {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	return etls
}

func ETLCheckNoRunningContainers(t *testing.T, params api.BaseParams) {
	etls := ETLRunningContainers(t, params)
	tassert.Fatalf(t, len(etls) == 0, "Expected no ETL running, got %+v", etls)
}
