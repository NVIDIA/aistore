// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport/bundle"
)

type (
	CoiParams struct {
		DP        core.DP // copy or transform via data provider, see impl-s: (ext/etl/dp.go, core/ldp.go)
		Xact      core.Xact
		Config    *cmn.Config
		BckTo     *meta.Bck
		ObjnameTo string
		Buf       []byte
		OWT       cmn.OWT
		Finalize  bool // copies and EC (as in poi.finalize())
		DryRun    bool
		LatestVer bool // can be used without changing bucket's 'versioning.validate_warm_get'; see also: QparamLatestVer
		Sync      bool // ditto -  bucket's 'versioning.synchronize'
	}

	COI interface {
		CopyObject(lom *core.LOM, dm *bundle.DataMover, coi *CoiParams) (int64, error)
	}
)
