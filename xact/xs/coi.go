// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
		core.GetROC
	}
	CoiRes struct {
		Err   error
		Lsize int64
		Ecode int
		RGET  bool // when reading source via backend.GetObjReader
	}

	COI interface {
		CopyObject(lom *core.LOM, dm *bundle.DataMover, coi *CoiParams) CoiRes
	}
)
