// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	llcFactory struct {
		t    cluster.Target
		xact *xactLLC
		uuid string
	}
	xactLLC struct {
		xaction.XactBckJog
	}
)

// interface guard
var (
	_ cluster.Xact    = (*xactLLC)(nil)
	_ xreg.BckFactory = (*llcFactory)(nil)
)

////////////////
// llcFactory //
////////////////

func (*llcFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	return &llcFactory{t: args.T, uuid: args.UUID}
}

func (p *llcFactory) Start(bck cmn.Bck) error {
	xact := newXactLLC(p.t, p.uuid, bck)
	p.xact = xact
	go xact.Run()
	return nil
}

func (*llcFactory) Kind() string        { return cmn.ActLoadLomCache }
func (p *llcFactory) Get() cluster.Xact { return p.xact }

// overriding xreg.BaseBckEntry because it would return `false, nil`.
func (*llcFactory) PreRenewHook(_ xreg.BucketEntry) (bool, error) { return true, nil }
func (*llcFactory) PostRenewHook(_ xreg.BucketEntry)              {}

/////////////
// xactLLC //
/////////////

func newXactLLC(t cluster.Target, uuid string, bck cmn.Bck) *xactLLC {
	return &xactLLC{
		XactBckJog: *xaction.NewXactBckJog(uuid, cmn.ActLoadLomCache, bck, &mpather.JoggerGroupOpts{
			T:        t,
			Bck:      bck,
			CTs:      []string{fs.ObjectType},
			VisitObj: func(_ *cluster.LOM, _ []byte) error { return nil },
			DoLoad:   mpather.Load,
		}),
	}
}

func (r *xactLLC) Run() {
	r.XactBckJog.Run()
	glog.Infoln(r.String())
	err := r.XactBckJog.Wait()
	r.Finish(err)
}
