// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
)

type (
	BucketXact struct {
		cmn.XactBase
		t     cluster.Target
		ctx   context.Context
		bckTo *cluster.Bck

		bckMsg *OfflineMsg
		comm   Communicator
	}
)

func NewBucketXact(t cluster.Target, id string, bckFrom, bckTo *cluster.Bck, msg *OfflineMsg) (*BucketXact, error) {
	comm, exists := reg.getByUUID(msg.ID)
	if !exists {
		return nil, fmt.Errorf("ETL %q doesn't exist", msg.ID)
	}

	return &BucketXact{
		XactBase: *cmn.NewXactBaseBck(id, cmn.ActETLBucket, bckFrom.Bck),
		t:        t,
		bckTo:    bckTo,
		comm:     comm,
		bckMsg:   msg,
		ctx:      context.Background(),
	}, nil
}

// TODO: reuse query xaction? introduce mpath-joggers like in copyBck?
func (r *BucketXact) Run() error {
	wi := walkinfo.NewWalkInfo(r.ctx, r.t, &cmn.SelectMsg{})

	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := wi.Callback(fqn, de)
		if entry == nil || err != nil {
			return err
		}

		select {
		case <-r.ChanAbort():
			return cmn.NewAbortedError(r.Kind())
		default:
			break
		}

		return r.transformAndPut(entry)
	}

	opts := &fs.WalkBckOptions{
		Options: fs.Options{
			Bck:      r.Bck(),
			CTs:      []string{fs.ObjectType},
			Callback: cb,
			Sorted:   true,
		},
		ValidateCallback: func(fqn string, de fs.DirEntry) error {
			if de.IsDir() {
				return wi.ProcessDir(fqn)
			}
			return nil
		},
	}

	err := fs.WalkBck(opts)
	r.Finish(err)
	return err
}

func (r *BucketXact) transformAndPut(entry *cmn.BucketEntry) error {
	// Get transformed object reader from ETL request
	body, length, err := r.comm.Get(cluster.NewBckEmbed(r.Bck()), entry.Name)
	if err != nil {
		return err
	}

	r.ObjectsInc()

	if r.bckMsg.DryRun {
		if length > 0 {
			// Trust the length from content length header is set correctly.
			r.BytesAdd(length)
			debug.AssertNoErr(body.Close())
			return nil
		}

		n, err := io.Copy(ioutil.Discard, body)
		r.BytesAdd(n)
		return err
	}

	if length > 0 {
		// Trust the length from content length header is set correctly.
		r.BytesAdd(length)
	}

	// Get object name for a transformed object.
	newObjName := newETLObjName(entry.Name, r.bckMsg)

	// If targets membership changes, this xaction will be aborted by ETL Aborter.
	destTarget, err := cluster.HrwTarget(r.bckTo.MakeUname(newObjName), r.t.GetSowner().Get())
	if err != nil {
		return err
	}

	if !r.t.Snode().Equals(destTarget) {
		// Send object to a different target
		header := make(http.Header, 1)
		header.Add(cmn.HeaderContentLength, fmt.Sprintf("%d", length))
		params := cluster.SendToParams{
			Reader:    body,
			BckTo:     r.bckTo,
			ObjNameTo: newObjName,
			Header:    header,
			Tsi:       destTarget,
		}
		return r.t.SendTo(params)
	}

	// We have luck. Save object locally.
	lom := &cluster.LOM{T: r.t, ObjName: newObjName}
	err = lom.Init(r.bckTo.Bck)
	if err != nil {
		return err
	}

	// Put object locally.
	err = r.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       body,
		WorkFQN:      fs.CSM.GenContentFQN(lom.FQN, fs.WorkfileType, newObjName),
		Started:      time.Now(),
		WithFinalize: true,
		RecvType:     cluster.ColdGet,
	})
	if err != nil {
		return err
	}

	debug.AssertNoErr(body.Close())
	return nil
}

func newETLObjName(name string, msg *OfflineMsg) string {
	if msg.Ext != "" {
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			name = name[:idx+1] + msg.Ext
		}
	}
	if msg.Prefix != "" {
		name = msg.Prefix + name
	}
	if msg.Suffix != "" {
		name += msg.Suffix
	}

	return name
}
