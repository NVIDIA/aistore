// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"encoding/json"
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"

	jsoniter "github.com/json-iterator/go"
)

type (
	// ETLEntity represents an ETL instance managed by an individual target.
	// - Created and added to the manager before entering the `Initializing` stage.
	// - Removed only after the user explicitly deletes it from the `Aborted` stage.
	//
	// Expected state transitions:
	// - `Initializing`: Set up resources in the following order:
	//     1. Create (or reuse) communicator and pod watcher
	//     2. Start (or renew) xaction
	//     3. Create Kubernetes resources (pod/service)
	// - `Running`: All resources are active, handling inline and offline transform requests via the communicator.
	// - `Aborted`: Kubernetes resources (pod/service) are cleaned up.
	ETLEntity struct {
		InitMsg InitMsg `json:"init_msg"`
		Stage   Stage   `json:"stage"`
	}
	ETLs map[string]ETLEntity

	// ETL metadata
	MD struct {
		Ext     any
		ETLs    ETLs
		Version int64
	}

	jsonETL struct {
		Type string              `json:"type,string"`
		Msg  jsoniter.RawMessage `json:"msg"`
	}
	jsonMD struct {
		Ext     any                `json:"ext,omitempty"` // within meta-version extensions
		ETLs    map[string]jsonETL `json:"etls"`
		Version int64              `json:"version"`
	}
)

var etlMDJspOpts = jsp.CCSign(cmn.MetaverEtlMD)

// interface guard
var (
	_ json.Marshaler   = (*MD)(nil)
	_ json.Unmarshaler = (*MD)(nil)

	_ jsp.Opts = (*MD)(nil)
)

////////
// MD //
////////

func (e *MD) Init(l int) { e.ETLs = make(ETLs, l) }
func (e *MD) Add(msg InitMsg, stage Stage) {
	if msg == nil {
		return
	}
	e.ETLs[msg.Name()] = ETLEntity{msg, stage}
}
func (*MD) JspOpts() jsp.Options { return etlMDJspOpts }

func (e *MD) Get(id string) (en ETLEntity, present bool) {
	if e == nil {
		return ETLEntity{}, false
	}
	en, present = e.ETLs[id]
	return
}

func (e *MD) Del(id string) (deleted bool) {
	if _, present := e.ETLs[id]; !present {
		return
	}
	delete(e.ETLs, id)
	return true
}

func (e *MD) String() string {
	if e == nil {
		return "EtlMD <nil>"
	}
	return fmt.Sprintf("EtlMD v%d(%d)", e.Version, len(e.ETLs))
}

func (e *MD) MarshalJSON() ([]byte, error) {
	jsonMD := jsonMD{
		Version: e.Version,
		ETLs:    make(map[string]jsonETL, len(e.ETLs)),
		Ext:     e.Ext,
	}
	for k, v := range e.ETLs {
		jsonMD.ETLs[k] = jsonETL{v.InitMsg.MsgType(), cos.MustMarshal(v)}
	}
	return jsoniter.Marshal(jsonMD)
}

func (e *MD) UnmarshalJSON(data []byte) (err error) {
	jsonMD := &jsonMD{}
	if err = jsoniter.Unmarshal(data, jsonMD); err != nil {
		return err
	}
	e.Version, e.Ext = jsonMD.Version, jsonMD.Ext
	e.ETLs = make(ETLs, len(jsonMD.ETLs))
	for k, v := range jsonMD.ETLs {
		en := ETLEntity{}
		switch v.Type {
		case CodeType: // do nothing
		case SpecType:
			en.InitMsg = &InitSpecMsg{}
		case ETLSpecType:
			en.InitMsg = &ETLSpecMsg{}
		default:
			err = fmt.Errorf("invalid InitMsg type %q", v.Type)
			debug.AssertNoErr(err)
			return err
		}
		err = jsoniter.Unmarshal(v.Msg, &en)
		if err != nil || en.InitMsg == nil {
			// NOTE; version 3.30 introduces new ETL MD format - a breaking change
			// TODO -- FIXME: this is a workaround for incompatible etlMD formats
			nlog.Errorln("failed to unmarshal etlMD (ignoring, proceeding anyway), err:", err, "type:", v.Type, "msg:", v.Msg)
			err = nil
			continue
		}
		if err = en.InitMsg.Validate(); err != nil {
			// TODO -- FIXME: this is a workaround for incompatible etlMD formats
			nlog.Errorln("failed to validate etlMD entry (ignoring, proceeding anyway), err:", err, "type:", v.Type, "msg:", v.Msg)
			err = nil
			continue
		}
		e.ETLs[k] = en
	}
	return err
}
