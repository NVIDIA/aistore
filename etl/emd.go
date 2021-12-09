// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"encoding/json"
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	jsoniter "github.com/json-iterator/go"
)

type (
	InitMsg interface {
		ID() string
		CommType() string
		InitType() string
	}

	jsonETL struct {
		Type string              `json:"type,string"`
		Msg  jsoniter.RawMessage `json:"msg"`
	}

	ETLs map[string]InitMsg

	// ETL metadata
	MD struct {
		Version int64       `json:"version"`
		ETLs    ETLs        `json:"etls"`
		Ext     interface{} `json:"ext,omitempty"` // within meta-version extensions
	}
)

var etlMDJspOpts = jsp.CCSign(cmn.MetaverEtlMD)

// interface guard
var (
	_ json.Marshaler   = (*ETLs)(nil)
	_ json.Unmarshaler = (*ETLs)(nil)

	_ jsp.Opts = (*MD)(nil)
)

func (e *MD) Add(spec InitMsg) {
	e.ETLs[spec.ID()] = spec
}

func (e *MD) Get(id string) (msg InitMsg, present bool) {
	if e == nil {
		return
	}
	msg, present = e.ETLs[id]
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

func (e ETLs) MarshalJSON() ([]byte, error) {
	jETL := make(map[string]jsonETL, len(e))
	for k, v := range e {
		jETL[k] = jsonETL{v.InitType(), cos.MustMarshal(v)}
	}
	return jsoniter.Marshal(jETL)
}

func (e *ETLs) UnmarshalJSON(data []byte) (err error) {
	jETL := make(map[string]jsonETL, 16)
	if err = jsoniter.Unmarshal(data, &jETL); err != nil {
		return
	}
	debug.Assert(e != nil)
	for k, v := range jETL {
		if v.Type == cmn.ETLInitCode {
			(*e)[k] = &InitCodeMsg{}
		} else if v.Type == cmn.ETLInitSpec {
			(*e)[k] = &InitSpecMsg{}
		}

		if err = jsoniter.Unmarshal(v.Msg, (*e)[k]); err != nil {
			return
		}
	}
	return
}

func (*MD) JspOpts() jsp.Options { return etlMDJspOpts }
