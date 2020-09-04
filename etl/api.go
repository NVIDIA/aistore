// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/etl/template"
)

type (
	InitMsg struct {
		ID          string           `json:"id"`
		Spec        []byte           `json:"spec"`
		CommType    string           `json:"communication_type"`
		WaitTimeout cmn.DurationJSON `json:"wait_timeout"`
	}

	BuildMsg struct {
		ID      string `json:"id"`
		Code    []byte `json:"code"`
		Deps    []byte `json:"dependencies"`
		Runtime string `json:"runtime"`
	}

	Info struct {
		ID           string `json:"id"`
		Name         string `json:"name"`
		RemoteAddrIP string `json:"remote_addr_ip"`
	}

	OfflineMsg struct {
		ID     string `json:"id"`      // ETL ID
		Prefix string `json:"prefix"`  // Prefix added to each resulting object.
		Suffix string `json:"suffix"`  // Suffix added to each resulting object.
		DryRun bool   `json:"dry_run"` // Don't perform any PUT

		// New objects names will have this extension. Warning: if in a source
		// bucket exist two objects with the same base name, but different
		// extension, specifying this field might cause object overriding.
		// This is because of resulting name conflict.
		Ext string `json:"ext"`
	}

	OfflineBckMsg struct {
		cmn.Bck
		OfflineMsg
	}
)

func ParseOfflineBckMsg(v interface{}) (*OfflineBckMsg, error) {
	bckMsg := OfflineBckMsg{}
	if err := cmn.MorphMarshal(v, &bckMsg); err != nil {
		return nil, fmt.Errorf("error unmarshaling OfflineBckMsg: %s", err.Error())
	}

	cleanUpBckMsg(&bckMsg.OfflineMsg)
	return &bckMsg, nil
}

func ParseOfflineMsg(v interface{}) (*OfflineMsg, error) {
	msg := OfflineMsg{}
	if err := cmn.MorphMarshal(v, &msg); err != nil {
		return nil, fmt.Errorf("error unmarshaling OfflineBckMsg: %s", err.Error())
	}

	cleanUpBckMsg(&msg)
	return &msg, nil
}

func cleanUpBckMsg(msg *OfflineMsg) {
	msg.Ext = strings.TrimLeft(msg.Ext, ".")
}

func (m BuildMsg) Validate() error {
	cmn.Assert(m.ID != "")

	if len(m.Code) == 0 {
		return fmt.Errorf("code is empty")
	}
	if m.Runtime == "" {
		return fmt.Errorf("runtime is not specified")
	}
	if _, ok := template.Runtimes[m.Runtime]; !ok {
		return fmt.Errorf("unsupported runtime provided: %s", m.Runtime)
	}
	return nil
}
