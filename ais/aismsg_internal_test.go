// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package ais

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

type aismsgTestConf struct {
	actionMsgPresent bool
	restPropsPresent bool
}

func (atc aismsgTestConf) Name() string {
	return fmt.Sprintf("actionMsgPresent:%v/restPropsPresent:%v",
		atc.actionMsgPresent, atc.restPropsPresent)
}

func testAisMsgMarshal(t *testing.T, tc aismsgTestConf) {
	t.Run(tc.Name(), func(t *testing.T) {
		beforeMsg := &actMsgExt{}
		if tc.actionMsgPresent {
			actionMsg := apc.ActMsg{
				Action: "test-action",
				Name:   "test-name",
				Value: &cmn.Bck{
					Name:     "test-bck",
					Provider: "test-provider",
					Ns: cmn.Ns{
						UUID: "test-uuid",
						Name: "test-name",
					},
				},
			}
			beforeMsg.ActMsg = actionMsg
		}
		if tc.restPropsPresent {
			beforeMsg.BMDVersion = 12
			beforeMsg.RMDVersion = 34
			beforeMsg.UUID = "outer-uuid"
		}
		b, err := jsoniter.Marshal(beforeMsg)
		if err != nil {
			t.Errorf("Failed to marshal beforeMsg: %v", err)
		}
		afterAisMsg := &actMsgExt{}

		err = jsoniter.Unmarshal(b, afterAisMsg)
		if err != nil {
			t.Errorf("Unmarshal failed for actMsgExt, err: %v", err)
		}

		if afterAisMsg.Value != nil {
			bck := &cmn.Bck{}
			err = cos.MorphMarshal(afterAisMsg.Value, bck)
			if err != nil {
				t.Errorf("Morph marshal failed for actMsgExt.Value: %v, err: %v", afterAisMsg.Value, err)
			}
			afterAisMsg.Value = bck
		}
		if !reflect.DeepEqual(*beforeMsg, *afterAisMsg) {
			t.Errorf("Marshaled: %v and Unmarshalled: %v beforeMsg differ", beforeMsg, afterAisMsg)
		}
	})
}

func TestAisMsgMarshal(t *testing.T) {
	tests := []aismsgTestConf{
		{actionMsgPresent: true, restPropsPresent: true},
		{actionMsgPresent: true, restPropsPresent: false},
		{actionMsgPresent: false, restPropsPresent: true},
		{actionMsgPresent: false, restPropsPresent: false},
	}
	for _, test := range tests {
		testAisMsgMarshal(t, test)
	}
}
