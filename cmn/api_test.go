// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestBpropsPlacementUpdate(t *testing.T) {
	for _, test := range []struct {
		name        string
		initial     bool
		update      *string
		wantUniform bool
		wantErr     bool
	}{
		{name: "set-uniform", update: apc.Ptr("uniform"), wantUniform: true},
		{name: "reset-empty", initial: true, update: apc.Ptr("")},
		{name: "keep-uniform", initial: true, wantUniform: true},
		{name: "default"},
		{name: "invalid", initial: true, update: apc.Ptr("node"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := &cmn.Bprops{Provider: apc.AIS, Cksum: cmn.CksumConf{Type: cos.ChecksumNone}}
			if test.initial {
				original.Placement = &cmn.PlacementConf{Weighting: cmn.WeightingUniform}
			}
			if err := original.Validate(3); err != nil {
				t.Fatal(err)
			}
			update := &cmn.BpropsToSet{}
			if test.update != nil {
				var err error
				update, err = cmn.NewBpropsToSet(cos.StrKVs{cmn.PropPlacementWeighting: *test.update})
				if err != nil {
					t.Fatal(err)
				}
			}
			props := original.Clone()
			props.Apply(update)
			err := props.Validate(3)
			if test.initial && original.Placement.Weighting != cmn.WeightingUniform {
				t.Fatal("update mutated original bucket properties")
			}
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), cmn.PropPlacementWeighting) {
					t.Fatalf("expected placement validation error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantUniform {
				if props.Placement == nil || props.Placement.Weighting != cmn.WeightingUniform {
					t.Fatalf("expected uniform placement, got %+v", props.Placement)
				}
			} else if props.Placement != nil {
				t.Fatalf("default placement not normalized: %+v", props.Placement)
			}
			data, err := cos.JSON.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			if present := bytes.Contains(data, []byte(`"placement":`)); present != test.wantUniform {
				t.Fatalf("placement wire presence = %t, want %t", present, test.wantUniform)
			}
		})
	}
}

func TestBpropsHTTPExtraCompatibility(t *testing.T) {
	var bprops cmn.Bprops
	data := []byte(`{"extra":{"http":{"original_url":"https://example.com/"},"aws":{"profile":"p1"},"gcp":{"application_creds":"gcp.json"},"oci":{"region":"us-phoenix-1"},"custom":"k=v"}}`)
	if err := cos.JSON.Unmarshal(data, &bprops); err != nil {
		t.Fatal(err)
	}
	want := cmn.ExtraProps{
		AWS:    cmn.ExtraPropsAWS{Profile: "p1"},
		GCP:    cmn.ExtraPropsGCP{ApplicationCreds: "gcp.json"},
		OCI:    cmn.ExtraPropsOCI{Region: "us-phoenix-1"},
		Custom: "k=v",
	}
	if !reflect.DeepEqual(bprops.Extra, want) {
		t.Fatalf("got %+v, want %+v", bprops.Extra, want)
	}
}
