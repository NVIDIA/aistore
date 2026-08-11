// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn_test

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

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
