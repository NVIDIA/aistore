// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestParseBckObjectURI(t *testing.T) {
	tests := []struct {
		uri         string
		query       bool
		expectedErr bool
		expectedBck cmn.Bck
		expectedObj string
	}{
		{
			uri: "",
		},
		{
			uri:         "ais://",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS},
		},
		{
			uri:         "ais://@uuid",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}},
		},
		{
			uri:         "ais://@uuid#namespace/bucket/object",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Name: "bucket", Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
			expectedObj: "object",
		},
		{
			uri:         "ais://@uuid#",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}},
		},
		{
			uri:         "ais://@uuid#/",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}},
		},
		{
			uri:         "ais://@#/",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS},
		},
		{
			uri:         "ais://@#",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS},
		},

		{
			uri:         "ais://@",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS},
		},
		{
			uri:         "ais://@",
			query:       true,
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.NsAnyRemote},
		},
		{
			uri:         "@uuid#namespace",
			query:       true,
			expectedBck: cmn.Bck{Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
		},
		{
			uri:         "@",
			query:       true,
			expectedBck: cmn.Bck{Ns: cmn.NsAnyRemote},
		},
		{
			uri:         "@#",
			query:       true,
			expectedBck: cmn.Bck{},
		},
		{
			uri:         "ais://@#/bucket",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "ais://bucket",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "ais://bucket/objname",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Name: "bucket"},
			expectedObj: "objname",
		},
		{
			uri:         "ais://@uuid#namespace/bucket",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Name: "bucket", Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
		},
		{
			uri:         "ais://bucket",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "aws://",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAmazon},
		},
		{
			uri:         "az:///",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAzure},
		},
		// errors
		{uri: "ais://%something", expectedErr: true},
		{uri: "aiss://", expectedErr: true},
		{uri: "ais:/", expectedErr: true},
		{uri: "bucket", expectedErr: true},
		{uri: "bucket/object", expectedErr: true},
		{uri: "@#", expectedErr: true},
		{uri: "@uuid#namespace", expectedErr: true},
		{uri: "@uuid#namespace/bucket", expectedErr: true},
		{uri: "@uuid#namespace/bucket/object", expectedErr: true},
	}

	for _, test := range tests {
		bck, obj, err := cmn.ParseBckObjectURI(test.uri, test.query)
		if err == nil && test.expectedErr {
			t.Errorf("expected error for input: %s", test.uri)
			continue
		} else if err != nil && !test.expectedErr {
			t.Errorf("unpexpected error for input: %s, err: %v", test.uri, err)
			continue
		} else if err != nil && test.expectedErr {
			continue
		}

		if !bck.Equal(test.expectedBck) {
			t.Errorf("buckets does not match got: %v, expected: %v (input: %s)", bck, test.expectedBck, test.uri)
		}
		if obj != test.expectedObj {
			t.Errorf("object names does not match got: %s, expected: %s (input: %s)", obj, test.expectedObj, test.uri)
		}
	}
}
