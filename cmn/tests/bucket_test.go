// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

func TestParseBckObjectURI(t *testing.T) {
	positiveTests := []struct {
		uri  string
		opts cmn.ParseURIOpts

		expectedBck cmn.Bck
		expectedObj string
	}{
		{
			uri: "",
		},
		{
			uri:         "ais://",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS},
		},
		{
			uri:         "ais://@uuid",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}},
		},
		{
			uri:         "ais://@uuid#namespace/bucket/object",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
			expectedObj: "object",
		},
		{
			uri:         "ais://@uuid#",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}},
		},
		{
			uri:         "ais://@uuid#/",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}},
		},
		{
			uri:         "ais://@#/",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS},
		},
		{
			uri:         "ais://@#",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS},
		},
		{
			uri:         "ais://@",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS},
		},
		{
			uri:         "ais://@",
			opts:        cmn.ParseURIOpts{IsQuery: true},
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Ns: cmn.NsAnyRemote},
		},
		{
			uri:         "@uuid#namespace",
			opts:        cmn.ParseURIOpts{IsQuery: true},
			expectedBck: cmn.Bck{Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
		},
		{
			uri:         "@",
			opts:        cmn.ParseURIOpts{IsQuery: true},
			expectedBck: cmn.Bck{Ns: cmn.NsAnyRemote},
		},
		{
			uri:         "@#",
			opts:        cmn.ParseURIOpts{IsQuery: true},
			expectedBck: cmn.Bck{},
		},
		{
			uri:         "ais://@#/bucket",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "ais://bucket",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "bucket",
			opts:        cmn.ParseURIOpts{DefaultProvider: apc.ProviderAIS},
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "bucket",
			opts:        cmn.ParseURIOpts{DefaultProvider: apc.ProviderAmazon},
			expectedBck: cmn.Bck{Provider: apc.ProviderAmazon, Name: "bucket"},
		},
		{
			uri:         "ais://bucket/objname",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
			expectedObj: "objname",
		},
		{
			uri:         "ais://@uuid#namespace/bucket",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
		},
		{
			uri:         "@uuid#namespace/bucket",
			opts:        cmn.ParseURIOpts{DefaultProvider: apc.ProviderAmazon},
			expectedBck: cmn.Bck{Provider: apc.ProviderAmazon, Name: "bucket", Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
		},
		{
			uri:         "ais://bucket",
			expectedBck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
		},
		{
			uri:         "aws://",
			expectedBck: cmn.Bck{Provider: apc.ProviderAmazon},
		},
		{
			uri:         "az:///",
			expectedBck: cmn.Bck{Provider: apc.ProviderAzure},
		},
	}

	for _, test := range positiveTests {
		bck, obj, err := cmn.ParseBckObjectURI(test.uri, test.opts)
		tassert.Errorf(t, err == nil, "unexpected error for input: %s, err: %v", test.uri, err)

		if !bck.Equal(test.expectedBck) {
			t.Errorf("buckets does not match got: %v, expected: %v (input: %s)", bck, test.expectedBck, test.uri)
		}
		if obj != test.expectedObj {
			t.Errorf("object names does not match got: %s, expected: %s (input: %s)", obj, test.expectedObj, test.uri)
		}
	}

	negativeTests := []struct {
		uri  string
		opts cmn.ParseURIOpts
	}{
		{uri: "ais://%something"},
		{uri: "aiss://"},
		{uri: "ais:/"},
		{uri: "bucket"},
		{uri: "bucket/object"},
		{uri: "@#"},
		{uri: "@uuid#namespace"},
		{uri: "@uuid#namespace/bucket"},
		{uri: "@uuid#namespace/bucket/object"},
	}

	for _, test := range negativeTests {
		_, _, err := cmn.ParseBckObjectURI(test.uri, test.opts)
		tassert.Errorf(t, err != nil, "expected error for input: %s", test.uri)
	}
}
