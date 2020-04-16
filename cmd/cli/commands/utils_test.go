// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestParseSourceValidURIs(t *testing.T) {
	var parseSourceTests = []struct {
		url      string
		expected string
	}{
		{"http://www.googleapis.com/storage/v1/b/bucket?query=key#key", "http://www.googleapis.com/storage/v1/b/bucket?query=key#key"},
		{"http://www.googleapis.com/storage/v1/b/bucket?query=key", "http://www.googleapis.com/storage/v1/b/bucket?query=key"},
		{"http://www.googleapis.com/storage/v1/b/bucket", "http://www.googleapis.com/storage/v1/b/bucket"},

		{"gs://bucket", "https://storage.googleapis.com/bucket"},
		{"gs://bucket/objname.tar", "https://storage.googleapis.com/bucket/objname.tar"},
		{"gs://bucket/subfolder/objname.tar", "https://storage.googleapis.com/bucket/subfolder/objname.tar"},

		{"https://s3.amazonaws.com/bucket", "https://s3.amazonaws.com/bucket"},
		{"http://s3.amazonaws.com/bucket/objname.tar", "http://s3.amazonaws.com/bucket/objname.tar"},
		{"http://s3.amazonaws.com/bucket/subfolder/objname.tar", "http://s3.amazonaws.com/bucket/subfolder/objname.tar"},

		{"s3.amazonaws.com/bucket", "https://s3.amazonaws.com/bucket"},
		{"s3://bucket", "http://s3.amazonaws.com/bucket"},
		{"s3://bucket/objname.tar", "http://s3.amazonaws.com/bucket/objname.tar"},
		{"s3://bucket/subfolder/objname.tar", "http://s3.amazonaws.com/bucket/subfolder/objname.tar"},

		{"src.com/image001.tar.gz", "https://src.com/image001.tar.gz"},

		{"https://www.googleapis.com/storage/v1/b/lpr-vision/o/imagenet%2Fimagenet_train-{000000..000002}.tgz?alt=media",
			"https://www.googleapis.com/storage/v1/b/lpr-vision/o/imagenet/imagenet_train-{000000..000002}.tgz?alt=media"},
		{"gs://bucket/obj{00..10}.tgz", "https://storage.googleapis.com/bucket/obj{00..10}.tgz"},
		{"ais://172.10.10.10/bucket", "http://172.10.10.10:8080/v1/objects/bucket"},
		{"ais://172.10.10.10:4444/bucket", "http://172.10.10.10:4444/v1/objects/bucket"},
	}

	for _, test := range parseSourceTests {
		actual, err := parseSource(test.url)

		if err != nil {
			t.Errorf("unexpected error while parsing source URI %s: %v", test.url, err)
		}

		if actual != test.expected {
			t.Errorf("parseSource(%s) expected: %s, got: %s", test.url, test.expected, actual)
		}
	}
}

func TestParseDestValidURIs(t *testing.T) {
	var parseDestTests = []struct {
		url     string
		bucket  string
		objName string
	}{
		{"ais://bucket/objname", "bucket", "objname"},
		{"ais://bucket//subfolder/objname.tar", "bucket", "subfolder/objname.tar"},
		{"ais://bucket/subfolder/objname.tar", "bucket", "subfolder/objname.tar"},
		{"ais://bucket", "bucket", ""},
	}

	for _, test := range parseDestTests {
		bucket, pathSuffix, err := parseDest(test.url)

		if err != nil {
			t.Errorf("unexpected error while parsing dest URI %s: %v", test.url, err)
		}

		if bucket != test.bucket {
			t.Errorf("parseSource(%s) expected bucket: %s, got: %s", test.url, test.bucket, bucket)
		}
		if pathSuffix != test.objName {
			t.Errorf("parseSource(%s) expected bucket: %s, got: %s", test.url, test.objName, pathSuffix)
		}
	}
}

func TestParseDestInvalidURIs(t *testing.T) {
	var parseDestTests = []string{
		"gcp://bucket",
		"gcp://bucket/objname",
		"s3://bucket/objname",
		"aws://bucket/objname",
		"http://bucket/objname",
		"ais://",
	}

	for _, test := range parseDestTests {
		_, _, err := parseDest(test)
		if err == nil {
			t.Errorf("expected error while parsing dest URI %s: %v", test, err)
		}
	}
}

func TestMakePairs(t *testing.T) {
	var makePairsTest = []struct {
		input []string
		nvs   cmn.SimpleKVs
	}{
		{[]string{"key1=value1", "key2=value2", "key3=value3"},
			map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}},
		{[]string{"key1", "value1", "key2", "value2", "key3", "value3"},
			map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}},
		{[]string{"key1=value1", "key2", "value2", "key3=value3"},
			map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}},
	}

	for _, test := range makePairsTest {
		nvs, err := makePairs(test.input)

		if err != nil {
			t.Fatalf("unexpected error of make pairs for input %#v: %v", test.input, err)
		}

		if !reflect.DeepEqual(nvs, test.nvs) {
			t.Errorf("makePairs expected output: %#v, got: %#v", test.nvs, nvs)
		}
	}
}

func TestMakePairsErrors(t *testing.T) {
	var makePairsTest = []struct {
		input []string
	}{
		{[]string{"key1", "value1", "key2=value2", "key3"}},
	}

	for _, test := range makePairsTest {
		_, err := makePairs(test.input)

		if err == nil {
			t.Fatalf("expected error of make pairs for input %#v, but got none", test.input)
		}
	}
}

func TestParseBckObjectURI(t *testing.T) {
	var tests = []struct {
		uri         string
		expectedErr bool
		expectedBck cmn.Bck
		expectedObj string
	}{
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
			expectedBck: cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
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
			uri:         "ais://@#/bucket",
			expectedBck: cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS},
		},
		{
			uri:         "ais://@uuid#namespace/bucket",
			expectedBck: cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
		},
		{
			uri:         "ais://bucket",
			expectedBck: cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS},
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
	}

	for _, test := range tests {
		bck, obj, err := parseBckObjectURI(test.uri)
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
