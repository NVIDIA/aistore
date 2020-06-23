// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestParseSourceValidURIs(t *testing.T) {
	var parseSourceTests = []struct {
		input    string
		expected dlSource
	}{
		{
			input:    "http://www.googleapis.com/storage/v1/b/bucket?query=key#key",
			expected: dlSource{link: "http://www.googleapis.com/storage/v1/b/bucket?query=key#key"},
		},
		{
			input:    "http://www.googleapis.com/storage/v1/b/bucket?query=key",
			expected: dlSource{link: "http://www.googleapis.com/storage/v1/b/bucket?query=key"},
		},
		{
			input:    "http://www.googleapis.com/storage/v1/b/bucket",
			expected: dlSource{link: "http://www.googleapis.com/storage/v1/b/bucket"},
		},

		{
			input: "gs://bucket/very/long/prefix-",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/very/long/prefix-",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle},
					prefix: "very/long/prefix-",
				},
			},
		},
		{
			input: "gcp://bucket",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle},
					prefix: "",
				},
			},
		},
		{
			input: "gs://bucket/objname.tar",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/objname.tar",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle},
					prefix: "objname.tar",
				},
			},
		},
		{
			input: "gs://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/subfolder/objname.tar",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle},
					prefix: "subfolder/objname.tar",
				},
			},
		},

		{
			input:    "https://s3.amazonaws.com/bucket",
			expected: dlSource{link: "https://s3.amazonaws.com/bucket"},
		},
		{
			input:    "http://s3.amazonaws.com/bucket/objname.tar",
			expected: dlSource{link: "http://s3.amazonaws.com/bucket/objname.tar"},
		},
		{
			input:    "http://s3.amazonaws.com/bucket/subfolder/objname.tar",
			expected: dlSource{link: "http://s3.amazonaws.com/bucket/subfolder/objname.tar"},
		},

		{
			input:    "s3.amazonaws.com/bucket",
			expected: dlSource{link: "https://s3.amazonaws.com/bucket"},
		},
		{
			input: "s3://bucket/very/long/prefix-",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/very/long/prefix-",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon},
					prefix: "very/long/prefix-",
				},
			},
		},
		{
			input: "aws://bucket",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon},
					prefix: "",
				},
			},
		},
		{
			input: "s3://bucket/objname.tar",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/objname.tar",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon},
					prefix: "objname.tar",
				},
			},
		},
		{
			input: "s3://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/subfolder/objname.tar",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon},
					prefix: "subfolder/objname.tar",
				},
			},
		},

		{
			input: "az://bucket",
			expected: dlSource{
				link: "",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderAzure},
					prefix: "",
				},
			},
		},
		{
			input: "azure://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderAzure},
					prefix: "subfolder/objname.tar",
				},
			},
		},

		{
			input:    "src.com/image001.tar.gz",
			expected: dlSource{link: "https://src.com/image001.tar.gz"},
		},

		{
			input:    "https://www.googleapis.com/storage/v1/b/nvdata-openimages/o/imagenet%2Fimagenet_train-{000000..000002}.tgz?alt=media",
			expected: dlSource{link: "https://www.googleapis.com/storage/v1/b/nvdata-openimages/o/imagenet/imagenet_train-{000000..000002}.tgz?alt=media"},
		},
		{
			input: "gs://bucket/obj{00..10}.tgz",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/obj{00..10}.tgz",
				cloud: dlSourceCloud{
					bck:    cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle},
					prefix: "obj{00..10}.tgz",
				},
			},
		},
		{
			input:    "ais://172.10.10.10/bucket",
			expected: dlSource{link: "http://172.10.10.10:8080/v1/objects/bucket"},
		},
		{
			input:    "ais://172.10.10.10:4444/bucket",
			expected: dlSource{link: "http://172.10.10.10:4444/v1/objects/bucket"},
		},
	}

	for _, test := range parseSourceTests {
		source, err := parseSource(test.input)
		if err != nil {
			t.Errorf("unexpected error while parsing source URI %s: %v", test.input, err)
		}

		if source != test.expected {
			t.Errorf("parseSource(%s) expected: %v, got: %v", test.input, test.expected, source)
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
		query       bool
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
			uri:         "ais://@",
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS},
		},
		{
			uri:         "ais://@",
			query:       true,
			expectedBck: cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.NsAnyRemote},
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
		bck, obj, err := parseBckObjectURI(test.uri, test.query)
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
