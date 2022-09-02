// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/urfave/cli"
)

func TestParseSourceValidURIs(t *testing.T) {
	parseSourceTests := []struct {
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
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderGoogle},
					prefix: "very/long/prefix-",
				},
			},
		},
		{
			input: "gcp://bucket",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderGoogle},
					prefix: "",
				},
			},
		},
		{
			input: "gs://bucket/objname.tar",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderGoogle},
					prefix: "objname.tar",
				},
			},
		},
		{
			input: "gs://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/subfolder/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderGoogle},
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
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderAmazon},
					prefix: "very/long/prefix-",
				},
			},
		},
		{
			input: "aws://bucket",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderAmazon},
					prefix: "",
				},
			},
		},
		{
			input: "s3://bucket/objname.tar",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderAmazon},
					prefix: "objname.tar",
				},
			},
		},
		{
			input: "s3://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/subfolder/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderAmazon},
					prefix: "subfolder/objname.tar",
				},
			},
		},

		{
			input: "az://bucket",
			expected: dlSource{
				link: "",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderAzure},
					prefix: "",
				},
			},
		},
		{
			input: "azure://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderAzure},
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
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.ProviderGoogle},
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
	parseDestTests := []struct {
		url     string
		bucket  cmn.Bck
		objName string
	}{
		{"ais://bucket/objname", cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"}, "objname"},
		{"ais://bucket//subfolder/objname.tar", cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"}, "subfolder/objname.tar"},
		{"ais://bucket/subfolder/objname.tar", cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"}, "subfolder/objname.tar"},
		{"ais://bucket", cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"}, ""},
		{"aws://bucket/something/", cmn.Bck{Provider: apc.ProviderAmazon, Name: "bucket"}, "something"},
		{"az://bucket/something//", cmn.Bck{Provider: apc.ProviderAzure, Name: "bucket"}, "something"},
		{"gcp://bucket/one/two/", cmn.Bck{Provider: apc.ProviderGoogle, Name: "bucket"}, "one/two"},
	}

	for _, test := range parseDestTests {
		bucket, pathSuffix, err := parseDest(&cli.Context{}, test.url)
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
	parseDestTests := []string{
		"ais://",
		"ais:///",
		"http://",
		"http://bucket",
		"http://bucket/something",
	}

	for _, test := range parseDestTests {
		_, _, err := parseDest(&cli.Context{}, test)
		if err == nil {
			t.Errorf("expected error while parsing dest URI %s", test)
		}
	}
}

func TestMakePairs(t *testing.T) {
	makePairsTest := []struct {
		input []string
		nvs   cos.SimpleKVs
	}{
		{
			[]string{"key1=value1", "key2=value2", "key3=value3"},
			map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			[]string{"key1", "value1", "key2", "value2", "key3", "value3"},
			map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			[]string{"key1=value1", "key2", "value2", "key3=value3"},
			map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			[]string{"key1=value11=value12", "key2", "value21=value22", "key3=value31=value32,value33=value34"},
			map[string]string{"key1": "value11=value12", "key2": "value21=value22", "key3": "value31=value32,value33=value34"},
		},
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
	makePairsTest := []struct {
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

func TestParseQueryBckURI(t *testing.T) {
	positiveTests := []struct {
		uri string
		bck cmn.QueryBcks
	}{
		{uri: "", bck: cmn.QueryBcks{}},
		{uri: "ais://", bck: cmn.QueryBcks{Provider: apc.ProviderAIS}},
		{uri: "ais://#ns", bck: cmn.QueryBcks{Provider: apc.ProviderAIS, Ns: cmn.Ns{Name: "ns"}}},
		{uri: "ais://@uuid", bck: cmn.QueryBcks{Provider: apc.ProviderAIS, Ns: cmn.Ns{UUID: "uuid"}}},
		{uri: "ais://@uuid#ns", bck: cmn.QueryBcks{Provider: apc.ProviderAIS, Ns: cmn.Ns{Name: "ns", UUID: "uuid"}}},
		{uri: "ais://bucket", bck: cmn.QueryBcks{Provider: apc.ProviderAIS, Name: "bucket"}},
		{uri: "hdfs://bucket", bck: cmn.QueryBcks{Provider: apc.ProviderHDFS, Name: "bucket"}},
		{uri: "ais://#ns/bucket", bck: cmn.QueryBcks{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}}},
		{uri: "ais://@uuid#ns/bucket", bck: cmn.QueryBcks{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns", UUID: "uuid"}}},
		{uri: "http://web.url/dataset", bck: cmn.QueryBcks{Provider: apc.ProviderHTTP, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}},
		{uri: "https://web.url/dataset", bck: cmn.QueryBcks{Provider: apc.ProviderHTTP, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}},
	}
	for _, test := range positiveTests {
		bck, err := parseQueryBckURI(&cli.Context{}, test.uri)
		tassert.Errorf(t, err == nil, "failed on %s with err: %v", test.uri, err)
		b := cmn.Bck(bck)
		tassert.Errorf(t, test.bck.Equal(&b), "failed on %s buckets are not equal (expected: %q, got: %q)", test.uri, test.bck, bck)
	}

	negativeTests := []struct {
		uri string
	}{
		{uri: "bucket"},
		{uri: ":/"},
		{uri: "://"},
		{uri: "aiss://"},
		{uri: "aiss://bucket"},
		{uri: "ais://bucket/objname"},
		{uri: "ais:///objectname"},
		{uri: "ftp://unsupported"},
	}
	for _, test := range negativeTests {
		bck, err := parseQueryBckURI(&cli.Context{}, test.uri)
		tassert.Errorf(t, err != nil, "expected error on %s (bck: %q)", test.uri, bck)
	}
}

func TestParseBckURI(t *testing.T) {
	positiveTests := []struct {
		uri string
		bck cmn.Bck
	}{
		{uri: "ais://bucket", bck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"}},
		{uri: "hdfs://bucket", bck: cmn.Bck{Provider: apc.ProviderHDFS, Name: "bucket"}},
		{uri: "ais://#ns/bucket", bck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}}},
		{uri: "ais://@uuid#ns/bucket", bck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns", UUID: "uuid"}}},
		{uri: "http://web.url/dataset", bck: cmn.Bck{Provider: apc.ProviderHTTP, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}},
		{uri: "https://web.url/dataset", bck: cmn.Bck{Provider: apc.ProviderHTTP, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}},
	}
	for _, test := range positiveTests {
		bck, err := parseBckURI(&cli.Context{}, test.uri)
		tassert.Errorf(t, err == nil, "failed on %s with err: %v", test.uri, err)
		tassert.Errorf(t, test.bck.Equal(&bck), "failed on %s buckets are not equal (expected: %q, got: %q)", test.uri, test.bck, bck)
	}

	negativeTests := []struct {
		uri string
	}{
		{uri: "bucket"},
		{uri: ""},
		{uri: "://"},
		{uri: "://bucket"},
		{uri: "ais://"},
		{uri: "ais:///"},
		{uri: "aiss://bucket"},
		{uri: "ais://bucket/objname"},
		{uri: "ais://#ns"},
		{uri: "ais://@uuid#ns"},
		{uri: "ais://@uuid"},
		{uri: "ais:///objectname"},
		{uri: "ftp://unsupported"},
	}
	for _, test := range negativeTests {
		bck, err := parseBckURI(&cli.Context{}, test.uri)
		tassert.Errorf(t, err != nil, "expected error on %s (bck: %q)", test.uri, bck)
	}
}

func TestParseBckObjectURI(t *testing.T) {
	positiveTests := []struct {
		uri        string
		optObjName bool

		bck     cmn.Bck
		objName string
	}{
		{
			uri: "ais://bucket", optObjName: true,
			bck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
		},
		{
			uri: "ais://bucket/object_name", optObjName: true,
			bck:     cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
			objName: "object_name",
		},
		{
			uri:     "ais://bucket/object_name",
			bck:     cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket"},
			objName: "object_name",
		},
		{
			uri:     "hdfs://bucket/object_name/something",
			bck:     cmn.Bck{Provider: apc.ProviderHDFS, Name: "bucket"},
			objName: "object_name/something",
		},
		{
			uri:     "ais://#ns/bucket/a/b/c",
			bck:     cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}},
			objName: "a/b/c",
		},
		{
			uri: "ais://#ns/bucket", optObjName: true,
			bck: cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}},
		},
		{
			uri:     "ais://@uuid#ns/bucket/object",
			bck:     cmn.Bck{Provider: apc.ProviderAIS, Name: "bucket", Ns: cmn.Ns{Name: "ns", UUID: "uuid"}},
			objName: "object",
		},
		{
			uri:     "http://web.url/dataset/object_name",
			bck:     cmn.Bck{Provider: apc.ProviderHTTP, Name: "ZWUyYWFiOGEzYjEwMTJkNw"},
			objName: "object_name",
		},
		{
			uri:     "https://web.url/dataset/object_name",
			bck:     cmn.Bck{Provider: apc.ProviderHTTP, Name: "ZWUyYWFiOGEzYjEwMTJkNw"},
			objName: "object_name",
		},
	}
	for _, test := range positiveTests {
		bck, objName, err := parseBckObjectURI(&cli.Context{}, test.uri, test.optObjName)
		tassert.Errorf(t, err == nil, "failed on %s with err: %v", test.uri, err)
		tassert.Errorf(t, test.bck.Equal(&bck), "failed on %s buckets are not equal (expected: %q, got: %q)", test.uri, test.bck, bck)
		tassert.Errorf(t, test.objName == objName, "failed on %s object names are not equal (expected: %q, got: %q)", test.uri, test.objName, objName)
	}

	negativeTests := []struct {
		uri        string
		optObjName bool
	}{
		{uri: ""},
		{uri: "bucket"},
		{uri: "bucket", optObjName: true},
		{uri: ":/"},
		{uri: "://"},
		{uri: "://", optObjName: true},
		{uri: "://bucket"},
		{uri: "://bucket", optObjName: true},
		{uri: "ais://"},
		{uri: "ais:///"},
		{uri: "aiss://bucket"},
		{uri: "ais://bucket"},
		{uri: "ais:///objectname"},
		{uri: "ais:///objectname", optObjName: true},
		{uri: "ais://#ns"},
		{uri: "ais://#ns/bucket"},
		{uri: "ais://@uuid"},
		{uri: "ais://@uuid#ns"},
		{uri: "ais://@uuid#ns/bucket"},
		{uri: "ftp://unsupported"},
		{uri: "hdfs://bucket"},
	}
	for _, test := range negativeTests {
		bck, objName, err := parseBckObjectURI(&cli.Context{}, test.uri, test.optObjName)
		tassert.Errorf(t, err != nil, "expected error on %s (bck: %q, obj_name: %q)", test.uri, bck, objName)
	}
}
