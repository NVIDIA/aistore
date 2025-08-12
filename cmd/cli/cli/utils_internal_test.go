// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"

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
					bck:    cmn.Bck{Name: "bucket", Provider: apc.GCP},
					prefix: "very/long/prefix-",
				},
			},
		},
		{
			input: "gcp://bucket",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.GCP},
					prefix: "",
				},
			},
		},
		{
			input: "gs://bucket/objname.tar",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.GCP},
					prefix: "objname.tar",
				},
			},
		},
		{
			input: "gs://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "https://storage.googleapis.com/bucket/subfolder/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.GCP},
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
					bck:    cmn.Bck{Name: "bucket", Provider: apc.AWS},
					prefix: "very/long/prefix-",
				},
			},
		},
		{
			input: "aws://bucket",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.AWS},
					prefix: "",
				},
			},
		},
		{
			input: "s3://bucket/objname.tar",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.AWS},
					prefix: "objname.tar",
				},
			},
		},
		{
			input: "s3://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "http://s3.amazonaws.com/bucket/subfolder/objname.tar",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.AWS},
					prefix: "subfolder/objname.tar",
				},
			},
		},

		{
			input: "az://bucket",
			expected: dlSource{
				link: "",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.Azure},
					prefix: "",
				},
			},
		},
		{
			input: "azure://bucket/subfolder/objname.tar",
			expected: dlSource{
				link: "",
				backend: dlSourceBackend{
					bck:    cmn.Bck{Name: "bucket", Provider: apc.Azure},
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
					bck:    cmn.Bck{Name: "bucket", Provider: apc.GCP},
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
		source, err := parseDlSource(nil, test.input)
		if err != nil {
			t.Errorf("unexpected error while parsing source URI %s: %v", test.input, err)
		}

		if !reflect.DeepEqual(source, test.expected) {
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
		{"ais://bucket/objname", cmn.Bck{Provider: apc.AIS, Name: "bucket"}, "objname"},
		{"ais://bucket//subfolder/objname.tar", cmn.Bck{Provider: apc.AIS, Name: "bucket"}, "subfolder/objname.tar"},
		{"ais://bucket/subfolder/objname.tar", cmn.Bck{Provider: apc.AIS, Name: "bucket"}, "subfolder/objname.tar"},
		{"ais://bucket", cmn.Bck{Provider: apc.AIS, Name: "bucket"}, ""},
		{"aws://bucket/something/", cmn.Bck{Provider: apc.AWS, Name: "bucket"}, "something"},
		{"az://bucket/something//", cmn.Bck{Provider: apc.Azure, Name: "bucket"}, "something"},
		{"gcp://bucket/one/two/", cmn.Bck{Provider: apc.GCP, Name: "bucket"}, "one/two"},
	}

	for _, test := range parseDestTests {
		bucket, pathSuffix, err := parseBckObjAux(&cli.Context{}, test.url)
		if err != nil {
			t.Errorf("unexpected error while parsing dest URI %s: %v", test.url, err)
		}

		if bucket != test.bucket {
			t.Errorf("parseSource(%s) expected bucket: %s, got: %s", test.url, test.bucket.String(), bucket.String())
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
		_, _, err := parseBckObjAux(&cli.Context{}, test)
		if err == nil {
			t.Errorf("expected error while parsing dest URI %s", test)
		}
	}
}

func TestMakePairs(t *testing.T) {
	makePairsTest := []struct {
		input []string
		nvs   cos.StrKVs
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
		uri  string
		bck  cmn.QueryBcks
		pref string
	}{
		{uri: "", bck: cmn.QueryBcks{}, pref: ""},
		{uri: "ais://", bck: cmn.QueryBcks{Provider: apc.AIS}, pref: ""},
		{uri: "ais://#ns", bck: cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.Ns{Name: "ns"}}, pref: ""},
		{uri: "ais://@uuid", bck: cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.Ns{UUID: "uuid"}}, pref: ""},
		{uri: "ais://@uuid#ns", bck: cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.Ns{Name: "ns", UUID: "uuid"}}, pref: ""},
		{uri: "ais://bucket", bck: cmn.QueryBcks{Provider: apc.AIS, Name: "bucket"}, pref: ""},
		{uri: "ais://#ns/bucket", bck: cmn.QueryBcks{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}}, pref: ""},
		{uri: "ais://@uuid#ns/bucket", bck: cmn.QueryBcks{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns", UUID: "uuid"}}, pref: ""},
		{uri: "http://web.url/dataset", bck: cmn.QueryBcks{Provider: apc.HT, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}, pref: ""},
		{uri: "https://web.url/dataset", bck: cmn.QueryBcks{Provider: apc.HT, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}, pref: ""},
		{uri: "ais://bucket/objname", bck: cmn.QueryBcks{Provider: apc.AIS, Name: "bucket"}, pref: "objname"},
		{uri: "ais://bucket/aaa/bbb/objname", bck: cmn.QueryBcks{Provider: apc.AIS, Name: "bucket"}, pref: "aaa/bbb/objname"},
	}
	for _, test := range positiveTests {
		bck, pref, err := parseQueryBckURI(test.uri)
		tassert.Errorf(t, err == nil, "failed on %s with err: %v", test.uri, err)
		b := cmn.Bck(bck)
		tassert.Errorf(t, test.bck.Equal(&b), "failed on %s buckets are not equal (expected: %q, got: %q)", test.uri, test.bck, bck)
		tassert.Errorf(t, test.pref == pref, "parsed %s to invalid embedded prefix %q (expected %q)", test.uri, pref, test.pref)
	}

	negativeTests := []struct {
		uri string
	}{
		{uri: "bucket"},
		{uri: ":/"},
		{uri: "://"},
		{uri: "aiss://"},
		{uri: "aiss://bucket"},
		{uri: "ais:///objectname"},
		{uri: "ais:///aaa/bbb/objectname"},
		{uri: "ftp://unsupported"},
	}
	for _, test := range negativeTests {
		bck, pref, err := parseQueryBckURI(test.uri)
		tassert.Errorf(t, err != nil, "expected error parsing %q, got: (bck %q, pref %q)", test.uri, bck, pref)
	}
}

func TestParseBckURI(t *testing.T) {
	positiveTests := []struct {
		uri string
		bck cmn.Bck
	}{
		{uri: "ais://bucket", bck: cmn.Bck{Provider: apc.AIS, Name: "bucket"}},
		{uri: "ais://#ns/bucket", bck: cmn.Bck{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}}},
		{uri: "ais://@uuid#ns/bucket", bck: cmn.Bck{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns", UUID: "uuid"}}},
		{uri: "http://web.url/dataset", bck: cmn.Bck{Provider: apc.HT, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}},
		{uri: "https://web.url/dataset", bck: cmn.Bck{Provider: apc.HT, Name: "ZWUyYWFiOGEzYjEwMTJkNw"}},
	}
	for _, test := range positiveTests {
		bck, err := parseBckURI(&cli.Context{}, test.uri, true /*require provider*/)
		tassert.Errorf(t, err == nil, "failed on %s with err: %v", test.uri, err)
		tassert.Errorf(t, test.bck.Equal(&bck), "failed on %s buckets are not equal (expected: %q, got: %q)",
			test.uri, test.bck.String(), bck.String())
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
		bck, err := parseBckURI(&cli.Context{}, test.uri, true /*require provider*/)
		tassert.Errorf(t, err != nil, "expected error on %s (bck: %q)", test.uri, bck.String())
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
			bck: cmn.Bck{Provider: apc.AIS, Name: "bucket"},
		},
		{
			uri: "ais://bucket/object_name", optObjName: true,
			bck:     cmn.Bck{Provider: apc.AIS, Name: "bucket"},
			objName: "object_name",
		},
		{
			uri:     "ais://bucket/object_name",
			bck:     cmn.Bck{Provider: apc.AIS, Name: "bucket"},
			objName: "object_name",
		},
		{
			uri:     "ais://#ns/bucket/a/b/c",
			bck:     cmn.Bck{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}},
			objName: "a/b/c",
		},
		{
			uri: "ais://#ns/bucket", optObjName: true,
			bck: cmn.Bck{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns"}},
		},
		{
			uri:     "ais://@uuid#ns/bucket/object",
			bck:     cmn.Bck{Provider: apc.AIS, Name: "bucket", Ns: cmn.Ns{Name: "ns", UUID: "uuid"}},
			objName: "object",
		},
		{
			uri:     "http://web.url/dataset/object_name",
			bck:     cmn.Bck{Provider: apc.HT, Name: "ZWUyYWFiOGEzYjEwMTJkNw"},
			objName: "object_name",
		},
		{
			uri:     "https://web.url/dataset/object_name",
			bck:     cmn.Bck{Provider: apc.HT, Name: "ZWUyYWFiOGEzYjEwMTJkNw"},
			objName: "object_name",
		},
	}
	for _, test := range positiveTests {
		bck, objName, err := parseBckObjURI(&cli.Context{}, test.uri, test.optObjName)
		tassert.Errorf(t, err == nil, "failed on %s with err: %v", test.uri, err)
		tassert.Errorf(t, test.bck.Equal(&bck), "failed on %s buckets are not equal (expected: %q, got: %q)",
			test.uri, test.bck.String(), bck.String())
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
	}
	for _, test := range negativeTests {
		bck, objName, err := parseBckObjURI(&cli.Context{}, test.uri, test.optObjName)
		tassert.Errorf(t, err != nil, "expected error on %s (bck: %q, obj_name: %q)", test.uri, bck.String(), objName)
	}
}

func TestFlattenJSONNoStructEntries(t *testing.T) {
	// Test ensures struct containers don't show up in config display

	type LogConf struct {
		Level   int    `json:"level"`
		MaxSize string `json:"max_size"`
	}

	type TestConfig struct {
		Log LogConf `json:"log"`
	}

	config := TestConfig{
		Log: LogConf{Level: 5, MaxSize: "4MiB"},
	}

	// Test ais config cluster log.level 5
	result := flattenJSON(config, "log")

	resultMap := make(map[string]string)
	for _, nv := range result {
		resultMap[nv.Name] = nv.Value
	}

	// Should contain leaf fields
	tassert.Fatalf(t, resultMap["log.level"] == "5", "Expected log.level=5, got %q", resultMap["log.level"])
	tassert.Fatalf(t, resultMap["log.max_size"] == "4MiB", "Expected log.max_size=4MiB, got %q", resultMap["log.max_size"])

	// Should NOT contain the struct entry
	if structValue, found := resultMap["log"]; found {
		t.Errorf("REGRESSION: Found unwanted struct entry 'log' with value: %q", structValue)
	}

	// Should have exactly 2 entries
	if len(resultMap) != 2 {
		t.Errorf("Expected exactly 2 entries, got %d: %v", len(resultMap), resultMap)
	}
}

func TestFlattenJSONSectionFiltering(t *testing.T) {
	// Test that section filtering works correctly

	type TestConfig struct {
		Log struct {
			Level int `json:"level"`
		} `json:"log"`
		Mirror struct {
			Enabled bool `json:"enabled"`
		} `json:"mirror"`
	}

	config := TestConfig{}
	config.Log.Level = 3
	config.Mirror.Enabled = true

	// Test log section only
	logResult := flattenJSON(config, "log")
	logMap := make(map[string]string)
	for _, nv := range logResult {
		logMap[nv.Name] = nv.Value
	}

	// Should only have log fields
	tassert.Fatalf(t, logMap["log.level"] == "3", "Expected log.level=3, got %q", logMap["log.level"])
	if _, found := logMap["mirror.enabled"]; found {
		t.Errorf("Found mirror field in log section: %v", logMap)
	}

	// Test entire config
	allResult := flattenJSON(config, "")
	allMap := make(map[string]string)
	for _, nv := range allResult {
		allMap[nv.Name] = nv.Value
	}

	// Should have both sections
	tassert.Fatalf(t, allMap["log.level"] == "3", "Expected log.level=3 in full config")
	tassert.Fatalf(t, allMap["mirror.enabled"] == "true", "Expected mirror.enabled=true in full config")
}

func TestCheckCertExpiration(t *testing.T) {
	fcyan = fmt.Sprint

	node := &meta.Snode{}
	node.Init("test-node", "target")

	app := cli.NewApp()
	app.ErrWriter = io.Discard
	c := cli.NewContext(app, nil, nil)

	info := cos.StrKVs{"error": "tls-cert-expired"}
	result := checkCertExpiration(c, info, node)
	tassert.Errorf(t, result == 1, "expected 1 for error case, got %d", result)

	info = cos.StrKVs{"warning": "tls-cert-will-soon-expire"}
	result = checkCertExpiration(c, info, node)
	tassert.Errorf(t, result == 0, "expected 0 for warning case, got %d", result)

	info = cos.StrKVs{}
	result = checkCertExpiration(c, info, node)
	tassert.Errorf(t, result == 0, "expected 0 for no issues case, got %d", result)
}
