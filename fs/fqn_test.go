// Package fs_test provides tests for fs package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestParseFQN(t *testing.T) {
	const tmpMpath = "/tmp/ais-fqn-test"
	tests := []struct {
		testName        string
		fqn             string
		mpaths          []string
		wantMPath       string
		wantBck         cmn.Bck
		wantContentType string
		wantObjName     string
		wantErr         bool
		wantAddErr      bool
	}{
		// good
		{
			"smoke test",
			tmpMpath + "/@ais/#namespace/bucket/%ob/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.Ns{Name: "namespace"}},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"smoke test (namespace global)",
			tmpMpath + "/@ais/bucket/%ob/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.NsGlobal},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"content type (work)",
			tmpMpath + "/@aws/bucket/%wk/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.AWS, Ns: cmn.NsGlobal},
			fs.WorkCT, "objname", false,
			false,
		},
		{
			"cloud as bucket type (aws)",
			tmpMpath + "/@aws/bucket/%ob/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.AWS, Ns: cmn.NsGlobal},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"cloud as bucket type (gcp)",
			tmpMpath + "/@gcp/bucket/%ob/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.GCP, Ns: cmn.NsGlobal},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"non-empty namespace",
			tmpMpath + "/@ais/#namespace/bucket/%ob/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.Ns{Name: "namespace"}},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"cloud namespace",
			tmpMpath + "/@ais/@uuid#namespace/bucket/%ob/objname",
			[]string{tmpMpath},
			tmpMpath,
			cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.Ns{UUID: "uuid", Name: "namespace"}},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"long mount path name",
			tmpMpath + "/super/long/@aws/bucket/%ob/objname",
			[]string{tmpMpath + "/super/long"},
			tmpMpath + "/super/long",
			cmn.Bck{Name: "bucket", Provider: apc.AWS, Ns: cmn.NsGlobal},
			fs.ObjCT, "objname", false,
			false,
		},
		{
			"long mount path name and objname in folder",
			tmpMpath + "/super/long/@aws/bucket/%ob/folder/objname",
			[]string{tmpMpath + "/super/long"},
			tmpMpath + "/super/long",
			cmn.Bck{Name: "bucket", Provider: apc.AWS, Ns: cmn.NsGlobal},
			fs.ObjCT, "folder/objname", false,
			false,
		},

		// bad
		{
			"nested mountpaths",
			tmpMpath + "/super/long/long/@aws/bucket/%ob/folder/objname",
			[]string{"/super/long", "/super/long/long"},
			"",
			cmn.Bck{Name: "bucket", Provider: apc.AWS, Ns: cmn.NsGlobal},
			fs.ObjCT, "folder/objname", true,
			true,
		},
		{
			"too short name",
			tmpMpath + "/bucket/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid content type (not prefixed with '%')",
			tmpMpath + "/@gcp/bucket/ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid content type (empty)",
			tmpMpath + "/@ais/bucket/name",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid content type (unknown)",
			tmpMpath + "/@gcp/bucket/%un/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"empty bucket name",
			tmpMpath + "/@ais//%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"empty bucket name (without slash)",
			tmpMpath + "/@ais/%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"empty object name",
			tmpMpath + "/@ais/bucket/%ob/",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"empty backend provider",
			tmpMpath + "/bucket/%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid backend provider (not prefixed with '@')",
			tmpMpath + "/gcp/bucket/%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid backend provider (unknown)",
			tmpMpath + "/@unknown/bucket/%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid backend provider (cloud)",
			tmpMpath + "/@cloud/bucket/%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"invalid cloud namespace",
			tmpMpath + "/@cloud/@uuid/bucket/%ob/objname",
			[]string{tmpMpath},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"no matching mountpath",
			tmpMpath + "/@ais/bucket/%obj/objname",
			[]string{tmpMpath + "/a", tmpMpath + "/b"},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
		{
			"fqn is mpath",
			tmpMpath + "/mpath",
			[]string{tmpMpath + "/mpath"},
			"",
			cmn.Bck{},
			"", "", true,
			false,
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.testName, func(t *testing.T) {
			mios := mock.NewIOS()
			fs.TestNew(mios)

			for _, mpath := range tt.mpaths {
				if err := cos.Stat(mpath); cos.IsNotExist(err) {
					cos.CreateDir(mpath)
					defer os.RemoveAll(mpath)
				}
				_, err := fs.Add(mpath, "daeID")
				if err != nil && !tt.wantAddErr {
					tassert.CheckFatal(t, err)
				}
			}

			var parsed fs.ParsedFQN
			err := parsed.Init(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fqn2info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			var (
				gotMpath, gotBck           = parsed.Mountpath.Path, parsed.Bck
				gotContentType, gotObjName = parsed.ContentType, parsed.ObjName
			)
			if gotMpath != tt.wantMPath {
				t.Errorf("gotMpath = %v, want %v", gotMpath, tt.wantMPath)
			}
			if !gotBck.Equal(&tt.wantBck) {
				t.Errorf("gotBck = %v, want %v", gotBck, tt.wantBck)
			}
			if gotContentType != tt.wantContentType {
				t.Errorf("gotContentType = %v, want %v", gotContentType, tt.wantContentType)
			}
			if gotObjName != tt.wantObjName {
				t.Errorf("gotObjName = %v, want %v", gotObjName, tt.wantObjName)
			}
		})
	}
}

func TestMakeAndParseFQN(t *testing.T) {
	tests := []struct {
		mpath       string
		bck         cmn.Bck
		contentType string
		objName     string
	}{
		{
			mpath: "/tmp/path",
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: apc.AIS,
				Ns:       cmn.NsGlobal,
			},
			contentType: fs.ObjCT,
			objName:     "object/name",
		},
		{
			mpath: "/tmp/path",
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: apc.AWS,
				Ns:       cmn.Ns{UUID: "uuid", Name: "namespace"},
			},
			contentType: fs.WorkCT,
			objName:     "object/name",
		},
		{
			mpath: "/tmp/path",
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: apc.AWS,
				Ns:       cmn.Ns{Name: "alias"},
			},
			contentType: fs.ObjCT,
			objName:     "object/name",
		},
		{
			mpath: "/tmp/path",
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: apc.GCP,
				Ns:       cmn.NsGlobal,
			},
			contentType: fs.ObjCT,
			objName:     "object/name",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(strings.Join([]string{tt.mpath, tt.bck.String(), tt.contentType, tt.objName}, "|"), func(t *testing.T) {
			mios := mock.NewIOS()
			fs.TestNew(mios)

			if err := cos.Stat(tt.mpath); cos.IsNotExist(err) {
				cos.CreateDir(tt.mpath)
				defer os.RemoveAll(tt.mpath)
			}
			_, err := fs.Add(tt.mpath, "daeID")
			tassert.CheckFatal(t, err)

			mpaths := fs.GetAvail()
			fqn := mpaths[tt.mpath].MakePathFQN(&tt.bck, tt.contentType, tt.objName)

			var parsed fs.ParsedFQN
			err = parsed.Init(fqn)
			if err != nil {
				t.Fatalf("failed to parse FQN: %v", err)
			}
			var (
				gotMpath, gotBck           = parsed.Mountpath.Path, parsed.Bck
				gotContentType, gotObjName = parsed.ContentType, parsed.ObjName
			)
			if gotMpath != tt.mpath {
				t.Errorf("gotMpath = %v, want %v", gotMpath, tt.mpath)
			}
			if gotBck != tt.bck {
				t.Errorf("gotBck = %v, want %v", gotBck, tt.bck)
			}
			if gotContentType != tt.contentType {
				t.Errorf("getContentType = %v, want %v", gotContentType, tt.contentType)
			}
			if gotObjName != tt.objName {
				t.Errorf("gotObjName = %v, want %v", gotObjName, tt.objName)
			}
		})
	}
}

// Borderline obj names produce workfile basenames that overflow NAME_MAX
// once <tag>.<obj>.<tie>.<pid> decoration is added; verify CSM.Gen catches
// this and returns a basename within the per-component filesystem limit.
func TestWorkfileFntlBorderline(t *testing.T) {
	tmpMpath := t.TempDir()

	mios := mock.NewIOS()
	fs.TestNew(mios)
	_, err := fs.Add(tmpMpath, "daeID")
	tassert.CheckFatal(t, err)

	mpaths := fs.GetAvail()
	mi := mpaths[tmpMpath]
	bck := &cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.NsGlobal}

	tests := []struct {
		name    string
		objName string
	}{
		{"short obj name", "obj.bin"},
		{"borderline obj name (252B)", strings.Repeat("a", 252)},
		{"already-too-long obj name (300B)", strings.Repeat("b", 300)},
		{"extremely long obj name (4096B)", strings.Repeat("c", 4096)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fqn := fs.CSM.Gen(tc.objName, fs.WorkCT, bck, mi, fs.WorkfileColdget)
			base := filepath.Base(fqn)
			if len(base) > 255 {
				t.Fatalf("workfile basename %d > NAME_MAX: %q", len(base), base)
			}
		})
	}
}

func TestObjCTDeepPath(t *testing.T) {
	tmpMpath := t.TempDir()

	mios := mock.NewIOS()
	fs.TestNew(mios)
	_, err := fs.Add(tmpMpath, "daeID")
	tassert.CheckFatal(t, err)

	mpaths := fs.GetAvail()
	mi := mpaths[tmpMpath]
	bck := &cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.NsGlobal}

	tests := []struct {
		name        string
		objName     string
		expectFntl  bool // true = expect basename to be the .x<sha> hash form
		expectRound bool // true = expect ParseFQN to recover the original obj name
	}{
		{"flat short obj", "obj.bin", false, true},
		{"deep dir, short base, total=308B", strings.Repeat("p/", 150) + "file.bin", false, true},
		{"flat 252B obj (under NAME_MAX)", strings.Repeat("a", 252), false, true},
		{"flat 300B obj (over NAME_MAX)", strings.Repeat("b", 300), true, false},
		{"deep dir totaling >PATH_MAX budget", strings.Repeat("p/", 1700) + "file.bin", true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fqn := fs.CSM.Gen(tc.objName, fs.ObjCT, bck, mi, "")
			base := filepath.Base(fqn)
			if len(base) > 255 {
				t.Fatalf("obj basename %d > NAME_MAX: %q", len(base), base)
			}

			isHashed := fs.HasPrefixFntl(base)
			if isHashed != tc.expectFntl {
				t.Fatalf("expectFntl=%v, got hashed=%v (basename=%q)", tc.expectFntl, isHashed, base)
			}

			var parsed fs.ParsedFQN
			if err := parsed.Init(fqn); err != nil {
				t.Fatalf("ParseFQN: %v", err)
			}
			roundtrips := parsed.ObjName == tc.objName
			if roundtrips != tc.expectRound {
				t.Fatalf("expectRound=%v, got roundtrip=%v (parsed=%d bytes)",
					tc.expectRound, roundtrips, len(parsed.ObjName))
			}
		})
	}
}

func BenchmarkParseFQN(b *testing.B) {
	var (
		mpath = "/tmp/mpath"
		mios  = mock.NewIOS()
		bck   = cmn.Bck{Name: "bucket", Provider: apc.AIS, Ns: cmn.NsGlobal}
	)

	fs.TestNew(mios)
	cos.CreateDir(mpath)
	defer os.RemoveAll(mpath)
	fs.Add(mpath, "daeID")

	mpaths := fs.GetAvail()
	fqn := mpaths[mpath].MakePathFQN(&bck, fs.ObjCT, "super/long/name")

	for b.Loop() {
		var parsed fs.ParsedFQN
		parsed.Init(fqn)
	}
}
