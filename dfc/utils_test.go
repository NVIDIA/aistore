/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"testing"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

func TestFqn2Info(t *testing.T) {
	tests := []struct {
		testName    string
		fqn         string
		mpaths      []string
		wantMPath   string
		wantBucket  string
		wantObjName string
		wantIsLocal bool
		wantErr     bool
	}{
		// good
		{
			"smoke test",
			"/tmp/local/bucket/objname",
			[]string{"/tmp"},
			"/tmp", "bucket", "objname", true, false,
		},
		{
			"/ as mountpath",
			"/local/bucket/objname",
			[]string{"/"},
			"/", "bucket", "objname", true, false,
		},
		{
			"cloud as bucket type",
			"/tmp/cloud/bucket/objname",
			[]string{"/tmp"},
			"/tmp", "bucket", "objname", false, false,
		},
		{
			"long mount path name",
			"/tmp/super/long/cloud/bucket/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "bucket", "objname", false, false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "bucket", "folder/objname", false, false,
		},
		{
			"multiple mpaths matching, choose the longest",
			"/tmp/super/long/long/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/super/long/long"},
			"/tmp/super/long/long", "bucket", "folder/objname", false, false,
		},
		// Refer to https://golang.org/pkg/path/filepath/#Clean for the definition of a 'clean' fqn
		{
			"dirty fqn",
			"/tmp/.///super////long///..////long/long////..///long//cloud////../cloud/bucket////..//bucket//folder/./../folder//objname",
			[]string{"/tmp/super/long/long"},
			"/tmp/super/long/long", "bucket", "folder/objname", false, false,
		},
		{
			"dirty fqn and mpath",
			"/tmp/.///super////long///..////long/long////..///long//cloud////../cloud/bucket////..//bucket//folder/./../folder//objname",
			[]string{"/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", "bucket", "folder/objname", false, false,
		},
		{
			"dirty mpath",
			"/tmp/super/long/long/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", "bucket", "folder/objname", false, false,
		},

		// bad
		{
			"too short name",
			"/bucket/objname",
			[]string{"/"},
			"", "", "", false, true,
		},
		{
			"empty bucket name",
			"/local//objname",
			[]string{"/"},
			"", "", "", false, true,
		},
		{
			"empty object name",
			"/local//objname",
			[]string{"/"},
			"", "", "", false, true,
		},
		{
			"empty bucket type",
			"//bucket/objname",
			[]string{"/"},
			"", "", "", false, true,
		},
		{
			"bad bucket type",
			"/tmp/local_or_cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", "", false, true,
		},
		{
			"no matching mountpath",
			"/tmp/local/bucket/objname",
			[]string{"/tmp/a", "/tmp/b"},
			"", "", "", false, true,
		},
		{
			"fqn is mpath",
			"/tmp/mpath",
			[]string{"/tmp/mpath"},
			"", "", "", false, true,
		},
	}

	ctx.config.CloudBuckets = "cloud"
	ctx.config.LocalBuckets = "local"

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			mfs := fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
			mfs.DisableFsIDCheck()
			for _, mpath := range tt.mpaths {
				if _, err := os.Stat(mpath); os.IsNotExist(err) {
					cmn.CreateDir(mpath)
					defer os.RemoveAll(mpath)
				}
				err := mfs.AddMountpath(mpath)
				if err != nil {
					t.Errorf("error in add mountpath: %v", err)
					return
				}
			}
			fs.Mountpaths = mfs

			parsedFQN, err := fqn2info(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fqn2info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			mpathInfo, gotBucket, gotObjName, gotIsLocal := parsedFQN.mpathInfo, parsedFQN.bucket, parsedFQN.objname, parsedFQN.islocal
			gotMPath := mpathInfo.Path
			if gotMPath != tt.wantMPath {
				t.Errorf("fqn2info() gotMPath = %v, want %v", gotMPath, tt.wantMPath)
			}
			if gotBucket != tt.wantBucket {
				t.Errorf("fqn2info() gotBucket = %v, want %v", gotBucket, tt.wantBucket)
			}
			if gotObjName != tt.wantObjName {
				t.Errorf("fqn2info() gotObjName = %v, want %v", gotObjName, tt.wantObjName)
			}
			if gotIsLocal != tt.wantIsLocal {
				t.Errorf("fqn2info() gotIsLocal = %v, want %v", gotIsLocal, tt.wantIsLocal)
			}
		})
	}
}

func TestBytesToStr(t *testing.T) {
	type tstruct struct {
		val int64
		num int
		str string
	}
	tests := []tstruct{
		{0, 0, "0B"},
		{0, 1, "0B"},
		{10, 0, "10B"},
		{1000, 0, "1000B"},
		{1100, 0, "1KiB"},
		{1100, 2, "1.07KiB"},
		{1024 * 1000, 0, "1000KiB"},
		{1024 * 1025, 0, "1MiB"},
		{1024 * 1024 * 1024 * 3, 3, "3.000GiB"},
		{1024 * 1024 * 1024 * 1024 * 17, 0, "17TiB"},
		{1024 * 1024 * 1024 * 1024 * 1024 * 2, 0, "2048TiB"},
	}

	for _, tst := range tests {
		s := cmn.B2S(tst.val, tst.num)
		if s != tst.str {
			t.Errorf("Expected %s got %s", tst.str, s)
		}
	}
}

func TestStrToBytes(t *testing.T) {
	type tstruct struct {
		val int64
		str string
	}
	tests := []tstruct{
		{0, "0"},
		{0, "0B"},
		{10, "10B"},
		{1000, "1000B"},
		{1024 * 1000, "1000KiB"},
		{1024 * 1024, "1MiB"},
		{1024 * 1024 * 2, "2m"},
		{1024 * 1024 * 1024 * 3, "3GiB"},
		{1024 * 1024 * 1024 * 1024 * 17, "17TiB"},
		{1024 * 1024 * 1024 * 1024 * 1024 * 2, "2048TiB"},
	}

	for _, tst := range tests {
		n, e := cmn.S2B(tst.str)
		if e != nil {
			t.Errorf("Failed to convert %s: %v", tst.str, e)
		}
		if n != tst.val {
			t.Errorf("Expected %d got %d", tst.val, n)
		}
	}
}
