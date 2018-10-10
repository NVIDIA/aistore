package dfc

import (
	"os"
	"testing"

	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/fs"
)

func TestSplitFQN(t *testing.T) {
	tests := []struct {
		testName    string
		fqn         string
		mpath       string
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
			"/tmp",
			"/tmp", "bucket", "objname", true, false,
		},
		{
			"/ as mountpath",
			"/local/bucket/objname",
			"/",
			"/", "bucket", "objname", true, false,
		},
		{
			"cloud as bucket type",
			"/tmp/cloud/bucket/objname",
			"/tmp",
			"/tmp", "bucket", "objname", false, false,
		},
		{
			"long mount path name",
			"/tmp/super/long/cloud/bucket/objname",
			"/tmp/super/long",
			"/tmp/super/long", "bucket", "objname", false, false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/cloud/bucket/folder/objname",
			"/tmp/super/long",
			"/tmp/super/long", "bucket", "folder/objname", false, false,
		},

		// bad
		{
			"too short name",
			"/bucket/objname",
			"/",
			"", "", "", false, true,
		},
		{
			"empty bucket name",
			"/local//objname",
			"/",
			"", "", "", false, true,
		},
		{
			"empty object name",
			"/local//objname",
			"/",
			"", "", "", false, true,
		},
		{
			"empty bucket type",
			"//bucket/objname",
			"/",
			"", "", "", false, true,
		},
		{
			"bad bucket type",
			"/tmp/local_or_cloud/bucket/objname",
			"/tmp",
			"", "", "", false, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			mfs := fs.NewMountedFS()
			if _, err := os.Stat(tt.mpath); os.IsNotExist(err) {
				common.CreateDir(tt.mpath)
				defer os.RemoveAll(tt.mpath)
			}
			err := mfs.AddMountpath(tt.mpath)
			if err != nil {
				t.Errorf("error in add mountpath: %v", err)
				return
			}
			ctx.mountpaths = mfs

			gotMPath, gotBucket, gotObjName, gotIsLocal, err := splitFQN(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitFQN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if gotMPath != tt.wantMPath {
				t.Errorf("splitFQN() gotMPath = %v, want %v", gotMPath, tt.wantMPath)
			}
			if gotBucket != tt.wantBucket {
				t.Errorf("splitFQN() gotBucket = %v, want %v", gotBucket, tt.wantBucket)
			}
			if gotObjName != tt.wantObjName {
				t.Errorf("splitFQN() gotObjName = %v, want %v", gotObjName, tt.wantObjName)
			}
			if gotIsLocal != tt.wantIsLocal {
				t.Errorf("splitFQN() gotIsLocal = %v, want %v", gotIsLocal, tt.wantIsLocal)
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
		s := common.B2S(tst.val, tst.num)
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
		n, e := common.S2B(tst.str)
		if e != nil {
			t.Errorf("Failed to convert %s: %v", tst.str, e)
		}
		if n != tst.val {
			t.Errorf("Expected %d got %d", tst.val, n)
		}
	}
}
