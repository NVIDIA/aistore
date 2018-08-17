package dfc

import (
	"os"
	"testing"

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
				CreateDir(tt.mpath)
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
