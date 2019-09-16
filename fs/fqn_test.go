package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestFQN2Info(t *testing.T) {
	tests := []struct {
		testName        string
		fqn             string
		mpaths          []string
		wantMPath       string
		wantContentType string
		wantBucket      string
		wantBckProvider string
		wantObjName     string
		wantErr         bool
	}{
		// good
		{
			"smoke test",
			"/tmp/obj/local/bucket/objname",
			[]string{"/tmp"},
			"/tmp", "obj", "bucket", cmn.AIS, "objname", false,
		},
		{
			"cloud as bucket type",
			"/tmp/obj/cloud/bucket/objname",
			[]string{"/tmp"},
			"/tmp", "obj", "bucket", cmn.Cloud, "objname", false,
		},
		{
			"long mount path name",
			"/tmp/super/long/obj/cloud/bucket/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "obj", "bucket", cmn.Cloud, "objname", false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "obj", "bucket", cmn.Cloud, "folder/objname", false,
		},
		{
			"multiple mpaths matching, choose the longest",
			"/tmp/super/long/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/super/long/long"},
			"/tmp/super/long/long", "obj", "bucket", cmn.Cloud, "folder/objname", false,
		},
		{
			"dirty mpath",
			"/tmp/super/long/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", "obj", "bucket", cmn.Cloud, "folder/objname", false,
		},

		// bad
		{
			"too short name",
			"/tmp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", cmn.Cloud, "", true,
		},
		{
			"empty bucket name",
			"/tmp/local//objname",
			[]string{"/tmp"},
			"", "", "", cmn.Cloud, "", true,
		},
		{
			"empty object name",
			"/tmp/local//objname",
			[]string{"/tmp"},
			"", "", "", cmn.Cloud, "", true,
		},
		{
			"empty bucket type",
			"/tmp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", cmn.Cloud, "", true,
		},
		{
			"bad bucket type",
			"/tmp/local_or_cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", "", cmn.Cloud, "", true,
		},
		{
			"no matching mountpath",
			"/tmp/local/bucket/objname",
			[]string{"/tmp/a", "/tmp/b"},
			"", "", "", cmn.Cloud, "", true,
		},
		{
			"fqn is mpath",
			"/tmp/mpath",
			[]string{"/tmp/mpath"},
			"", "", "", cmn.Cloud, "", true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			mfs := NewMountedFS()
			mfs.DisableFsIDCheck()

			for _, mpath := range tt.mpaths {
				if _, err := os.Stat(mpath); os.IsNotExist(err) {
					cmn.CreateDir(mpath)
					defer os.RemoveAll(mpath)
				}
				err := mfs.Add(mpath)
				if err != nil {
					t.Errorf("error in add mountpath: %v", err)
					return
				}
			}
			Mountpaths = mfs
			CSM.RegisterFileType(ObjectType, &ObjectContentResolver{})

			parsedFQN, err := mfs.FQN2Info(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fqn2info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			mpathInfo, gotContentType, gotBucket, gotBckProvider, gotObjName := parsedFQN.MpathInfo, parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.BckProvider, parsedFQN.ObjName
			gotMPath := mpathInfo.Path
			if gotMPath != tt.wantMPath {
				t.Errorf("fqn2info() gotMPath = %v, want %v", gotMPath, tt.wantMPath)
			}
			if gotContentType != tt.wantContentType {
				t.Errorf("fqn2info() gotMPath = %v, want %v", gotContentType, tt.wantContentType)
			}
			if gotBucket != tt.wantBucket {
				t.Errorf("fqn2info() gotBucket = %v, want %v", gotBucket, tt.wantBucket)
			}
			if gotBckProvider != tt.wantBckProvider {
				t.Errorf("fqn2info() gotBckProvider = %v, want %v", gotBckProvider, tt.wantBckProvider)
			}
			if gotObjName != tt.wantObjName {
				t.Errorf("fqn2info() gotObjName = %v, want %v", gotObjName, tt.wantObjName)
			}
		})
	}
}
