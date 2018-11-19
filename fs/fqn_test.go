package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/dfcpub/cmn"
)

func TestFQN2Info(t *testing.T) {
	tests := []struct {
		testName        string
		fqn             string
		mpaths          []string
		wantMPath       string
		wantContentType string
		wantBucket      string
		wantObjName     string
		wantIsLocal     bool
		wantErr         bool
	}{
		// good
		{
			"smoke test",
			"/tmp/obj/local/bucket/objname",
			[]string{"/tmp"},
			"/tmp", "obj", "bucket", "objname", true, false,
		},
		{
			"/ as mountpath",
			"/obj/local/bucket/objname",
			[]string{"/"},
			"/", "obj", "bucket", "objname", true, false,
		},
		{
			"cloud as bucket type",
			"/tmp/obj/cloud/bucket/objname",
			[]string{"/tmp"},
			"/tmp", "obj", "bucket", "objname", false, false,
		},
		{
			"long mount path name",
			"/tmp/super/long/obj/cloud/bucket/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "obj", "bucket", "objname", false, false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "obj", "bucket", "folder/objname", false, false,
		},
		{
			"multiple mpaths matching, choose the longest",
			"/tmp/super/long/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/super/long/long"},
			"/tmp/super/long/long", "obj", "bucket", "folder/objname", false, false,
		},
		// Refer to https://golang.org/pkg/path/filepath/#Clean for the definition of a 'clean' fqn
		{
			"dirty fqn",
			"/tmp/.///super////long///..////long/long////..///long/obj//cloud////../cloud/bucket////..//bucket//folder/./../folder//objname",
			[]string{"/tmp/super/long/long"},
			"/tmp/super/long/long", "obj", "bucket", "folder/objname", false, false,
		},
		{
			"dirty fqn and mpath",
			"/tmp/.///super////long///..////long/long////..///long/obj//cloud////../cloud/bucket////..//bucket//folder/./../folder//objname",
			[]string{"/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", "obj", "bucket", "folder/objname", false, false,
		},
		{
			"dirty mpath",
			"/tmp/super/long/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", "obj", "bucket", "folder/objname", false, false,
		},

		// bad
		{
			"too short name",
			"/bucket/objname",
			[]string{"/"},
			"", "", "", "", false, true,
		},
		{
			"empty bucket name",
			"/local//objname",
			[]string{"/"},
			"", "", "", "", false, true,
		},
		{
			"empty object name",
			"/local//objname",
			[]string{"/"},
			"", "", "", "", false, true,
		},
		{
			"empty bucket type",
			"//bucket/objname",
			[]string{"/"},
			"", "", "", "", false, true,
		},
		{
			"bad bucket type",
			"/tmp/local_or_cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", false, true,
		},
		{
			"no matching mountpath",
			"/tmp/local/bucket/objname",
			[]string{"/tmp/a", "/tmp/b"},
			"", "", "", "", false, true,
		},
		{
			"fqn is mpath",
			"/tmp/mpath",
			[]string{"/tmp/mpath"},
			"", "", "", "", false, true,
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
			mpathInfo, gotContentType, gotBucket, gotObjName, gotIsLocal := parsedFQN.MpathInfo, parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname, parsedFQN.IsLocal
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
			if gotObjName != tt.wantObjName {
				t.Errorf("fqn2info() gotObjName = %v, want %v", gotObjName, tt.wantObjName)
			}
			if gotIsLocal != tt.wantIsLocal {
				t.Errorf("fqn2info() gotIsLocal = %v, want %v", gotIsLocal, tt.wantIsLocal)
			}
		})
	}
}
