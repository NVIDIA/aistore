package fs_test

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
)

func TestFQN2Info(t *testing.T) {
	tests := []struct {
		testName        string
		fqn             string
		mpaths          []string
		wantMPath       string
		wantContentType string
		wantBucket      string
		wantProvider    string
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
			"/tmp", "obj", "bucket", cmn.ProviderAmazon, "objname", false,
		},
		{
			"long mount path name",
			"/tmp/super/long/obj/cloud/bucket/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "obj", "bucket", cmn.ProviderAmazon, "objname", false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", "obj", "bucket", cmn.ProviderAmazon, "folder/objname", false,
		},
		{
			"multiple mpaths matching, choose the longest",
			"/tmp/super/long/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/super/long/long"},
			"/tmp/super/long/long", "obj", "bucket", cmn.ProviderAmazon, "folder/objname", false,
		},
		{
			"dirty mpath",
			"/tmp/super/long/long/obj/cloud/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", "obj", "bucket", cmn.ProviderAmazon, "folder/objname", false,
		},

		// bad
		{
			"too short name",
			"/tmp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
		{
			"empty bucket name",
			"/tmp/local//objname",
			[]string{"/tmp"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
		{
			"empty object name",
			"/tmp/local//objname",
			[]string{"/tmp"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
		{
			"empty bucket type",
			"/tmp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
		{
			"bad bucket type",
			"/tmp/local_or_cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
		{
			"no matching mountpath",
			"/tmp/local/bucket/objname",
			[]string{"/tmp/a", "/tmp/b"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
		{
			"fqn is mpath",
			"/tmp/mpath",
			[]string{"/tmp/mpath"},
			"", "", "", cmn.ProviderAmazon, "", true,
		},
	}

	cfg := cmn.GCO.BeginUpdate()
	cfg.CloudProvider = cmn.ProviderAmazon
	cmn.GCO.CommitUpdate(cfg)

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			mios := ios.NewIOStaterMock()
			mfs := fs.NewMountedFS(mios)
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
			fs.Mountpaths = mfs
			fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})

			parsedFQN, err := mfs.FQN2Info(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fqn2info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			mpathInfo, gotContentType, gotBucket, gotProvider, gotObjName := parsedFQN.MpathInfo, parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Provider, parsedFQN.ObjName
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
			if gotProvider != tt.wantProvider {
				t.Errorf("fqn2info() gotProvider = %v, want %v", gotProvider, tt.wantProvider)
			}
			if gotObjName != tt.wantObjName {
				t.Errorf("fqn2info() gotObjName = %v, want %v", gotObjName, tt.wantObjName)
			}
		})
	}
}

var (
	parsedFQN fs.ParsedFQN
)

func BenchmarkFQN2Info(b *testing.B) {
	var (
		mpath = "/tmp/mpath"
		mios  = ios.NewIOStaterMock()
		mfs   = fs.NewMountedFS(mios)
	)

	mfs.DisableFsIDCheck()
	cmn.CreateDir(mpath)
	defer os.RemoveAll(mpath)
	mfs.Add(mpath)
	fs.Mountpaths = mfs
	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsedFQN, _ = mfs.FQN2Info("/tmp/mpath/obj/local/bucket/super/long/name")
	}
}
