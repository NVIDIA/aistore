package fs_test

import (
	"os"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
)

func TestParseFQN(t *testing.T) {
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
			"/tmp/~obj/@ais/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, "bucket", cmn.ProviderAIS, "objname", false,
		},
		{
			"cloud as bucket type",
			"/tmp/~obj/@aws/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, "bucket", cmn.ProviderAmazon, "objname", false,
		},
		{
			"cloud as bucket type",
			"/tmp/~obj/@gcp/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, "bucket", cmn.ProviderGoogle, "objname", false,
		},
		{
			"long mount path name",
			"/tmp/super/long/~obj/@aws/bucket/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", fs.ObjectType, "bucket", cmn.ProviderAmazon, "objname", false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/~obj/@aws/bucket/folder/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", fs.ObjectType, "bucket", cmn.ProviderAmazon, "folder/objname", false,
		},
		{
			"multiple mpaths matching, choose the longest",
			"/tmp/super/long/long/~obj/@aws/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/super/long/long"},
			"/tmp/super/long/long", fs.ObjectType, "bucket", cmn.ProviderAmazon, "folder/objname", false,
		},
		{
			"dirty mpath",
			"/tmp/super/long/long/~obj/@gcp/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", fs.ObjectType, "bucket", cmn.ProviderGoogle, "folder/objname", false,
		},

		// bad
		{
			"too short name",
			"/tmp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"invalid content type (not prefixed with '~')",
			"/tmp/obj/@gcp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"invalid cloud provider (unknown)",
			"/tmp/~unknown/@gcp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"empty bucket name",
			"/tmp/~obj/@ais//objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"empty object name",
			"/tmp/~obj/@ais/bucket/",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"empty cloud provider",
			"/tmp/~obj/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"invalid cloud provider (not prefixed with '@')",
			"/tmp/~obj/gcp/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"invalid cloud provider (unknown)",
			"/tmp/~obj/@unknown/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"invalid cloud provider (cloud)",
			"/tmp/~obj/@cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"invalid cloud provider (local)",
			"/tmp/~obj/@cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", "", "", "", true,
		},
		{
			"no matching mountpath",
			"/tmp/~obj/@ais/bucket/objname",
			[]string{"/tmp/a", "/tmp/b"},
			"", "", "", "", "", true,
		},
		{
			"fqn is mpath",
			"/tmp/mpath",
			[]string{"/tmp/mpath"},
			"", "", "", "", "", true,
		},
	}

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

			parsedFQN, err := mfs.ParseFQN(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fqn2info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			gotMpath, gotContentType, gotBucket, gotProvider, gotObjName := parsedFQN.MpathInfo.Path, parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Provider, parsedFQN.ObjName
			if gotMpath != tt.wantMPath {
				t.Errorf("gotMpath = %v, want %v", gotMpath, tt.wantMPath)
			}
			if gotProvider != tt.wantProvider {
				t.Errorf("gotProvider = %v, want %v", gotProvider, tt.wantProvider)
			}
			if gotContentType != tt.wantContentType {
				t.Errorf("gotContentType = %v, want %v", gotContentType, tt.wantContentType)
			}
			if gotBucket != tt.wantBucket {
				t.Errorf("gotBucket = %v, want %v", gotBucket, tt.wantBucket)
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
		contentType string
		provider    string
		bucket      string
		objName     string
	}{
		{
			mpath:       "/tmp/path",
			contentType: fs.ObjectType,
			provider:    cmn.ProviderAIS,
			bucket:      "bucket",
			objName:     "object/name",
		},
		{
			mpath:       "/tmp/path",
			contentType: fs.WorkfileType,
			provider:    cmn.ProviderAmazon,
			bucket:      "bucket",
			objName:     "object/name",
		},
		{
			mpath:       "/tmp/path",
			contentType: fs.ObjectType,
			provider:    cmn.ProviderAmazon,
			bucket:      "bucket",
			objName:     "object/name",
		},
		{
			mpath:       "/tmp/path",
			contentType: fs.ObjectType,
			provider:    cmn.ProviderGoogle,
			bucket:      "bucket",
			objName:     "object/name",
		},
	}

	for _, tt := range tests {
		t.Run(strings.Join([]string{tt.mpath, tt.contentType, tt.provider, tt.bucket, tt.objName}, "|"), func(t *testing.T) {
			mios := ios.NewIOStaterMock()
			mfs := fs.NewMountedFS(mios)
			mfs.DisableFsIDCheck()

			if _, err := os.Stat(tt.mpath); os.IsNotExist(err) {
				cmn.CreateDir(tt.mpath)
				defer os.RemoveAll(tt.mpath)
			}
			err := mfs.Add(tt.mpath)
			if err != nil {
				t.Errorf("error in add mountpath: %v", err)
				return
			}

			fs.Mountpaths = mfs
			fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
			fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

			mpaths, _ := fs.Mountpaths.Get()
			fqn := mpaths[tt.mpath].MakePathBucketObject(tt.contentType, tt.bucket, tt.provider, tt.objName)

			parsedFQN, err := mfs.ParseFQN(fqn)
			if err != nil {
				t.Fatalf("failed to parse FQN: %v", err)
			}
			gotMpath, gotContentType, gotBucket, gotProvider, gotObjName := parsedFQN.MpathInfo.Path, parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Provider, parsedFQN.ObjName
			if gotMpath != tt.mpath {
				t.Errorf("gotMpath = %v, want %v", gotMpath, tt.mpath)
			}
			if gotContentType != tt.contentType {
				t.Errorf("getContentType = %v, want %v", gotContentType, tt.contentType)
			}
			if gotProvider != tt.provider {
				t.Errorf("gotProvider = %v, want %v", gotProvider, tt.provider)
			}
			if gotBucket != tt.bucket {
				t.Errorf("gotBucket = %v, want %v", gotBucket, tt.bucket)
			}
			if gotObjName != tt.objName {
				t.Errorf("gotObjName = %v, want %v", gotObjName, tt.objName)
			}
		})
	}
}

var (
	parsedFQN fs.ParsedFQN
)

func BenchmarkParseFQN(b *testing.B) {
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

	mpaths, _ := fs.Mountpaths.Get()
	fqn := mpaths[mpath].MakePathBucketObject(fs.ObjectType, "bucket", cmn.ProviderAIS, "super/long/name")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsedFQN, _ = mfs.ParseFQN(fqn)
	}
}
