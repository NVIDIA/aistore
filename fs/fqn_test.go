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
		wantBck         cmn.Bck
		wantObjName     string
		wantErr         bool
	}{
		// good
		{
			"smoke test",
			"/tmp/~obj/@ais/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}, "objname", false,
		},
		{
			"content type (work)",
			"/tmp/~work/@aws/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.WorkfileType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}, "objname", false,
		},
		{
			"content type (empty - obj)",
			"/tmp/@ais/bucket/obj/name",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}, "obj/name", false,
		},
		{
			"cloud as bucket type (aws)",
			"/tmp/~obj/@aws/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}, "objname", false,
		},
		{
			"cloud as bucket type (gcp)",
			"/tmp/~obj/@gcp/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle, Ns: cmn.NsGlobal}, "objname", false,
		},
		{
			"global namespace (empty)",
			"/tmp/~obj/@ais/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}, "objname", false,
		},
		{
			"non-empty namespace",
			"/tmp/~obj/@ais/#namespace/bucket/objname",
			[]string{"/tmp"},
			"/tmp", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: "namespace"}, "objname", false,
		},
		{
			"long mount path name",
			"/tmp/super/long/~obj/@aws/bucket/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}, "objname", false,
		},
		{
			"long mount path name and objname in folder",
			"/tmp/super/long/~obj/@aws/bucket/folder/objname",
			[]string{"/tmp/super/long"},
			"/tmp/super/long", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}, "folder/objname", false,
		},
		{
			"multiple mpaths matching, choose the longest",
			"/tmp/super/long/long/~obj/@aws/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/super/long/long"},
			"/tmp/super/long/long", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}, "folder/objname", false,
		},
		{
			"dirty mpath",
			"/tmp/super/long/long/~obj/@gcp/bucket/folder/objname",
			[]string{"/tmp/super/long", "/tmp/.////super/../super//./long///////////long"},
			"/tmp/super/long/long", fs.ObjectType, cmn.Bck{Name: "bucket", Provider: cmn.ProviderGoogle, Ns: cmn.NsGlobal}, "folder/objname", false,
		},

		// bad
		{
			"too short name",
			"/tmp/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"invalid content type (not prefixed with '~')",
			"/tmp/obj/@gcp/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"invalid content type (unknown)",
			"/tmp/~unknown/@gcp/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"empty bucket name",
			"/tmp/~obj/@ais//objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"empty object name",
			"/tmp/~obj/@ais/bucket/",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"empty cloud provider",
			"/tmp/~obj/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"invalid cloud provider (not prefixed with '@')",
			"/tmp/~obj/gcp/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"invalid cloud provider (unknown)",
			"/tmp/~obj/@unknown/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"invalid cloud provider (cloud)",
			"/tmp/~obj/@cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"invalid cloud provider (local)",
			"/tmp/~obj/@cloud/bucket/objname",
			[]string{"/tmp"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"no matching mountpath",
			"/tmp/~obj/@ais/bucket/objname",
			[]string{"/tmp/a", "/tmp/b"},
			"", "", cmn.Bck{}, "", true,
		},
		{
			"fqn is mpath",
			"/tmp/mpath",
			[]string{"/tmp/mpath"},
			"", "", cmn.Bck{}, "", true,
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
			fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

			parsedFQN, err := mfs.ParseFQN(tt.fqn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fqn2info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			gotMpath, gotContentType, gotBck, gotObjName := parsedFQN.MpathInfo.Path, parsedFQN.ContentType, parsedFQN.Bck, parsedFQN.ObjName
			if gotMpath != tt.wantMPath {
				t.Errorf("gotMpath = %v, want %v", gotMpath, tt.wantMPath)
			}
			if gotContentType != tt.wantContentType {
				t.Errorf("gotContentType = %v, want %v", gotContentType, tt.wantContentType)
			}
			if !gotBck.Equal(tt.wantBck) {
				t.Errorf("gotBck = %v, want %v", gotBck, tt.wantBck)
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
		bck         cmn.Bck
		objName     string
	}{
		{
			mpath:       "/tmp/path",
			contentType: fs.ObjectType,
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: cmn.ProviderAIS,
				Ns:       cmn.NsGlobal,
			},
			objName: "object/name",
		},
		{
			mpath:       "/tmp/path",
			contentType: fs.WorkfileType,
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: cmn.ProviderAmazon,
				Ns:       "uuid10294",
			},
			objName: "object/name",
		},
		{
			mpath:       "/tmp/path",
			contentType: fs.ObjectType,
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: cmn.ProviderAmazon,
				Ns:       "alias",
			},
			objName: "object/name",
		},
		{
			mpath:       "/tmp/path",
			contentType: fs.ObjectType,
			bck: cmn.Bck{
				Name:     "bucket",
				Provider: cmn.ProviderGoogle,
				Ns:       cmn.NsGlobal,
			},
			objName: "object/name",
		},
	}

	for _, tt := range tests {
		t.Run(strings.Join([]string{tt.mpath, tt.contentType, tt.bck.String(), tt.objName}, "|"), func(t *testing.T) {
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
			fqn := mpaths[tt.mpath].MakePathCT(tt.contentType, tt.bck, tt.objName)

			parsedFQN, err := mfs.ParseFQN(fqn)
			if err != nil {
				t.Fatalf("failed to parse FQN: %v", err)
			}
			gotMpath, gotContentType, gotBck, gotObjName := parsedFQN.MpathInfo.Path, parsedFQN.ContentType, parsedFQN.Bck, parsedFQN.ObjName
			if gotMpath != tt.mpath {
				t.Errorf("gotMpath = %v, want %v", gotMpath, tt.mpath)
			}
			if gotContentType != tt.contentType {
				t.Errorf("getContentType = %v, want %v", gotContentType, tt.contentType)
			}
			if gotBck != tt.bck {
				t.Errorf("gotBck = %v, want %v", gotBck, tt.bck)
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
		bck   = cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
	)

	mfs.DisableFsIDCheck()
	cmn.CreateDir(mpath)
	defer os.RemoveAll(mpath)
	mfs.Add(mpath)
	fs.Mountpaths = mfs
	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})

	mpaths, _ := fs.Mountpaths.Get()
	fqn := mpaths[mpath].MakePathCT(fs.ObjectType, bck, "super/long/name")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsedFQN, _ = mfs.ParseFQN(fqn)
	}
}
