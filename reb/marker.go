package reb

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
)

const (
	globRebMarker  = ".global_rebalancing"
	localRebMarker = ".resilvering"
)

func MarkerExists(kind string) bool {
	avail, _ := fs.Mountpaths.Get()
	for _, mp := range avail {
		path := makeMarkerPath(mp.Path, kind)
		if err := fs.Access(path); err == nil {
			return true
		}
	}
	return false
}

func IsRebalancing(kind string) (aborted, running bool) {
	cmn.Assert(kind == cmn.ActGlobalReb || kind == cmn.ActLocalReb)
	if MarkerExists(kind) {
		aborted = true
	}
	entry := xaction.Registry.GetL(kind)
	if entry == nil {
		return
	}
	xact := entry.Get()
	running = !xact.Finished()
	if running {
		aborted = false
	}
	return
}

// persistent mark indicating rebalancing in progress
func makeMarkerPath(path, kind string) (pm string) {
	switch kind {
	case cmn.ActLocalReb:
		pm = filepath.Join(path, localRebMarker)
	case cmn.ActGlobalReb:
		pm = filepath.Join(path, globRebMarker)
	default:
		cmn.Assert(false)
	}
	return
}

func putMarker(kind string) error {
	var (
		mpath *fs.MountpathInfo
	)
	avail, _ := fs.Mountpaths.Get()
	// get random mountpath
	for _, mp := range avail {
		mpath = mp
		break
	}
	if mpath == nil {
		return errors.New("could not create rebalance marker: no mountpaths")
	}

	path := makeMarkerPath(mpath.Path, kind)
	_, err := cmn.CreateFile(path)
	return err
}

func removeMarker(kind string) error {
	var err error
	avail, _ := fs.Mountpaths.Get()
	deleted := false
	for _, mp := range avail {
		path := makeMarkerPath(mp.Path, kind)
		if errRm := os.Remove(path); errRm != nil && !os.IsNotExist(errRm) {
			glog.Errorf("failed to cleanup rebalance %s marker from %s: %v",
				kind, mp.Path, err)
			err = errRm
		} else if errRm == nil {
			deleted = true
		}
	}
	if err == nil && !deleted {
		err = errors.New("marker not found on any mpath")
	}
	return err
}
