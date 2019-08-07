package ec

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

type (
	// Xaction responsible for responding to EC requests of other targets
	// Should not be stopped if number of known targets is small
	XactRespond struct {
		xactECBase
	}
)

func NewRespondXact(t cluster.Target, smap cluster.Sowner,
	si *cluster.Snode, bucket string, reqBundle, respBundle *transport.StreamBundle) *XactRespond {

	runner := &XactRespond{
		xactECBase: newXactECBase(t, smap, si, bucket, reqBundle, respBundle),
	}

	return runner
}

func (r *XactRespond) Run() (err error) {
	glog.Infof("Starting %s", r.Getname())

	conf := cmn.GCO.Get()
	tck := time.NewTicker(conf.Periodic.StatsTime)
	lastAction := time.Now()
	idleTimeout := conf.Timeout.SendFile * 3

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-tck.C:
			if s := fmt.Sprintf("%v", r.stats.stats()); s != "" {
				glog.Info(s)
			}
		case <-r.ChanCheckTimeout():
			idleEnds := lastAction.Add(idleTimeout)
			if idleEnds.Before(time.Now()) && r.Timeout() {
				if glog.V(4) {
					glog.Infof("Idle time is over: %v. Last action at: %v",
						time.Now(), lastAction)
				}

				r.stop()

				return nil
			}

		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		}
	}
}

// Utility function to cleanup both object/slice and its meta on the local node
// Used when processing object deletion request
func (r *XactRespond) removeObjAndMeta(bucket, objname string, bckIsLocal bool) error {
	if glog.V(4) {
		glog.Infof("Delete request for %s/%s", bucket, objname)
	}

	// to be consistent with PUT, object's files are deleted in a reversed
	// order: first Metafile is removed, then Replica/Slice
	// Why: the main object is gone already, so we do not want any target
	// responds that it has the object because it has metafile. We delete
	// metafile that makes remained slices/replicas outdated and can be cleaned
	// up later by LRU or other runner
	for _, tp := range []string{MetaType, fs.ObjectType, SliceType} {
		fqnMeta, _, err := cluster.HrwFQN(tp, bucket, objname, bckIsLocal)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(fqnMeta); err != nil {
			return fmt.Errorf("error removing %s %q: %v", tp, fqnMeta, err)
		}
	}

	return nil
}

// DispatchReq is responsible for handling request from other targets
func (r *XactRespond) DispatchReq(iReq IntraReq, bucket, objName string) {
	bckIsLocal := r.t.GetBowner().Get().IsLocal(bucket)
	daemonID := iReq.Sender

	switch iReq.Act {
	case reqDel:
		// object cleanup request: delete replicas, slices and metafiles
		if err := r.removeObjAndMeta(bucket, objName, bckIsLocal); err != nil {
			glog.Errorf("Failed to delete %s/%s: %v", bucket, objName, err)
		}
	case reqGet:
		// slice or replica request: send the object's data to the caller
		var (
			fqn string
			err error
		)
		if iReq.IsSlice {
			if glog.V(4) {
				glog.Infof("Received request for slice %d of %s", iReq.Meta.SliceID, objName)
			}
			fqn, _, err = cluster.HrwFQN(SliceType, bucket, objName, bckIsLocal)
		} else {
			if glog.V(4) {
				glog.Infof("Received request for replica %s", objName)
			}
			// FIXME: (redundant) r.dataResponse() does not need it as it constructs
			//        LOM right away
			fqn, _, err = cluster.HrwFQN(fs.ObjectType, bucket, objName, bckIsLocal)
		}
		if err != nil {
			glog.Error(err)
			return
		}

		if err := r.dataResponse(RespPut, fqn, bucket, objName, daemonID); err != nil {
			glog.Errorf("Failed to send back [GET req] %q: %v", fqn, err)
		}
	case ReqMeta:
		// metadata request: send the metadata to the caller
		fqn, _, err := cluster.HrwFQN(MetaType, bucket, objName, bckIsLocal)
		if err != nil {
			glog.Error(err)
			return
		}

		if err := r.dataResponse(iReq.Act, fqn, bucket, objName, daemonID); err != nil {
			glog.Errorf("Failed to send back [META req] %q: %v", fqn, err)
		}
	default:
		// invalid request detected
		glog.Errorf("Invalid request type %d", iReq.Act)
	}
}

func (r *XactRespond) DispatchResp(iReq IntraReq, bucket, objName string, objAttrs transport.ObjectAttrs, object io.Reader) {
	uname := unique(iReq.Sender, bucket, objName)

	drain := func() {
		if err := cmn.DrainReader(object); err != nil {
			glog.Warningf("Failed to drain reader %s/%s: %v", bucket, objName, err)
		}
	}

	switch iReq.Act {
	case ReqPut:
		// a remote target sent a replica/slice while it was
		// encoding or restoring an object. In this case it just saves
		// the sent replica or slice to a local file along with its metadata
		// look for metadata in request
		if glog.V(4) {
			glog.Infof("Response from %s, %s", iReq.Sender, uname)
		}

		// First check if the request is valid: it must contain metadata
		meta := iReq.Meta
		if meta == nil {
			drain()
			glog.Errorf("No metadata in request for %s/%s", bucket, objName)
			return
		}

		// Check if the request is valid (e.g, a request may come after
		// the bucket is destroyed.
		bckIsLocal := r.t.GetBowner().Get().IsLocal(bucket)
		if !bckIsLocal {
			// TODO: now EC supports only local buckets, so check if a local bucket exists
			// NOTE: must read and discard otherwise next reads from the stream would fail
			drain()
			glog.Warningf("Received an EC slice/replica for non-existing bucket: %s/%s", bucket, objName)
			return
		}

		var (
			objFQN string
			lom    *cluster.LOM
			err    error
		)
		if iReq.IsSlice {
			if glog.V(4) {
				glog.Infof("Got slice response from %s (#%d of %s/%s)",
					iReq.Sender, iReq.Meta.SliceID, bucket, objName)
			}
			objFQN, _, err = cluster.HrwFQN(SliceType, bucket, objName, bckIsLocal)
			if err != nil {
				drain()
				glog.Error(err)
				return
			}
		} else {
			if glog.V(4) {
				glog.Infof("Got replica response from %s (%s/%s)",
					iReq.Sender, bucket, objName)
			}
			// FIXME: vs. lom.Fill() a few lines below
			objFQN, _, err = cluster.HrwFQN(fs.ObjectType, bucket, objName, bckIsLocal)
			if err != nil {
				drain()
				glog.Error(err)
				return
			}
		}

		// save slice/object
		tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec")
		buf, slab := mm.AllocDefault()
		_, err = cmn.SaveReaderSafe(tmpFQN, objFQN, object, buf, false)
		if err == nil {
			lom, err = cluster.LOM{FQN: objFQN, T: r.t}.Init("")
			if err != nil {
				glog.Errorf("Failed to read resolve FQN %s: %s", objFQN, err)
				slab.Free(buf)
				return
			}
			lom.SetVersion(objAttrs.Version)
			lom.SetAtimeUnix(objAttrs.Atime)
			lom.SetSize(objAttrs.Size)

			// LOM checksum is filled with checksum of a slice. Source object's checksum is stored in metadata
			if objAttrs.CksumType != "" {
				lom.SetCksum(cmn.NewCksum(objAttrs.CksumType, objAttrs.CksumValue))
			}

			err = lom.Persist()
		}

		if err != nil {
			drain()
			glog.Errorf("Failed to save %s/%s data to %q: %v",
				bucket, objName, objFQN, err)
			slab.Free(buf)
			return
		}

		// save its metadata
		metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
		metaBuf := meta.marshal()
		metaLen := len(metaBuf)
		_, err = cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), buf, false)

		if err == nil {
			lom, err = cluster.LOM{FQN: metaFQN, T: r.t}.Init("")
			if err == nil {
				lom.SetSize(int64(metaLen))
				err = lom.Persist()
			}
		}

		slab.Free(buf)
		if err != nil {
			glog.Errorf("Failed to save metadata to %q: %v", metaFQN, err)
		}
	default:
		// should be unreachable
		glog.Errorf("Invalid request type: %d", iReq.Act)
	}
}

func (r *XactRespond) Description() string {
	return "responsible for returning stored EC files on requests from other targets"
}

func (r *XactRespond) Stop(error) { r.Abort() }

func (r *XactRespond) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}

	r.XactDemandBase.Stop()
	r.EndTime(time.Now())
}
