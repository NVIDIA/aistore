package reb

import (
	"testing"

	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

type testObject struct {
	bucket string
	name   string
	ais    bool
}

func TestECWaiter(t *testing.T) {
	const (
		sliceCnt     = 3
		sliceDone    = 2
		toCleanLastN = 3
	)
	wt := newWaiter(memsys.GMM())
	// must be longer than ecRebBatchSize
	objs := []testObject{
		{"bck1", "obj1", false},
		{"bck1", "obj2", false},
		{"bck1", "obj2", true},
		{"bck1", "obj3", true},
		{"bck2", "obj1", false},
		{"bck2", "obj4", true},
		{"bck5", "obj1", false},
		{"bck5", "obj2", false},
		{"bck5", "obj3", true},
		{"bck5", "obj4", true},
		{"bck5", "obj5", false},
		{"bck5", "obj6", false},
		{"bck5", "obj8", true},
		{"bck5", "obj9", true},
	}
	rebObjs := make([]*ecRebObject, 0)
	// check that generated uids are unique
	objSet := make(map[string]struct{})
	for _, o := range objs {
		uname := uniqueWaitID(o.bucket, o.name, o.ais)
		objSet[uname] = struct{}{}
		rebObjs = append(rebObjs, &ecRebObject{uid: uname})
	}
	tassert.Fatalf(t, len(objSet) == len(rebObjs), "uniqueWaitID failed: %d unique of %d", len(objSet), len(rebObjs))

	// check that for every unique uid the unique waitSlice is created
	created := make([]*ecWaitSlice, 0, len(rebObjs))
	for _, o := range rebObjs {
		for i := 0; i < sliceCnt; i++ {
			ws := wt.lookupCreate(o.uid, int16(i))
			for _, s := range created {
				tassert.Errorf(t, s != ws, "Lookup create returned duplicated pointer for %s[%d]", o.uid, i)
			}
			created = append(created, ws)
		}
	}
	tassert.Fatalf(t, len(created) == len(rebObjs)*sliceCnt, "lookupCreate failed: %d unique of %d", len(created), len(rebObjs)*sliceCnt)

	// check that generating waitSlice for existing item returns existing waitSlice
	for _, o := range rebObjs {
		for i := 0; i < sliceCnt; i++ {
			ws := wt.lookupCreate(o.uid, int16(i))
			found := false
			for _, s := range created {
				if s == ws {
					found = true
					break
				}
			}
			tassert.Errorf(t, found, "Lookup created waitSlice for %s instead of returning existing one", o.uid)
		}
	}
	tassert.Fatalf(t, int(wt.waitSliceCnt()) == len(rebObjs)*sliceCnt, "Waiting for different number of slices %d instead of %d", wt.waitSliceCnt(), len(rebObjs)*sliceCnt)

	// done must decrease the number of waiting slices
	for i := 0; i < sliceDone; i++ {
		wt.done()
	}
	tassert.Fatalf(t, int(wt.waitSliceCnt()) == len(rebObjs)*sliceCnt-sliceDone, "done must decrease the number of waiting slices to %d, but it is of %d", len(rebObjs)*sliceCnt-sliceDone, wt.waitSliceCnt())

	// cleanup invalid batch must not change waitSlice list
	wt.cleanupBatch(rebObjs, len(rebObjs)+10)
	tassert.Fatalf(t, len(wt.objs) == len(rebObjs), "cleanup batch with invalid start index must not clean up amnything, but the number of slices changed to %d, must be %d", len(wt.objs), len(rebObjs))

	tassert.Fatalf(t, len(objs) > ecRebBatchSize, "Number of objects is insufficient: %d, must be greater than %d", len(objs), ecRebBatchSize)

	// cleanup in the middle
	wt.cleanupBatch(rebObjs, len(rebObjs)-ecRebBatchSize-3)
	tassert.Fatalf(t, len(wt.objs) == len(rebObjs)-ecRebBatchSize, "cleanup middle batch must remove %d items, actually it is %d instead of %d", ecRebBatchSize, len(wt.objs), len(rebObjs)-ecRebBatchSize)
	// after that first and last item should exist
	currLen := len(wt.objs)
	_ = wt.lookupCreate(rebObjs[0].uid, 1)
	_ = wt.lookupCreate(rebObjs[len(rebObjs)-1].uid, 1)
	tassert.Fatalf(t, currLen == len(wt.objs), "first and last items must be preserved")

	// cleanup last incomplete batch
	wt.cleanupBatch(rebObjs, len(rebObjs)-2)
	tassert.Fatalf(t, currLen-2 == len(wt.objs), "Must cleanup the last two items but the actual size is %d, must be %d", len(wt.objs), currLen-2)

	// cleanup everything
	wt.cleanup()
	tassert.Errorf(t, wt.waitSliceCnt() == 0, "waiting slice count must be 0")
	tassert.Errorf(t, len(wt.objs) == 0, "slice list must be empty")
}
