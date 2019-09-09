package ais

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// Tests:
// - BMD Marshal & Unmarshal give the same result
// - Unmarshal fails if checksum is invalid
// - Unmarshal fails if xattr is corrupted (simulated by changing
//   a bucket's name inside previously marshaled data)
// - trash in - error out
func TestBMDSaveLoad(t *testing.T) {
	bmd := newBucketMD()
	bmd.add("local1", true, &cmn.BucketProps{})
	bmd.add("local3", true, &cmn.BucketProps{})
	bmd.add("cloud0", false, &cmn.BucketProps{})

	b := bmd.MarshalXattr()
	tutils.Logf("Original checksum: %v\n", b[:9])

	// corrupted checksum must return an error
	corrupted := make([]byte, len(b))
	copy(corrupted, b)
	for i := 0; i < 6; i++ {
		corrupted[i] = 'x'
	}
	tutils.Logf("Corrupted checksum: %v\n", corrupted[:9])
	bmdFail := &bucketMD{}
	err := bmdFail.UnmarshalXattr(corrupted)
	if err == nil {
		t.Errorf("Must fail when checksum is invalid")
	}

	// corrupted BMD (a.k.a checksums mismatch) must return an error
	tutils.Logf("Check corrupted BMD: hash %v\n", b[:9])
	corrupted = bytes.Replace(b, []byte("local3"), []byte("local9"), 1)
	err = bmdFail.UnmarshalXattr(corrupted)
	if err == nil {
		t.Errorf("Must fail when checksum is invalid: %v", corrupted)
	}

	// Valid payload must restore the original content of a bucket
	tutils.Logf("Original BMD: %v, must unmarshal successfully\n", b[:9])
	bmdOK := &bucketMD{}
	err = bmdOK.UnmarshalXattr(b)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bmdOK.Version == bmd.Version,
		"Versions mismatch %d -> %d", bmd.Version, bmdOK.Version)
	tassert.Fatalf(t, len(bmdOK.LBmap) == len(bmd.LBmap),
		"ais bucket lists mismatch %v -> %v", bmd.LBmap, bmdOK.LBmap)
	tassert.Fatalf(t, len(bmdOK.CBmap) == len(bmd.CBmap),
		"Cloud bucket lists mismatch %v -> %v", bmd.CBmap, bmdOK.CBmap)

	// Unmarshaling corrupted data(invalid payload) must fail
	err = bmdFail.UnmarshalXattr([]byte("invalid BMD Xattr"))
	if err == nil {
		t.Errorf("Must fail when payload is not valid JSON")
	}
}
