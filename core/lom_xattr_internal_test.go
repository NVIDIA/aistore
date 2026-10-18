// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"

	onexxh "github.com/OneOfOne/xxhash"
)

// fixed-size values containing the record separator at every offset
var lmetaSepaValues = []uint64{
	0xe32fbd0102030405,
	0x01e32fbd02030405,
	0x0102e32fbd030405,
	0x010203e32fbd0405,
	0x01020304e32fbd05,
	0x0102030405e32fbd,
	0x01e32fe32fbd0203, // partial separator followed by the full one
}

// set record separator (not initialized w/o core.Init)
func lmetaTestSepa(t *testing.T) {
	saved := recdupSepa
	copy(recdupSepa[:], cmn.RecordSepa)
	t.Cleanup(func() { recdupSepa = saved })
}

// prepend preamble and checksum: malformed records must pass checksum verification to exercise the parser
func lmetaUnpackRaw(payload []byte) (md lmeta, err error) {
	buf := []byte{MetaverLOM, mdCksumTyXXHash}
	buf = binary.BigEndian.AppendUint64(buf, 0)
	buf = append(buf, payload...)
	binary.BigEndian.PutUint64(buf[2:], onexxh.Checksum64S(buf[prefLen:], cos.MLCG32))
	err = md.unpack(buf)
	return md, err
}

func lmetaFixedRec(key uint16, value uint64) []byte {
	b := binary.BigEndian.AppendUint16(nil, key)
	return binary.BigEndian.AppendUint64(b, value)
}

func lmetaVerRec(ver string) []byte {
	return append(binary.BigEndian.AppendUint16(nil, packedVer), ver...)
}

func TestLmetaUnpackFixedRecords(t *testing.T) {
	lmetaTestSepa(t)

	for _, tc := range []struct {
		name string
		key  uint16
	}{
		{"size", packedSize},
		{"lid", packedLid},
		{"flags", packedFlags},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unpack := func(record []byte) (lmeta, error) {
				var payload []byte
				if tc.key != packedSize {
					payload = lmetaFixedRec(packedSize, 0)
					payload = append(payload, recdupSepa[:]...)
				}
				return lmetaUnpackRaw(append(payload, record...))
			}

			for _, value := range lmetaSepaValues {
				if tc.key == packedFlags && value&lmflHRW != 0 {
					continue // persisted flags never carry HRW
				}
				record := lmetaFixedRec(tc.key, value)
				for _, trailing := range []bool{false, true} {
					t.Run(fmt.Sprintf("value=%x/trailing=%t", value, trailing), func(t *testing.T) {
						b := append([]byte(nil), record...)
						if !trailing {
							b = append(b, recdupSepa[:]...)
							b = append(b, lmetaVerRec("v1")...)
						}
						md, err := unpack(b)
						tassert.CheckFatal(t, err)
						var got uint64
						switch tc.key {
						case packedSize:
							got = uint64(md.Size)
						case packedLid:
							got = uint64(md.lid)
						case packedFlags:
							got = md.flags
						}
						tassert.Errorf(t, got == value, "expected %x, got %x", value, got)
						if !trailing {
							tassert.Errorf(t, md.Version() == "v1", "expected following version record, got %q", md.Version())
						}
					})
				}
			}

			record := lmetaFixedRec(tc.key, 0x01e32fbd12345678)
			for n := range cos.SizeofI64 {
				t.Run(fmt.Sprintf("short=%d", n), func(t *testing.T) {
					_, err := unpack(record[:cos.SizeofI16+n])
					tassert.Errorf(t, err != nil && err.Error() == badLmeta+": short binary record", "unexpected error: %v", err)
				})
			}
			for _, tail := range []string{"x", cmn.RecordSepa[:1], cmn.RecordSepa[:2], "\xe3/\xbc"} {
				t.Run(fmt.Sprintf("separator=%x", tail), func(t *testing.T) {
					_, err := unpack(append(append([]byte(nil), record...), tail...))
					tassert.Errorf(t, err != nil && err.Error() == badLmeta+": missing record separator", "unexpected error: %v", err)
				})
			}
			for _, tail := range []string{cmn.RecordSepa, cmn.RecordSepa + "\x00"} {
				t.Run(fmt.Sprintf("trailing-separator=%x", tail), func(t *testing.T) {
					_, err := unpack(append(append([]byte(nil), record...), tail...))
					tassert.Errorf(t, err != nil && err.Error() == badLmeta+": invalid record", "unexpected error: %v", err)
				})
			}
		})
	}
}

// records shorter than the key: empty, 1-byte, leading separator, trailing separator
func TestLmetaUnpackShortRecords(t *testing.T) {
	lmetaTestSepa(t)

	var (
		sepa = recdupSepa[:]
		size = lmetaFixedRec(packedSize, 0x01e32fbd12345678)
		ver  = lmetaVerRec("v1")
		join = func(parts ...[]byte) (b []byte) {
			for _, p := range parts {
				b = append(b, p...)
			}
			return b
		}
	)
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"empty", join(size, sepa, sepa, ver)},
		{"1-byte", join(size, sepa, []byte{0}, sepa, ver)},
		{"leading-separator", join(sepa, size)},
		{"trailing-separator", join(size, sepa, ver, sepa)},
		{"trailing-1-byte", join(size, sepa, ver, sepa, []byte{0})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := lmetaUnpackRaw(tc.payload)
			tassert.Errorf(t, err != nil && err.Error() == badLmeta+": invalid record", "unexpected error: %v", err)
		})
	}
}
