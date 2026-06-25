// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Target signing key is replicated at mountpath roots.
// Loading is intentionally tolerant of per-mountpath access failures:
//
// - missing xattr: not an error;
// - plain IO/access/xattr failure: keep scanning;
// - corrupted key, daemon-ID mismatch, or key mismatch across mountpaths:
//   storage-integrity error and therefore hard failure.
//
// Writing remains strict: mountpath enable/add must fail if the key cannot be
// validated or persisted on that mountpath.

func (mi *Mountpath) SetNodeSigningKeyXattr(tid string, pair *cos.NodeSigningKey) error {
	cos.Assert(tid != "")
	debug.Assert(pair != nil)

	mpathKey, err := _loadXattrNodeSigningKey(mi.Path, tid)
	if err != nil {
		return err
	}
	if mpathKey != nil {
		if mpathKey.Equal(pair) {
			return nil
		}
		return &ErrStorageIntegrity{
			Code: SieMpathSigningKeyMismatch,
			Msg:  fmt.Sprintf("target signing key mismatch: %q(%q)", tid, mi),
		}
	}

	return SetXattr(mi.Path, xattrNodeSigningKey, pair.Bytes(tid))
}

func LoadNodeSigningKey(mpaths cos.StrKVs, tid string) (pair *cos.NodeSigningKey, err error) {
	var (
		lastErr error
		scanned int // mountpaths read without error: key found OR confirmed absent
	)
	for mp := range mpaths {
		key, e := _loadXattrNodeSigningKey(mp, tid)
		if e != nil {
			if _, ok := e.(*ErrStorageIntegrity); ok {
				return nil, e // corruption or node-ID mismatch => fatal
			}
			lastErr = e
			continue
		}
		scanned++
		if key == nil {
			continue
		}
		if pair != nil {
			if !pair.Equal(key) {
				return nil, &ErrStorageIntegrity{
					Code: SieMpathSigningKeyMismatch,
					Msg:  fmt.Sprintf("target signing key mismatch: %q(%q)", tid, mp),
				}
			}
			continue
		}
		pair = key
	}

	// ok
	if pair != nil {
		return pair, nil
	}

	// differentiate: error(s) vs brand-new
	if lastErr != nil {
		err := fmt.Errorf("cannot determine node signing key: %d mountpath%s scanned, at least one unreadable: %w",
			scanned, cos.Plural(scanned), lastErr)
		return nil, err
	}

	return nil, nil
}

func _loadXattrNodeSigningKey(mpath, tid string) (pair *cos.NodeSigningKey, err error) {
	b, err := GetXattr(mpath, xattrNodeSigningKey)
	if err == nil {
		pair, err = cos.UnpackNodeSigningKey(tid, b)
		if err != nil {
			return nil, &ErrStorageIntegrity{
				Code: SieMetaCorrupted,
				Msg:  fmt.Sprintf("invalid target signing key at %q: %v", mpath, err),
			}
		}
		return pair, nil
	}
	if cos.IsErrXattrNotFound(err) {
		err = nil
	} else {
		err = fmt.Errorf("unexpected failure to access mountpath %q: %w", mpath, err)
	}
	return nil, err
}
