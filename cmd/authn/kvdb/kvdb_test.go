// Package kvdb_test contains authN server code for interacting with a persistent key-value store
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb_test

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/cmd/authn/kvdb"
	cmnkvdb "github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/lestrrat-go/jwx/v2/jwk"
)

func newTestDriver(t *testing.T) *kvdb.Driver {
	path := filepath.Join(t.TempDir(), "authn.db")
	inner, err := cmnkvdb.NewBuntDB(path)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = inner.Close() })
	return &kvdb.Driver{Driver: inner}
}

func createKeyDataWithJWKS(t *testing.T) *kvdb.KeyData {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	tassert.CheckFatal(t, err)
	jwkKey, err := jwk.FromRaw(key)
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, jwk.AssignKeyID(jwkKey) == nil, "AssignKeyID")
	pubKey, err := jwkKey.PublicKey()
	tassert.CheckFatal(t, err)
	set := jwk.NewSet()
	tassert.Fatal(t, set.AddKey(pubKey) == nil, "AddKey")
	return &kvdb.KeyData{
		MetaVersion: kvdb.KeyMetadataVersion,
		JWKS:        set,
	}
}

func TestDriver_PersistAndLoadKeyData(t *testing.T) {
	d := newTestDriver(t)
	meta := createKeyDataWithJWKS(t)

	err := d.PersistKeyData(meta)
	tassert.CheckFatal(t, err)

	loaded, err := d.LoadKeyData()
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, loaded != nil, "loaded is nil")
	tassert.Errorf(t, loaded.MetaVersion == meta.MetaVersion,
		"MetaVersion: got %d, want %d", loaded.MetaVersion, meta.MetaVersion)
	tassert.Fatal(t, loaded.JWKS != nil, "JWKS is nil")
	tassert.Errorf(t, loaded.JWKS.Len() == meta.JWKS.Len(),
		"JWKS.Len(): got %d, want %d", loaded.JWKS.Len(), meta.JWKS.Len())
}

func TestDriver_LoadKeyData_NotFound(t *testing.T) {
	d := newTestDriver(t)

	loaded, err := d.LoadKeyData()
	tassert.Fatal(t, errors.Is(err, fs.ErrNotExist), "expected a notExist error when key data not stored")
	tassert.Fatal(t, loaded == nil, "expected nil when error")
}

func TestDriver_PersistKeyData_Overwrite(t *testing.T) {
	d := newTestDriver(t)
	meta1 := createKeyDataWithJWKS(t)
	meta2 := createKeyDataWithJWKS(t)

	err := d.PersistKeyData(meta1)
	tassert.CheckFatal(t, err)

	err = d.PersistKeyData(meta2)
	tassert.CheckFatal(t, err)

	loaded, err := d.LoadKeyData()
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, loaded != nil, "loaded is nil")
	// Should have second key set (different key IDs from different RSA keys)
	tassert.Fatal(t, loaded.JWKS.Len() == meta2.JWKS.Len(), "expected second data")
}
