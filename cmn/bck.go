// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	onexxh "github.com/OneOfOne/xxhash"
)

type (
	// Ns (or Namespace) adds additional layer for scoping the data under
	// the same provider. It allows to have same dataset and bucket names
	// under different namespaces what allows for easy data manipulation without
	// affecting data in different namespaces.
	Ns struct {
		// UUID of other remote AIS cluster (for now only used for AIS). Note
		// that we can have different namespaces which refer to same UUID (cluster).
		// This means that in a sense UUID is a parent of the actual namespace.
		UUID string `json:"uuid" yaml:"uuid"`
		// Name uniquely identifies a namespace under the same UUID (which may
		// be empty) and is used in building FQN for the objects.
		Name string `json:"name" yaml:"name"`
	}

	Bck struct {
		Props    *Bprops `json:"-"`
		Name     string  `json:"name" yaml:"name"`
		Provider string  `json:"provider" yaml:"provider"` // NOTE: see api/apc/provider.go for supported enum
		Ns       Ns      `json:"namespace" yaml:"namespace" list:"omitempty"`
	}

	// Represents the AIS bucket, object and URL associated with a HTTP resource
	HTTPBckObj struct {
		Bck        Bck
		ObjName    string
		OrigURLBck string // HTTP URL of the bucket (object name excluded)
	}

	QueryBcks Bck

	Bcks []Bck
)

const (
	// NsGlobalUname is hardcoded here to avoid allocating it via Uname()
	// (the most common use case)
	NsGlobalUname = "@#"
)

var (
	// NsGlobal represents *this* cluster's global namespace that is used by default when
	// no specific namespace was defined or provided by the user.
	NsGlobal = Ns{}
	// NsAnyRemote represents any remote cluster. As such, NsGlobalRemote applies
	// exclusively to AIS (provider) given that other Backend providers are remote by definition.
	NsAnyRemote = Ns{UUID: string(apc.NsUUIDPrefix)}
)

// A note on validation logic: cmn.Bck vs cmn.QueryBcks - same structures,
// different types.
//
//  1. Validation of a concrete bucket checks that bucket name is set and is valid.
//     If the provider is not set the default will be used, see `NormalizeProvider`.
//     This case is handled in `newBckFromQuery` and `newBckFromQueryUname`. The
//     CLI counterpart is `parseBckURI`.
//  2. Validation of query buckets. Here all parts of the structure all optional.
//     This case is handled in `newQueryBcksFromQuery`. The CLI counterpart is
//     `parseQueryBckURI`.
// These 2 cases have a slightly different logic for the validation but the
// validation functions are always the same. Bucket name (`bck.ValidateName`)
// and bucket namespace (`bck.Ns.Validate`) validation is straightforward
// as we only need to check that the strings contain only valid characters. Bucket
// provider validation on the other hand a little bit more tricky as we have so
// called "normalized providers" and their aliases. Normalized providers are the
// providers registered in `Providers` set. Almost any provider that is being
// validated goes through `NormalizeProvider` which converts aliases to
// normalized form or sets default provider if the provider is empty. But there
// are cases where we already expect **only** the normalized providers, for
// example in FQN parsing. For this case `IsProvider` function must be
// used.
//
// Similar concepts are applied when bucket is provided as URI,
// eg. `ais://@uuid#ns/bucket_name`. URI form is heavily used by CLI. Parsing
// is handled by `ParseBckObjectURI` which by itself doesn't do much validation.
// The validation happens in aforementioned CLI specific parse functions.

func NormalizeProvider(provider string) (p string, err error) {
	if p = apc.NormalizeProvider(provider); p == "" {
		err = &ErrInvalidBackendProvider{Bck{Provider: provider}}
	}
	return p, err
}

////////
// Ns //
////////

// Parses [@uuid][#namespace]. It does a little bit more than just parsing
// a string from `Uname` so that logic can be reused in different places.
func ParseNsUname(s string) (n Ns) {
	if s == NsGlobalUname {
		return NsGlobal // to speedup the common case (here and elsewhere)
	}
	if s != "" && s[0] == apc.NsUUIDPrefix {
		s = s[1:]
	}
	idx := strings.IndexByte(s, apc.NsNamePrefix)
	if idx == -1 {
		n.UUID = s
	} else {
		n.UUID = s[:idx]
		n.Name = s[idx+1:]
	}
	return n
}

func (n Ns) String() string {
	if n.IsGlobal() {
		return ""
	}

	var sb strings.Builder
	sb.Grow(n.Len() + 1)
	n._str(&sb)

	return sb.String()
}

func (n Ns) _str(sb *strings.Builder) {
	if n.IsGlobal() {
		return
	}
	if n.IsAnyRemote() {
		sb.WriteByte(apc.NsUUIDPrefix)
		return
	}
	if n.UUID != "" {
		sb.WriteByte(apc.NsUUIDPrefix)
		sb.WriteString(n.UUID)
	}
	if n.Name != "" {
		sb.WriteByte(apc.NsNamePrefix)
		sb.WriteString(n.Name)
	}
}

func (n Ns) Len() int {
	if n.IsGlobal() {
		return len(NsGlobalUname)
	}
	return 2 + len(n.UUID) + len(n.Name)
}

func (n Ns) Uname() string {
	if n.IsGlobal() {
		return NsGlobalUname
	}
	l := n.Len()
	b := make([]byte, l)
	n._copy(b, l)
	return cos.UnsafeS(b)
}

func (n Ns) _copy(b []byte, l int) int {
	b[0] = apc.NsUUIDPrefix
	off := 1
	off += copy(b[off:], cos.UnsafeB(n.UUID))
	b[off] = apc.NsNamePrefix
	off++
	off += copy(b[off:], cos.UnsafeB(n.Name))
	debug.Assert(off == l)
	return off
}

func (n Ns) validate() (err error) {
	if n.IsGlobal() {
		return nil
	}
	if err = cos.CheckAlphaPlus(n.Name, "namespace"); err == nil {
		if cos.IsAlphaNice(n.UUID) {
			return nil
		}
		return fmt.Errorf(fmtErrNamespace, n.UUID, n.Name)
	}
	return err
}

func (n Ns) contains(other Ns) bool {
	if n.IsGlobal() {
		// If query is empty (ie., global) we accept any non-remote namespace
		return !other.IsRemote()
	}
	if n.IsAnyRemote() {
		return other.IsRemote()
	}
	if n.UUID == other.UUID && n.Name == "" {
		return true
	}
	return n == other
}

/////////
// Bck (value)
/////////

func (b *Bck) Equal(other *Bck) bool {
	return b.Name == other.Name && b.Provider == other.Provider && b.Ns == other.Ns
}

func (b *Bck) String() string {
	var sb strings.Builder
	sb.Grow(64)
	b.Str(&sb)
	return sb.String()
}

func (b *Bck) Str(sb *strings.Builder) {
	if b.Ns.IsGlobal() {
		if b.Provider == "" {
			sb.WriteString(b.Name)
			return
		}
		sb.WriteString(apc.ToScheme(b.Provider))
		sb.WriteString(apc.BckProviderSeparator)
		sb.WriteString(b.Name)
	} else {
		sb.WriteString(apc.ToScheme(b.Provider))
		sb.WriteString(apc.BckProviderSeparator)
		b.Ns._str(sb)
		sb.WriteByte('/')
		sb.WriteString(b.Name)
	}
	if back := b.Backend(); back != nil {
		sb.WriteString("->")
		back.Str(sb)
	}
}

// unique name => Bck
// - MakeUname to make
// - compare w/ meta.ParseUname (that does more) and maybe use the latter
func ParseUname(uname string) (b Bck, objName string) {
	var prev, itemIdx int
	for i := range len(uname) {
		if uname[i] != filepath.Separator {
			continue
		}

		item := uname[prev:i]
		switch itemIdx {
		case 0:
			b.Provider = item
		case 1:
			b.Ns = ParseNsUname(item)
		case 2:
			b.Name = item
			objName = uname[i+1:]
			return
		}

		itemIdx++
		prev = i + 1
	}
	return
}

/////////
// Bck (ref)
/////////

func (b *Bck) Copy(src *Bck) { *b = *src }

func (b *Bck) Less(other *Bck) bool {
	if QueryBcks(*b).Contains(other) {
		return true
	}
	if b.Provider != other.Provider {
		return b.Provider < other.Provider
	}
	sb, so := b.Ns.String(), other.Ns.String()
	if sb != so {
		return sb < so
	}
	return b.Name < other.Name
}

func (b *Bck) Validate() (err error) {
	err = b.ValidateName()
	if err == nil {
		err = b.Ns.validate()
	}
	return err
}

func (b *Bck) ValidateName() error {
	if b.Name == "" {
		return errors.New("bucket name is missing")
	}
	if b.Name == "." {
		return fmt.Errorf(fmtErrBckName, b.Name)
	}
	return cos.CheckAlphaPlus(b.Name, "bucket name")
}

// canonical name, with or without object
func (b *Bck) Cname(objname string) (s string) {
	var sb strings.Builder
	sb.Grow(len(b.Name) + len(objname) + b.Ns.Len() + 16)

	sb.WriteString(apc.ToScheme(b.Provider))
	sb.WriteString(apc.BckProviderSeparator)

	if b.Ns.IsGlobal() {
		sb.WriteString(b.Name)
	} else {
		b.Ns._str(&sb)
		sb.WriteByte('/')
		sb.WriteString(b.Name)
	}
	if objname == "" {
		return sb.String()
	}
	sb.WriteByte(filepath.Separator)
	sb.WriteString(objname)
	return sb.String()
}

func (b *Bck) IsEmpty() bool {
	return b == nil || (b.Name == "" && b.Provider == "" && b.Ns == NsGlobal)
}

// QueryBcks (see below) is a Bck that _can_ have an empty Name.
func (b *Bck) IsQuery() bool { return b.Name == "" }

func (b *Bck) LenUnameGlob(objName string) int {
	return len(b.Provider) + 1 + len(NsGlobalUname) + 1 + len(b.Name) + 1 + len(objName) // compare with the below
}

// Bck and optional objName => unique name (uname); note:
// - use ParseUname below to translate back
// - compare with HashUname
func (b *Bck) MakeUname(objName string) []byte {
	var (
		nsUname = b.Ns.Uname()
		l       = len(b.Provider) + 1 + len(nsUname) + 1 + len(b.Name) + 1 + len(objName) // compare with the above
		buf     = make([]byte, 0, l)
	)
	return b.ubuf(buf, nsUname, objName)
}

func (b *Bck) ubuf(buf []byte, nsUname, objName string) []byte {
	buf = append(buf, b.Provider...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, nsUname...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, b.Name...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return buf
}

// alternative (one-way) uniqueness
func (b *Bck) HashUname(s string /*verb*/) uint64 {
	const sepa = "\x00"
	h := onexxh.New64()
	h.WriteString(s)
	h.WriteString(sepa)
	h.WriteString(b.Provider)
	nsName := b.Ns.Uname()
	h.WriteString(nsName)
	h.WriteString(b.Name)
	return h.Sum64()
}

//
// Is-Whats
//

func (n Ns) IsGlobal() bool    { return n == NsGlobal }
func (n Ns) IsAnyRemote() bool { return n == NsAnyRemote }
func (n Ns) IsRemote() bool    { return n.UUID != "" }

func (b *Bck) Backend() *Bck {
	bprops := b.Props
	if bprops == nil {
		return nil
	}
	if bprops.BackendBck.Name == "" {
		return nil
	}
	return &bprops.BackendBck
}

func (b *Bck) RemoteBck() *Bck {
	if bck := b.Backend(); bck != nil {
		return bck
	}
	if apc.IsRemoteProvider(b.Provider) || b.IsRemoteAIS() {
		return b
	}
	return nil
}

func (b *Bck) IsAIS() bool {
	return b.Provider == apc.AIS && !b.Ns.IsRemote() && b.Backend() == nil
}

func (b *Bck) IsRemoteAIS() bool { return b.Provider == apc.AIS && b.Ns.IsRemote() }
func (b *Bck) IsHT() bool        { return b.Provider == apc.HT }

func (b *Bck) IsRemote() bool {
	return apc.IsRemoteProvider(b.Provider) || b.IsRemoteAIS() || b.Backend() != nil
}

//
// NOTE: for more Is* accessors (e.g. IsRemoteS3), see also: core/meta/bck.go
//

func (b *Bck) IsBuiltTagged() bool {
	return b.IsCloud() || b.Provider == apc.HT
}

func (b *Bck) IsCloud() bool {
	if apc.IsCloudProvider(b.Provider) {
		return true
	}
	backend := b.Backend()
	if backend == nil {
		return false
	}
	return apc.IsCloudProvider(backend.Provider)
}

// A subset of remote backends that maintain assorted items of versioning information -
// the items including ETag, checksum, etc. - that, in turn, can be used to populate `ObjAttrs`
// * see related: `ObjAttrs.Equal`
func (b *Bck) HasVersioningMD() bool { return b.IsCloud() || b.IsRemoteAIS() }

func (b *Bck) HasProvider() bool { return b.Provider != "" }

//
// useful helpers
//

// q = make(url.Values, 1) TODO -- FIXME
func (b *Bck) SetQuery(q url.Values) {
	if b.Provider != "" {
		q.Set(apc.QparamProvider, b.Provider)
	}
	if !b.Ns.IsGlobal() {
		q.Set(apc.QparamNamespace, b.Ns.Uname())
	}
}

func (b *Bck) AddToQuery(query url.Values) url.Values {
	if b.Provider != "" {
		if query == nil {
			query = make(url.Values, 1)
		}
		query.Set(apc.QparamProvider, b.Provider)
	}
	if !b.Ns.IsGlobal() {
		if query == nil {
			query = make(url.Values)
		}
		query.Set(apc.QparamNamespace, b.Ns.Uname())
	}
	return query
}

func (b *Bck) AddUnameToQuery(query url.Values, uparam string) url.Values {
	if query == nil {
		query = make(url.Values)
	}
	uname := b.MakeUname("")
	query.Set(uparam, cos.UnsafeS(uname))
	return query
}

func DelBckFromQuery(query url.Values) url.Values {
	query.Del(apc.QparamProvider)
	query.Del(apc.QparamNamespace)
	return query
}

///////////////
// QueryBcks //
///////////////

// QueryBcks is a Bck that _can_ have an empty Name. (TODO: extend to support prefix and regex.)
func (qbck *QueryBcks) IsBucket() bool { return !(*Bck)(qbck).IsQuery() }

func (qbck QueryBcks) String() string {
	if qbck.IsEmpty() {
		return ""
	}
	if qbck.Name == "" {
		p := qbck.Provider
		if p == "" {
			p = apc.AIS // querying default = apc.NormalizeProvider("")
		}
		if qbck.Ns.IsGlobal() {
			return apc.ToScheme(p) + apc.BckProviderSeparator
		}
		var (
			sb strings.Builder
		)
		sb.Grow(qbck.Ns.Len() + 8)
		sb.WriteString(apc.ToScheme(p))
		sb.WriteString(apc.BckProviderSeparator)
		qbck.Ns._str(&sb)
		return sb.String()
	}
	b := Bck(qbck)
	return b.String()
}

func (qbck *QueryBcks) IsAIS() bool       { b := (*Bck)(qbck); return b.IsAIS() }
func (qbck *QueryBcks) IsHT() bool        { b := (*Bck)(qbck); return b.IsHT() }
func (qbck *QueryBcks) IsRemoteAIS() bool { b := (*Bck)(qbck); return b.IsRemoteAIS() }
func (qbck *QueryBcks) IsCloud() bool     { return apc.IsCloudProvider(qbck.Provider) }

func (qbck *QueryBcks) IsEmpty() bool { b := (*Bck)(qbck); return b.IsEmpty() }

func (qbck *QueryBcks) SetQuery(q url.Values) {
	bck := (*Bck)(qbck)
	bck.SetQuery(q)
}

func (qbck *QueryBcks) AddToQuery(query url.Values) {
	bck := (*Bck)(qbck)
	_ = bck.AddToQuery(query)
}

func (qbck *QueryBcks) Validate() (err error) {
	if qbck.Name != "" {
		bck := Bck(*qbck)
		if err := bck.ValidateName(); err != nil {
			return err
		}
	}
	if qbck.Provider != "" {
		qbck.Provider, err = NormalizeProvider(qbck.Provider)
		if err != nil {
			return err
		}
	}
	if qbck.Ns != NsGlobal && qbck.Ns != NsAnyRemote {
		return qbck.Ns.validate()
	}
	return nil
}

func (qbck *QueryBcks) Equal(bck *Bck) bool {
	b := (*Bck)(qbck)
	return b.Equal(bck)
}

// NOTE: a named bucket with no provider is assumed to be ais://
func (qbck QueryBcks) Contains(other *Bck) bool {
	if qbck.Name != "" {
		other.Provider = cos.Right(apc.AIS, other.Provider)
		qbck.Provider = cos.Right(other.Provider, qbck.Provider) //nolint:revive // if not set we match the expected
		return qbck.Equal(other)
	}
	ok := qbck.Provider == other.Provider || qbck.Provider == ""
	return ok && qbck.Ns.contains(other.Ns)
}

//////////
// Bcks //
//////////

// interface guard
var _ sort.Interface = (*Bcks)(nil)

func (bcks Bcks) Len() int {
	return len(bcks)
}

func (bcks Bcks) Less(i, j int) bool {
	return bcks[i].Less(&bcks[j])
}

func (bcks Bcks) Swap(i, j int) {
	bcks[i], bcks[j] = bcks[j], bcks[i]
}

func (bcks Bcks) Select(query QueryBcks) (filtered Bcks) {
	for i := range bcks {
		if query.Contains(&bcks[i]) {
			filtered = append(filtered, bcks[i])
		}
	}
	return filtered
}

func (bcks Bcks) Equal(other Bcks) bool {
	if len(bcks) != len(other) {
		return false
	}
	for i := range bcks {
		var found bool
		for j := range other {
			if bcks[i].Equal(&other[j]) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

////////////////
// HTTPBckObj //
////////////////

func NewHTTPObj(u *url.URL) *HTTPBckObj {
	hbo := &HTTPBckObj{
		Bck: Bck{
			Provider: apc.HT,
			Ns:       NsGlobal,
		},
	}
	hbo.OrigURLBck, hbo.ObjName = filepath.Split(u.Path)
	hbo.OrigURLBck = u.Scheme + apc.BckProviderSeparator + u.Host + hbo.OrigURLBck
	hbo.Bck.Name = OrigURLBck2Name(hbo.OrigURLBck)
	return hbo
}

func NewHTTPObjPath(rawURL string) (*HTTPBckObj, error) {
	urlObj, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return nil, err
	}
	return NewHTTPObj(urlObj), nil
}
