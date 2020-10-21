// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// Cloud Provider enum
const (
	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
	ProviderAzure  = "azure"
	ProviderHTTP   = "ht"
	allProviders   = "aws, gcp, ais, azure, ht"

	NsUUIDPrefix = '@' // BEWARE: used by on-disk layout
	NsNamePrefix = '#' // BEWARE: used by on-disk layout

	BckProviderSeparator = "://"

	// Scheme parsing
	DefaultScheme = "https"
	GSScheme      = "gs"
	S3Scheme      = "s3"
	AZScheme      = "az"
	AISScheme     = "ais"
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
		UUID string `json:"uuid"`
		// Name uniquely identifies a namespace under the same UUID (which may
		// be empty) and is used in building FQN for the objects.
		Name string `json:"name"`
	}

	Bck struct {
		Name     string       `json:"name"`
		Provider string       `json:"provider"`
		Ns       Ns           `json:"namespace" list:"omitempty"`
		Props    *BucketProps `json:"-"`
	}

	// Represents the AIS bucket, object and URL associated with a HTTP resource
	HTTPBckObj struct {
		Bck        Bck
		ObjName    string
		OrigURLBck string // HTTP URL of the bucket (object name excluded)
	}

	QueryBcks Bck

	BucketNames []Bck

	// implemented by cluster.Bck
	NLP interface {
		Lock()
		TryLock() bool
		TryRLock() bool
		Unlock()
	}
)

var (
	// NsGlobal represents *this* cluster's global namespace that is used by default when
	// no specific namespace was defined or provided by the user.
	NsGlobal = Ns{}
	// NsAnyRemote represents any remote cluster. As such, NsGlobalRemote applies
	// exclusively to AIS (provider) given that other Cloud providers are remote by definition.
	NsAnyRemote = Ns{UUID: string(NsUUIDPrefix)}

	Providers = NewStringSet(
		ProviderAIS,
		ProviderGoogle,
		ProviderAmazon,
		ProviderAzure,
		ProviderHTTP,
	)
)

// Parses [@uuid][#namespace]. It does a little bit more than just parsing
// a string from `Uname` so that logic can be reused in different places.
func ParseNsUname(s string) (n Ns) {
	if len(s) > 0 && s[0] == NsUUIDPrefix {
		s = s[1:]
	}
	idx := strings.IndexByte(s, NsNamePrefix)
	if idx == -1 {
		n.UUID = s
	} else {
		n.UUID = s[:idx]
		n.Name = s[idx+1:]
	}
	return
}

// Replace provider aliases with real provider names
func NormalizeProvider(provider string) (string, error) {
	switch provider {
	case GSScheme:
		return ProviderGoogle, nil
	case S3Scheme:
		return ProviderAmazon, nil
	case AZScheme:
		return ProviderAzure, nil
	}

	if provider != "" && !IsValidProvider(provider) {
		return "", fmt.Errorf("invalid bucket provider %q", provider)
	}
	return provider, nil
}

// Parses "provider://@uuid#namespace/bucketName/objectName"
func ParseBckObjectURI(objName string, query ...bool) (bck Bck, object string, err error) {
	const (
		bucketSepa = "/"
	)

	parts := strings.SplitN(objName, BckProviderSeparator, 2)

	if len(parts) > 1 {
		bck.Provider, err = NormalizeProvider(parts[0])
		objName = parts[1]
	}

	if err != nil {
		return
	}

	parts = strings.SplitN(objName, bucketSepa, 2)
	if len(parts[0]) > 0 && (parts[0][0] == NsUUIDPrefix || parts[0][0] == NsNamePrefix) {
		bck.Ns = ParseNsUname(parts[0])
		if err := bck.Ns.Validate(); err != nil {
			return bck, "", err
		}
		if len(parts) == 1 {
			isQuery := len(query) > 0 && query[0]
			if parts[0] == string(NsUUIDPrefix) && isQuery {
				// Case: "[provider://]@" (only valid if uri is query)
				// We need to list buckets from all possible remote clusters
				bck.Ns = NsAnyRemote
				return bck, "", nil
			}

			// Case: "[provider://]@uuid#ns"
			return bck, "", nil
		}

		// Case: "[provider://]@uuid#ns/bucket"
		parts = strings.SplitN(parts[1], bucketSepa, 2)
	}

	bck.Name = parts[0]
	if bck.Name != "" {
		if err := ValidateBckName(bck.Name); err != nil {
			return bck, "", err
		}
	}
	if len(parts) > 1 {
		object = parts[1]
	}

	return
}

////////
// Ns //
////////

func (n Ns) String() string {
	if n.IsGlobal() {
		return ""
	}
	res := ""
	if n.UUID != "" {
		res += string(NsUUIDPrefix) + n.UUID
	}
	if n.Name != "" {
		res += string(NsNamePrefix) + n.Name
	}
	return res
}

func (n Ns) Uname() string {
	b := make([]byte, 0, 2+len(n.UUID)+len(n.Name))
	b = append(b, NsUUIDPrefix)
	b = append(b, n.UUID...)
	b = append(b, NsNamePrefix)
	b = append(b, n.Name...)
	return string(b)
}

func (n Ns) Validate() error {
	if !nsReg.MatchString(n.UUID) || !nsReg.MatchString(n.Name) {
		return fmt.Errorf(
			"namespace (uuid: %q, name: %q) may only contain letters, numbers, dashes (-), underscores (_)",
			n.UUID, n.Name,
		)
	}
	return nil
}

func (n Ns) Contains(other Ns) bool {
	if n.IsGlobal() {
		return true // If query is empty (global) we accept any namespace
	}
	if n.IsAnyRemote() {
		return other.IsRemote()
	}
	return n == other
}

/////////
// Bck //
/////////

func (b Bck) Less(other Bck) bool {
	if QueryBcks(b).Contains(other) {
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

func (b Bck) Equal(other Bck) bool {
	return b.Name == other.Name && b.Provider == other.Provider && b.Ns == other.Ns
}

func ValidateBckName(bucket string) (err error) {
	const nameHelp = "may only contain letters, numbers, dashes (-), underscores (_), and dots (.)"
	if bucket == "" || bucket == "." || !bucketReg.MatchString(bucket) {
		return fmt.Errorf("bucket name %q is invalid: %v", bucket, nameHelp)
	}
	if strings.Contains(bucket, "..") {
		return fmt.Errorf("bucket name %q cannot contain '..'", bucket)
	}
	return
}

func (b Bck) Valid() bool {
	return ValidateBckName(b.Name) == nil && IsValidProvider(b.Provider) && b.Ns.Validate() == nil
}

func (b Bck) String() string {
	if b.Ns.IsGlobal() {
		if b.Provider == "" {
			return b.Name
		}
		return fmt.Sprintf("%s%s%s", b.Provider, BckProviderSeparator, b.Name)
	}
	if b.Provider == "" {
		return fmt.Sprintf("%s/%s", b.Ns, b.Name)
	}
	return fmt.Sprintf("%s%s%s/%s", b.Provider, BckProviderSeparator, b.Ns, b.Name)
}

func (b Bck) IsEmpty() bool { return b.Name == "" && b.Provider == "" && b.Ns == NsGlobal }

//
// Is-Whats
//

func (n Ns) IsGlobal() bool    { return n == NsGlobal }
func (n Ns) IsAnyRemote() bool { return n == NsAnyRemote }
func (n Ns) IsRemote() bool    { return n.UUID != "" }

func (b *Bck) HasBackendBck() bool {
	return b.Provider == ProviderAIS && b.Props != nil && !b.Props.BackendBck.IsEmpty()
}

func (b *Bck) BackendBck() *Bck {
	if b.HasBackendBck() {
		return &b.Props.BackendBck
	}
	return nil
}

func (b *Bck) RemoteBck() *Bck {
	if !b.IsRemote() {
		return nil
	}
	if b.HasBackendBck() {
		return &b.Props.BackendBck
	}
	return b
}

func (b Bck) IsAIS() bool       { return b.Provider == ProviderAIS && !b.Ns.IsRemote() && !b.HasBackendBck() } // is local AIS cluster
func (b Bck) IsRemoteAIS() bool { return b.Provider == ProviderAIS && b.Ns.IsRemote() }                        // is remote AIS cluster
func (b Bck) IsHTTP() bool      { return b.Provider == ProviderHTTP }                                          // is HTTP

func (b Bck) IsRemote() bool {
	return b.IsCloud() || b.IsRemoteAIS() || b.IsHTTP() || b.HasBackendBck()
}

func (b Bck) IsCloud() bool { // is 3rd party Cloud
	if b.Provider == ProviderHTTP {
		return false
	}
	if bck := b.BackendBck(); bck != nil {
		debug.Assert(bck.IsCloud()) // currently, backend is always Cloud
		return bck.IsCloud()
	}
	return b.Provider != ProviderAIS && IsValidProvider(b.Provider)
}

func (b Bck) HasProvider() bool { return IsValidProvider(b.Provider) }

func IsValidProvider(provider string) bool {
	_, ok := Providers[provider]
	return ok
}

func (query QueryBcks) String() string     { return Bck(query).String() }
func (query QueryBcks) IsAIS() bool        { return Bck(query).IsAIS() }
func (query QueryBcks) IsRemoteAIS() bool  { return Bck(query).IsRemoteAIS() }
func (query QueryBcks) Equal(bck Bck) bool { return Bck(query).Equal(bck) }
func (query QueryBcks) Contains(other Bck) bool {
	if query.Name != "" {
		// NOTE: named bucket with no provider is assumed to be ais://
		if other.Provider == "" {
			other.Provider = ProviderAIS
		}
		if query.Provider == "" {
			// If query's provider not set, we should match the expected bucket
			query.Provider = other.Provider
		}
		return query.Equal(other)
	}
	ok := query.Provider == other.Provider || query.Provider == ""
	return ok && query.Ns.Contains(other.Ns)
}

func AddBckToQuery(query url.Values, bck Bck) url.Values {
	if bck.Provider != "" {
		if query == nil {
			query = make(url.Values)
		}
		query.Set(URLParamProvider, bck.Provider)
	}
	if !bck.Ns.IsGlobal() {
		if query == nil {
			query = make(url.Values)
		}
		query.Set(URLParamNamespace, bck.Ns.Uname())
	}
	return query
}

func DelBckFromQuery(query url.Values) url.Values {
	query.Del(URLParamProvider)
	query.Del(URLParamNamespace)
	return query
}

/////////////////
// BucketNames //
/////////////////

func (names BucketNames) Len() int {
	return len(names)
}

func (names BucketNames) Less(i, j int) bool {
	return names[i].Less(names[j])
}

func (names BucketNames) Swap(i, j int) {
	names[i], names[j] = names[j], names[i]
}

func (names BucketNames) Select(query QueryBcks) (filtered BucketNames) {
	for _, bck := range names {
		if query.Contains(bck) {
			filtered = append(filtered, bck)
		}
	}
	return filtered
}

func (names BucketNames) Contains(query QueryBcks) bool {
	for _, bck := range names {
		if query.Equal(bck) || query.Contains(bck) {
			return true
		}
	}
	return false
}

func (names BucketNames) Equal(other BucketNames) bool {
	if len(names) != len(other) {
		return false
	}
	for _, b1 := range names {
		var found bool
		for _, b2 := range other {
			if b1.Equal(b2) {
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
			Provider: ProviderHTTP,
			Ns:       NsGlobal,
		},
	}
	hbo.OrigURLBck, hbo.ObjName = filepath.Split(u.Path)
	hbo.OrigURLBck = u.Scheme + "://" + u.Host + hbo.OrigURLBck
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
