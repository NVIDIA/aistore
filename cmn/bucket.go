// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strings"
)

// Cloud Provider enum
const (
	Cloud = "cloud" // used only for API

	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
	ProviderAzure  = "azure"
	allProviders   = "aws, gcp, ais, azure"

	NsUUIDPrefix = '@' // BEWARE: used by on-disk layout
	NsNamePrefix = '#' // BEWARE: used by on-disk layout

	BckProviderSeparator = "://"
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
		Name     string `json:"name"`
		Provider string `json:"provider"`
		Ns       Ns     `json:"namespace"`
	}
)

var (
	// NsGlobal represents *this* cluster's global namespace that is used by default when
	// no specific namespace was defined or provided by the user.
	NsGlobal = Ns{}
	// NsGlobalRemote represents combined remote namespaces. As such, NsGlobalRemote applies
	// exclusively to AIS (provider) given that other Cloud providers are remote by definition.
	NsGlobalRemote = Ns{UUID: string(NsUUIDPrefix)}

	Providers = map[string]struct{}{
		ProviderAIS:    {},
		ProviderGoogle: {},
		ProviderAmazon: {},
		ProviderAzure:  {},
	}
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

func (b Bck) Valid() bool {
	return ValidateBckName(b.Name) == nil && IsValidProvider(b.Provider) && b.Ns.Validate() == nil
}

func (b Bck) Less(other Bck) bool {
	if b.Provider != other.Provider {
		return b.Provider < other.Provider
	}
	if b.Ns.IsGlobal() && !other.Ns.IsGlobal() {
		return false
	}
	if !b.Ns.IsGlobal() && other.Ns.IsGlobal() {
		return true
	}
	if b.Ns.String() != other.Ns.String() {
		return b.Ns.String() < other.Ns.String()
	}
	return b.Name < other.Name
}

func (b Bck) Equal(other Bck) bool {
	return b.Name == other.Name && b.Provider == other.Provider && b.Ns == other.Ns
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

///////////////
// Is-What-s //
///////////////

func (n Ns) IsGlobal() bool       { return n == NsGlobal }
func (n Ns) IsGlobalRemote() bool { return n == NsGlobalRemote }
func (n Ns) IsRemote() bool       { return n.UUID != "" }

func (b Bck) IsAIS() bool       { return b.Provider == ProviderAIS && !b.Ns.IsRemote() } // is local AIS cluster
func (b Bck) IsRemoteAIS() bool { return b.Provider == ProviderAIS && b.Ns.IsRemote() }  // is remote AIS cluster
// is 3rd party Cloud
func (b Bck) IsCloud(acceptAnon bool) bool {
	if b.Provider == ProviderAIS {
		return false
	}
	return IsValidProvider(b.Provider) || (acceptAnon && b.Provider == Cloud)
}
func (b Bck) HasProvider() bool { return b.IsAIS() || b.IsCloud(false) }

func IsValidProvider(provider string) bool {
	_, ok := Providers[provider]
	return ok
}
