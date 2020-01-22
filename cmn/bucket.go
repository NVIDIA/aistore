// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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

	nsSeparator = '#'
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
	// NsGlobal represents an global namespace that is used by default when
	// no specific namespace was defined or provided by the user.
	NsGlobal = Ns{}

	Providers = map[string]struct{}{
		ProviderAIS:    {},
		ProviderGoogle: {},
		ProviderAmazon: {},
	}
)

func ParseNsUname(s string) (n Ns) {
	idx := strings.IndexByte(s, nsSeparator)
	if idx == -1 {
		n.Name = s
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
	if n.UUID == "" {
		return n.Name
	}
	return fmt.Sprintf("%s%c%s", n.UUID, nsSeparator, n.Name)
}

func (n Ns) Uname() string  { return n.String() }
func (n Ns) IsGlobal() bool { return n == NsGlobal }
func (n Ns) IsCloud() bool  { return n.UUID != "" }
func (n Ns) Validate() error {
	if !nsReg.MatchString(n.UUID) || !nsReg.MatchString(n.Name) {
		return fmt.Errorf(
			"namespace (name: %s, uuid: %s) may only contain letters, numbers, dashes (-), underscores (_)",
			n.Name, n.UUID,
		)
	}
	return nil
}

func (b Bck) Valid() bool {
	return ValidateBckName(b.Name) == nil && IsValidProvider(b.Provider) && b.Ns.Validate() == nil
}

func (b Bck) Equal(other Bck) bool {
	return b.Name == other.Name && b.Provider == other.Provider && b.Ns == other.Ns
}

func (b Bck) String() string {
	if b.Ns.IsGlobal() {
		if b.Provider == "" {
			return b.Name
		}
		return fmt.Sprintf("%s/%s", b.Provider, b.Name)
	}
	if b.Provider == "" {
		return fmt.Sprintf("%s/%s", b.Ns, b.Name)
	}
	return fmt.Sprintf("%s/%s/%s", b.Provider, b.Ns, b.Name)
}

func IsProviderAIS(bck Bck) bool {
	return bck.Provider == ProviderAIS && bck.Ns == NsGlobal
}

func IsProviderCloud(bck Bck, acceptAnon bool) bool {
	return (!IsProviderAIS(bck) && IsValidProvider(bck.Provider)) || (acceptAnon && bck.Provider == Cloud)
}

func IsValidProvider(provider string) bool {
	_, ok := Providers[provider]
	return ok
}

func ListProviders() string {
	keys := make([]string, 0, len(Providers))
	for k := range Providers {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}
