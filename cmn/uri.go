// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/OneOfOne/xxhash"
)

type (
	ParseURIOpts struct {
		DefaultProvider string // If set the provider will be used as provider.
		IsQuery         bool   // Determines if the URI should be parsed as query.
	}
	ErrEmptyProvider struct {
		name   string
		detail string
	}
)

func (e *ErrEmptyProvider) Error() string {
	return fmt.Sprintf("backend provider cannot be empty%s (did you mean \"ais://%s\"?)", e.detail, e.name)
}

func IsErrEmptyProvider(err error) bool {
	_, ok := err.(*ErrEmptyProvider)
	return ok
}

//
// Parse URI = [provider://][@uuid#namespace][/][bucketName[/objectName]]
//

// Splits url into [(scheme)://](address).
// It's not possible to use url.Parse as (from url.Parse() docs)
// 'Trying to parse a hostname and path without a scheme is invalid'
func ParseURLScheme(url string) (scheme, address string) {
	s := strings.SplitN(url, apc.BckProviderSeparator, 2)
	if len(s) == 1 {
		return "", s[0]
	}
	return s[0], s[1]
}

func OrigURLBck2Name(origURLBck string) (bckName string) {
	_, b := ParseURLScheme(origURLBck)
	b1 := xxhash.Checksum64S(cos.UnsafeB(b), cos.MLCG32)
	b2 := strconv.FormatUint(b1, 16)
	bckName = base64.RawURLEncoding.EncodeToString([]byte(b2))
	return
}

func ParseBckObjectURI(uri string, opts ParseURIOpts) (bck Bck, objName string, err error) {
	parts := strings.SplitN(uri, apc.BckProviderSeparator, 2)
	if len(parts) > 1 && parts[0] != "" {
		if bck.Provider, err = NormalizeProvider(parts[0]); err != nil {
			return
		}
		uri = parts[1]
	} else if !opts.IsQuery {
		bck.Provider = opts.DefaultProvider
	}

	parts = strings.SplitN(uri, "/", 2)
	if parts[0] != "" && (parts[0][0] == apc.NsUUIDPrefix || parts[0][0] == apc.NsNamePrefix) {
		bck.Ns = ParseNsUname(parts[0])
		if err := bck.Ns.validate(); err != nil {
			return bck, "", err
		}
		if !opts.IsQuery && bck.Provider == "" {
			return bck, "", &ErrEmptyProvider{uri, " when namespace is not"}
		}
		if len(parts) == 1 {
			if parts[0] == string(apc.NsUUIDPrefix) && opts.IsQuery {
				// Case: "[provider://]@" (only valid if uri is query)
				// We need to list buckets from all possible remote clusters
				bck.Ns = NsAnyRemote
				return bck, "", nil
			}

			// Case: "[provider://]@uuid#ns"
			return bck, "", nil
		}

		// Case: "[provider://]@uuid#ns/bucket"
		parts = strings.SplitN(parts[1], "/", 2)
	}

	bck.Name = parts[0]
	if bck.Name != "" {
		if err := bck.ValidateName(); err != nil {
			return bck, "", err
		}
		if bck.Provider == "" {
			return bck, "", &ErrEmptyProvider{uri, ""}
		}
	}
	if len(parts) > 1 {
		objName = parts[1]
	}
	return
}
