/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"fmt"
	"testing"
)

func TestValidate(t *testing.T) {
	{
		rs := RequestSpec{
			Bucket:               "test",
			Start:                1,
			End:                  100,
			DigitsToPrependTo:    5,
			Extension:            ".tar",
			OutputShardSizeBytes: 100000,
			MaxMemUsagePercent:   40,
		}
		if err := rs.Validate(); err != nil {
			fmt.Errorf("expected nil error, got err: %v", err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".tgz",
			OutputShardSizeBytes: 100000,
		}
		if err := rs.Validate(); err != nil {
			fmt.Errorf("expected nil error, got err: %v", err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".tar.gz",
			OutputShardSizeBytes: 100000,
		}
		if err := rs.Validate(); err != nil {
			fmt.Errorf("expected nil error, got err: %v", err)
		}
	}
	{
		rs := RequestSpec{
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
		}
		if err := rs.Validate(); err != errMissingBucket {
			fmt.Errorf("expected err: %v, got err: %v", errMissingBucket, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
			Start:                10,
			End:                  0,
		}
		if err := rs.Validate(); err != errStartAfterEnd {
			fmt.Errorf("expected err: %v, got err: %v", errStartAfterEnd, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
			DigitsToPrependTo:    -12,
		}
		if err := rs.Validate(); err != errNegDigitsToPrependTo {
			fmt.Errorf("expected err: %v, got err: %v", errNegDigitsToPrependTo, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
		}
		if err := rs.Validate(); err != errInvalidExtension {
			fmt.Errorf("expected err: %v, got err: %v", errInvalidExtension, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
			MaxMemUsagePercent:   -2,
		}
		if err := rs.Validate(); err != errInvalidMaxMemPercent {
			fmt.Errorf("expected err: %v, got err: %v", errInvalidMaxMemPercent, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
			MaxMemUsagePercent:   150,
		}
		if err := rs.Validate(); err != errInvalidMaxMemPercent {
			fmt.Errorf("expected err: %v, got err: %v", errInvalidMaxMemPercent, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
			ExtractConcLimit:     -1,
		}
		if err := rs.Validate(); err != errNegativeConcurrencyLimit {
			fmt.Errorf("expected err: %v, got err: %v", errNegativeConcurrencyLimit, err)
		}
	}
	{
		rs := RequestSpec{
			Bucket:               "test",
			Extension:            ".txt",
			OutputShardSizeBytes: 100000,
			MaxMemUsagePercent:   -1,
		}
		if err := rs.Validate(); err != errNegativeConcurrencyLimit {
			fmt.Errorf("expected err: %v, got err: %v", errNegativeConcurrencyLimit, err)
		}
	}
}
