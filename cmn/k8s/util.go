// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"fmt"
	"strings"
)

// POD name (K8s doesn't allow `_` and uppercase)
func CleanName(name string) string { return strings.ReplaceAll(strings.ToLower(name), "_", "-") }

const (
	shortNameETL = 6
	longNameETL  = 32
)

func ValidateEtlName(name string) error {
	const prefix = "ETL name %q "
	l := len(name)
	if l == 0 {
		return fmt.Errorf(prefix+"is empty", name)
	}
	if l < shortNameETL {
		return fmt.Errorf(prefix+"is too short", name)
	}
	if l > longNameETL {
		return fmt.Errorf(prefix+"is too long", name)
	}
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		return fmt.Errorf(prefix+"is invalid: can only contain [a-z0-9-]", name)
	}
	return nil
}
