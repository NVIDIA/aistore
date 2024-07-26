// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"os"
)

// getEnvOrDefault returns the value of the environment variable if it exists,
// otherwise it returns the provided default value.
func GetEnvOrDefault(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}

// IsParseBoolOrDefault parses a boolean from the environment variable string
// or returns the default value if parsing fails.
func IsParseEnvBoolOrDefault(envVar string, defaultValue bool) (bool, error) {
	if value := os.Getenv(envVar); value != "" {
		return ParseBool(value)
	}
	return defaultValue, nil
}
