// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

// best-effort auto-detect running in container
func detect() bool { return false }
