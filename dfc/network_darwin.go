// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

// netifSpeed takes interface name and returns the interface's bandwidth (Mbps)
//FIXME TODO: how to detect network interface speed in OSX
func netifSpeed(netifName string) int {
	return 0
}
