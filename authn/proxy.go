// Authorization server for DFC
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

type (
	// Manages the current primary proxy URL and runs autodetection in case
	// of primary proxy does not response
	proxy struct {
		URL        string    `json:"url"`
		Smap       *dfc.Smap `json:"smap"`
		configPath string
	}
)

// Gets path to last working Smap and URL from authn configuration and
//   returns a real primary proxy URL
// First, it tries to load last working Smap from configPath. If there is no
//   file then the current Smap requested from defaultURL (that comes from
//   authn configuration file
// Next step is to choose the current primary proxy
// If primary proxy change is detected then the current Smap is saved
func newProxy(configPath, defaultURL string) *proxy {
	p := &proxy{}
	err := dfc.LocalLoad(configPath, p)
	if err != nil {
		// first run: read the current Smap and save to local file
		smap, err := client.GetClusterMap(defaultURL)
		if err != nil {
			glog.Errorf("Failed to get cluster map: %v", err)
			return &proxy{configPath: configPath, URL: defaultURL}
		}
		p.Smap = &smap
		p.saveSmap()
	}

	err = p.detectPrimary()
	if err != nil {
		glog.Errorf("Failed to detect primary proxy: %v", err)
		return &proxy{configPath: configPath, URL: defaultURL}
	}
	p.configPath = configPath

	return p
}

func (p *proxy) saveSmap() {
	err := dfc.LocalSave(p.configPath, p)
	if err != nil {
		glog.Errorf("Failed to save configuration: %v", err)
	}
}

// Requests Smap from a remote proxy or target
// If the node has responded then the function compares the current primary
//   URL with the URL in Smap. In case of they differ, Authn updates its
//   config and saves new valid Smap
// Returns error if the node failed to respond
func (p *proxy) comparePrimaryURL(url string) error {
	smap, err := client.GetClusterMap(url)
	if err != nil {
		return err
	}

	if smap.ProxySI.DirectURL != p.URL {
		p.URL = smap.ProxySI.DirectURL
		p.Smap = &smap
		p.saveSmap()
	}

	return nil
}

// Uses the last known Smap to detect the real primary proxy URL if the current
//   primary proxy does not respond
// It traverses all proxies and targets until the first of of them responses with
//   new Smap that contains primary URL
func (p *proxy) detectPrimary() error {
	if p.Smap == nil || len(p.Smap.Pmap)+len(p.Smap.Tmap) == 0 {
		return fmt.Errorf("Cluster map is empty")
	}

	for _, pinfo := range p.Smap.Pmap {
		err := p.comparePrimaryURL(pinfo.DirectURL)
		if err == nil {
			return nil
		}
		glog.Errorf("Failed to get cluster map from [%s]: %v", pinfo.DirectURL, err)
	}

	for _, tinfo := range p.Smap.Tmap {
		err := p.comparePrimaryURL(tinfo.DirectURL)
		if err == nil {
			return nil
		}
		glog.Errorf("Failed to get cluster map from [%s]: %v", tinfo.DirectURL, err)
	}

	return fmt.Errorf("No node has responded. Using primary URL from the config")
}
