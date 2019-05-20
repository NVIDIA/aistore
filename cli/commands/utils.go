// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

const (
	defaultScheme = "https"
	gsScheme      = "gs"
	s3Scheme      = "s3"
	aisScheme     = "ais"

	gsHost = "storage.googleapis.com"
	s3Host = "s3.amazonaws.com"

	AutoCompDir = "/etc/bash_completion.d/ais"

	Infinity = -1
)

var (
	ClusterURL = os.Getenv("AIS_URL")

	// Global variables set by daemon command handler
	refreshRate = "1s"
	count       = countDefault

	transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).DialContext,
	}
	httpClient = &http.Client{
		Timeout:   300 * time.Second,
		Transport: transport,
	}

	mu sync.Mutex
)

// Checks if URL is valid by trying to get Smap
func TestAISURL() (err error) {
	baseParams := cliAPIParams(ClusterURL)
	_, err = api.GetClusterMap(baseParams)

	if cmn.IsErrConnectionRefused(err) {
		return fmt.Errorf("could not connect to AIS cluser at %s - check if the cluster is running", ClusterURL)
	}

	return err
}

func SetClusterURL(url string) {
	if ClusterURL == "" {
		ClusterURL = url
	}
}

func cliAPIParams(proxyURL string) *api.BaseParams {
	return &api.BaseParams{
		Client: httpClient,
		URL:    proxyURL,
	}
}

func fill(dae *cluster.Snode, daeMap map[string]*stats.DaemonStatus) error {
	baseParams := cliAPIParams(dae.URL(cmn.NetworkPublic))
	obj, err := api.GetDaemonStatus(baseParams)
	if err != nil {
		return err
	}
	mu.Lock()
	daeMap[dae.DaemonID] = obj
	mu.Unlock()
	return nil
}

func retrieveStatus(nodeMap cluster.NodeMap, daeMap map[string]*stats.DaemonStatus, wg *sync.WaitGroup, errCh chan error) {
	for _, dae := range nodeMap {
		go func(d *cluster.Snode) {
			errCh <- fill(d, daeMap)
			wg.Done()
		}(dae)
	}
}

// Populates the proxy and target maps
func fillMap(url string) error {
	var (
		baseParams = cliAPIParams(url)
		wg         = &sync.WaitGroup{}
	)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	// Get the primary proxy's smap
	primaryURL := smap.ProxySI.PublicNet.DirectURL
	baseParams.URL = primaryURL
	smapPrimary, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}

	proxyCount := len(smapPrimary.Pmap)
	targetCount := len(smapPrimary.Tmap)
	errCh := make(chan error, proxyCount+targetCount)

	wg.Add(proxyCount + targetCount)
	retrieveStatus(smapPrimary.Pmap, proxy, wg, errCh)
	retrieveStatus(smapPrimary.Tmap, target, wg, errCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// Checks if daemon exists and returns it's directURL
func daemonDirectURL(daemonID string) (string, error) {
	// Default: use AIS_URL value
	if daemonID == "" {
		return ClusterURL, nil
	}
	if res, ok := proxy[daemonID]; ok {
		return res.Snode.URL(cmn.NetworkPublic), nil
	}
	if res, ok := target[daemonID]; ok {
		return res.Snode.URL(cmn.NetworkPublic), nil
	}
	return "", fmt.Errorf(invalidDaemonMsg, daemonID)
}

func regexFilter(regex string, strList []string) (retList []string) {
	if regex == "" {
		return strList
	}

	retList = strList[:0]
	r, _ := regexp.Compile(regex)
	for _, item := range strList {
		if r.MatchString(item) {
			retList = append(retList, item)
		}
	}
	return retList
}

// If the flag has multiple values (separated by comma), take the first one
func cleanFlag(flag string) string {
	return strings.Split(flag, ",")[0]
}

// Users can pass in a delimiter separated list
func makeList(list, delimiter string) []string {
	cleanList := strings.Split(list, delimiter)
	for ii, val := range cleanList {
		cleanList[ii] = strings.TrimSpace(val)
	}
	return cleanList
}

// Converts key=value to map
func makeKVS(args []string, delimiter string) (nvs cmn.SimpleKVs, err error) {
	nvs = cmn.SimpleKVs{}
	for _, ele := range args {
		pairs := makeList(ele, delimiter)
		if len(pairs) != 2 {
			return nil, fmt.Errorf("Could not parse key value: %v", pairs)
		}
		nvs[pairs[0]] = pairs[1]
	}
	return
}

func parseURI(rawURL string) (scheme, bucket, objName string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	scheme = u.Scheme
	bucket = u.Host
	objName = u.Path
	return
}

// Replace protocol (gs://, s3://) with proper google cloud / s3 URL
func parseSource(rawURL string) (link string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	scheme := u.Scheme
	host := u.Host
	fullPath := u.Path

	// if rawURL is using gs or s3 scheme ({gs/s3}://<bucket>/...) then <bucket> is considered a host by `url.Parse`
	switch u.Scheme {
	case gsScheme:
		scheme = "https"
		host = gsHost
		fullPath = path.Join(u.Host, fullPath)
	case s3Scheme:
		scheme = "http"
		host = s3Host
		fullPath = path.Join(u.Host, fullPath)
	case "":
		scheme = defaultScheme
	case "https", "http":
		break
	case aisScheme:
		err = fmt.Errorf("%q scheme cannot be used as source", aisScheme)
		return
	default:
		err = fmt.Errorf("invalid scheme: %s", scheme)
		return
	}

	normalizedURL := url.URL{
		Scheme:   scheme,
		User:     u.User,
		Host:     host,
		Path:     fullPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	return url.QueryUnescape(normalizedURL.String())
}

func parseDest(rawURL string) (bucket, pathSuffix string, err error) {
	destScheme, destBucket, destPathSuffix, err := parseURI(rawURL)
	if err != nil {
		return
	}
	if destScheme != aisScheme {
		err = fmt.Errorf("destination should be %q scheme, eg. %s://bucket/objname", aisScheme, aisScheme)
		return
	}
	if destBucket == "" {
		err = fmt.Errorf("destination bucket cannot be empty")
		return
	}
	destPathSuffix = strings.Trim(destPathSuffix, "/")
	return destBucket, destPathSuffix, nil
}

func canReachBucket(baseParams *api.BaseParams, bckName, bckProvider string) error {
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	if _, err := api.HeadBucket(baseParams, bckName, query); err != nil {
		return fmt.Errorf("could not reach %q bucket: %v", bckName, err)
	}
	return nil
}
