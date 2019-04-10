// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
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
)

var (
	ClusterURL  = os.Getenv("AIS_URL") // The URL that points to the AIS cluster
	watch       = false
	refreshRate = "5s"

	transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).DialContext,
	}
	httpClient = &http.Client{
		Timeout:   300 * time.Second,
		Transport: transport,
	}
)

// Checks if URL is valid by trying to get Smap
func TestAISURL(clusterURL string) error {
	if ClusterURL == "" {
		return errors.New("env variable 'AIS_URL' is not set")
	}
	baseParams := cliAPIParams(clusterURL)
	_, err := api.GetClusterMap(baseParams)
	return err
}

func cliAPIParams(proxyURL string) *api.BaseParams {
	return &api.BaseParams{
		Client: httpClient,
		URL:    proxyURL,
	}
}

func retrieveStatus(url string, errCh chan error, dataCh chan *stats.DaemonStatus) {
	baseParams := cliAPIParams(url)
	obj, err := api.GetDaemonStatus(baseParams)
	if err != nil {
		errCh <- err
		return
	}
	dataCh <- obj
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

	// Call API to get proxy information
	dataCh := make(chan *stats.DaemonStatus, proxyCount)
	wg.Add(proxyCount)
	for _, dae := range smapPrimary.Pmap {
		go func(url string) {
			retrieveStatus(url, errCh, dataCh)
			wg.Done()
		}(dae.URL(cmn.NetworkPublic))
	}

	wg.Wait()
	close(dataCh)

	for datum := range dataCh {
		proxy[datum.Snode.DaemonID] = datum
	}

	dataCh = make(chan *stats.DaemonStatus, targetCount)
	wg.Add(targetCount)
	// Call API to get target information
	for _, dae := range smapPrimary.Tmap {
		go func(url string) {
			retrieveStatus(url, errCh, dataCh)
			wg.Done()
		}(dae.URL(cmn.NetworkPublic))
	}
	wg.Wait()
	close(dataCh)

	for datum := range dataCh {
		target[datum.Snode.DaemonID] = datum
	}

	close(errCh)
	if err, ok := <-errCh; ok {
		return err
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

func parseDest(rawURL string) (bucket, objName string, err error) {
	destScheme, destBucket, destObjName, err := parseURI(rawURL)
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
	destObjName = strings.Trim(destObjName, "/")
	return destBucket, destObjName, nil
}

// Formats the error message from HTTPErrors (see http.go)
func errorHandler(e error) error {
	switch err := e.(type) {
	case *cmn.HTTPError:
		return fmt.Errorf("%s (%d): %s", http.StatusText(err.Status), err.Status, err.Message)
	default:
		return err
	}
}
