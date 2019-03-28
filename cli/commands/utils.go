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
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

var (
	ClusterURL = os.Getenv("AIS_URL") // The URL that points to the AIS cluster
	transport  = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).DialContext,
	}
	HTTPClient = &http.Client{
		Timeout:   300 * time.Second,
		Transport: transport,
	}
)

// Checks if URL is valid by trying to get Smap
func TestAISURL(clusterURL string) error {
	if ClusterURL == "" {
		return errors.New("env variable 'AIS_URL' unset")
	}
	baseParams := cliAPIParams(clusterURL)
	_, err := api.GetClusterMap(baseParams)
	return err
}

func cliAPIParams(proxyURL string) *api.BaseParams {
	return &api.BaseParams{
		Client: HTTPClient,
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
	return "", fmt.Errorf("%s is not a valid daemon ID", daemonID)
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

// Users can pass in a comma separated list
func makeList(list, delimiter string) []string {
	cleanList := strings.Split(list, delimiter)
	for ii, val := range cleanList {
		cleanList[ii] = strings.TrimSpace(val)
	}
	return cleanList
}
