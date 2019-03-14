// The commands package provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
)

// The URL that points to the AIS cluster
var ClusterURL = os.Getenv("AIS_URL")

// Checks if URL is valid by trying to get Smap
func TestAISURL(clusterURL string) error {
	if ClusterURL == "" {
		return errors.New("AIS_URL variable unset.")
	}
	baseParams := tutils.BaseAPIParams(clusterURL)
	_, err := api.GetClusterMap(baseParams)
	return err
}

func retrieveStatus(url string, errCh chan error, dataCh chan *stats.DaemonStatus) {
	baseParams := tutils.BaseAPIParams(url)
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
		baseParams = tutils.BaseAPIParams(url)
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
