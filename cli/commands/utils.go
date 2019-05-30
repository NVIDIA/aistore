// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"bufio"
	"bytes"
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

	"github.com/urfave/cli"

	"github.com/NVIDIA/aistore/containers"

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

	gsHost               = "storage.googleapis.com"
	s3Host               = "s3.amazonaws.com"
	defaultAISHost       = "http://127.0.0.1:8080"
	defaultAISDockerHost = "http://172.50.0.2:8080"

	Infinity = -1

	envVarURL       = "AIS_URL"
	envVarNamespace = "AIS_NAMESPACE"

	keyAndValueSeparator = "="
)

var (
	ClusterURL string

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

// cluster URL resolving order
// 1. AIS_URL; if not present:
// 2. If kubernetes detected, tries to find a primary proxy in k8s cluster
// 3. Proxy docker containter IP address; if not successful:
// 4. Docker default; if not present:
// 5. Default = localhost:8080
func GetClusterURL() string {
	if envURL := os.Getenv(envVarURL); envURL != "" {
		return envURL
	}

	namespace := os.Getenv(envVarNamespace)
	k8sURL, err := containers.K8sPrimaryURL(namespace)
	if err == nil && k8sURL != "" {
		return k8sURL
	} else if containers.DockerRunning() {
		clustersIDs, err := containers.ClusterIDs()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Couldn't automatically discover docker proxy URL (%s), using the default: %q. To change: export AIS_URL=`url`\n", err.Error(), defaultAISDockerHost)
			return defaultAISDockerHost
		}

		cmn.AssertMsg(len(clustersIDs) > 0, "there should be at least one cluster running, when docker running detected")
		proxyGateway, err := containers.ClusterProxyURL(clustersIDs[0])

		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Couldn't automatically discover docker proxy URL (%s), using the default: %q. To change: export AIS_URL=`url`\n", err.Error(), defaultAISDockerHost)
			return defaultAISDockerHost
		}

		if len(clustersIDs) > 1 {
			_, _ = fmt.Fprintf(os.Stderr, "Multiple docker clusters running. Connected to %d via %s. To change: export AIS_URL=`url`\n", clustersIDs[0], proxyGateway)
		}

		return "http://" + proxyGateway + ":8080"
	}

	return defaultAISHost
}

// Checks if URL is valid by trying to get Smap
func TestAISURL() (err error) {
	baseParams := cliAPIParams(ClusterURL)
	_, err = api.GetClusterMap(baseParams)

	if cmn.IsErrConnectionRefused(err) {
		return fmt.Errorf("could not connect to AIS cluser at %s - check if the cluster is running", ClusterURL)
	}

	return err
}

func bucketFromArgsOrEnv(c *cli.Context) (string, error) {
	bucket := c.Args().First()
	if bucket == "" {
		var ok bool
		if bucket, ok = os.LookupEnv(aisBucketEnvVar); !ok {
			return "", missingArgumentsError(c, "bucket name")
		}
	}

	return bucket, nil
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

// Converts a list of "key value" and "key=value" into map
func makePairs(args []string) (nvs cmn.SimpleKVs, err error) {
	nvs = cmn.SimpleKVs{}
	i := 0
	for i < len(args) {
		pairs := makeList(args[i], keyAndValueSeparator)
		if len(pairs) != 1 {
			// "key=value" case
			nvs[pairs[0]] = pairs[1]
			i++
		} else {
			// "key value" case with a corner case: last name without a value
			if i == len(args)-1 {
				return nil, fmt.Errorf("no value for %v", args[i])
			}
			nvs[args[i]] = args[i+1]
			i += 2
		}
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

func helpMessage(template string, data interface{}) string {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	// Execute the template that generates command usage text
	cli.HelpPrinter(w, template, data)
	_ = w.Flush()

	return buf.String()
}

type usageError struct {
	context *cli.Context
	message string

	helpData     interface{}
	helpTemplate string
}

func (e *usageError) Error() string {
	msg := helpMessage(e.helpTemplate, e.helpData)
	return fmt.Sprintf("Incorrect usage of %q: %s.\n\n%s", e.context.App.Name, e.message, msg)
}

func incorrectUsageError(c *cli.Context, err error) error {
	cmn.Assert(err != nil)
	return &usageError{
		context:      c,
		message:      err.Error(),
		helpData:     c.Command,
		helpTemplate: cli.CommandHelpTemplate,
	}
}

func missingArgumentsError(c *cli.Context, missingArgs ...string) error {
	cmn.Assert(len(missingArgs) > 0)
	return &usageError{
		context:      c,
		message:      fmt.Sprintf("missing arguments: %s", strings.Join(missingArgs, ", ")),
		helpData:     c.Command,
		helpTemplate: cli.CommandHelpTemplate,
	}
}

func commandNotFoundError(c *cli.Context, cmd string) error {
	return &usageError{
		context:      c,
		message:      fmt.Sprintf("unknown command %q", cmd),
		helpData:     c.App,
		helpTemplate: cli.SubcommandHelpTemplate,
	}
}
