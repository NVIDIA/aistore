// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file contains util functions and types.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cli/config"
	"github.com/NVIDIA/aistore/containers"

	"github.com/urfave/cli"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

const (
	Infinity             = -1
	keyAndValueSeparator = "="
	fileStdIO            = "-"

	// Error messages
	dockerErrMsgFmt       = "Failed to discover docker proxy URL: %v.\nUsing default %q.\n"
	durationParseErrorFmt = "error converting refresh flag value %q to time duration: %v"
	invalidDaemonMsg      = "%s is not a valid DAEMON_ID"
	invalidCmdMsg         = "invalid command name '%s'"
	invalidFlagsMsgFmt    = "flags %s are invalid when arguments have been provided"

	// Scheme parsing
	defaultScheme = "https"
	gsScheme      = "gs"
	s3Scheme      = "s3"
	aisScheme     = "ais"
	gsHost        = "storage.googleapis.com"
	s3Host        = "s3.amazonaws.com"
)

var (
	clusterURL        string
	defaultHTTPClient *http.Client
	defaultAPIParams  api.BaseParams
	mu                sync.Mutex
)

//
// Error handling
//

type usageError struct {
	context      *cli.Context
	message      string
	helpData     interface{}
	helpTemplate string
}

type additionalInfoError struct {
	baseErr        error
	additionalInfo string
}

func (e *usageError) Error() string {
	msg := helpMessage(e.helpTemplate, e.helpData)
	if e.context.Command.Name != "" {
		return fmt.Sprintf("Incorrect usage of \"%s %s\": %s.\n\n%s", e.context.App.Name, e.context.Command.Name, e.message, msg)
	}
	return fmt.Sprintf("Incorrect usage of \"%s\": %s.\n\n%s", e.context.App.Name, e.message, msg)
}

func helpMessage(template string, data interface{}) string {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	// Execute the template that generates command usage text
	cli.HelpPrinter(w, template, data)
	_ = w.Flush()

	return buf.String()
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
		message:      fmt.Sprintf("missing arguments %q", strings.Join(missingArgs, ", ")),
		helpData:     c.Command,
		helpTemplate: cli.CommandHelpTemplate,
	}
}

func missingFlagsError(c *cli.Context, missingFlags []string, message ...string) error {
	cmn.Assert(len(missingFlags) > 0)

	fullMessage := fmt.Sprintf("missing required flags %q", strings.Join(missingFlags, ", "))
	if len(message) > 0 {
		fullMessage += " - " + message[0]
	}

	return &usageError{
		context:      c,
		message:      fullMessage,
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

func (e *additionalInfoError) Error() string {
	return fmt.Sprintf("%s %s", e.baseErr.Error(), cmn.StrToSentence(e.additionalInfo))
}

func newAdditionalInfoError(err error, info string) error {
	cmn.Assert(err != nil)
	return &additionalInfoError{
		baseErr:        err,
		additionalInfo: info,
	}
}

//
// Smap
//

// Populates the proxy and target maps
func fillMap() (*cluster.Smap, error) {
	var (
		wg = &sync.WaitGroup{}
	)
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return nil, err
	}
	// Get the primary proxy's smap
	smapPrimary, err := api.GetNodeClusterMap(defaultAPIParams, smap.ProxySI.ID())
	if err != nil {
		return nil, err
	}

	proxyCount := smapPrimary.CountProxies()
	targetCount := smapPrimary.CountTargets()
	errCh := make(chan error, proxyCount+targetCount)

	wg.Add(proxyCount + targetCount)
	retrieveStatus(smapPrimary.Pmap, proxy, wg, errCh)
	retrieveStatus(smapPrimary.Tmap, target, wg, errCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	return smapPrimary, nil
}

func retrieveStatus(nodeMap cluster.NodeMap, daeMap map[string]*stats.DaemonStatus, wg *sync.WaitGroup, errCh chan error) {
	fill := func(dae *cluster.Snode) error {
		obj, err := api.GetDaemonStatus(defaultAPIParams, dae.ID())
		if err != nil {
			return err
		}
		mu.Lock()
		daeMap[dae.ID()] = obj
		mu.Unlock()
		return nil
	}

	for _, si := range nodeMap {
		go func(si *cluster.Snode) {
			errCh <- fill(si)
			wg.Done()
		}(si)
	}
}

//
// Scheme
//

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
		scheme = "http"
		if !strings.Contains(host, ":") {
			host += ":8080"
		}
		fullPath = path.Join(cmn.Version, cmn.Objects, fullPath)
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
		err = fmt.Errorf("destination must look as %q, for instance: %s://bucket/objname (got %s)",
			aisScheme, aisScheme, destScheme)
		return
	}
	if destBucket == "" {
		err = fmt.Errorf("destination bucket name cannot be omitted")
		return
	}
	destPathSuffix = strings.Trim(destPathSuffix, "/")
	return destBucket, destPathSuffix, nil
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

func getPrefixFromPrimary() (string, error) {
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return "", err
	}

	cfg, err := api.GetDaemonConfig(defaultAPIParams, smap.ProxySI.ID())
	if err != nil {
		return "", err
	}

	return cfg.Net.HTTP.Proto + "://", nil
}

//
// Flags
//

// If the flag has multiple values (separated by comma), take the first one
func cleanFlag(flag string) string {
	return strings.Split(flag, ",")[0]
}

func flagIsSet(c *cli.Context, flag cli.Flag) bool {
	// If the flag name has multiple values, take the first one
	flagName := cleanFlag(flag.GetName())
	return c.GlobalIsSet(flagName) || c.IsSet(flagName)
}

// Returns the value of a string flag (either parent or local scope)
func parseStrFlag(c *cli.Context, flag cli.Flag) string {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalString(flagName)
	}
	return c.String(flagName)
}

// Returns the value of an int flag (either parent or local scope)
func parseIntFlag(c *cli.Context, flag cli.Flag) int {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalInt(flagName)
	}
	return c.Int(flagName)
}

func parseByteFlagToInt(c *cli.Context, flag cli.Flag) (int64, error) {
	flagValue := parseStrFlag(c, flag)
	b, err := cmn.S2B(flagValue)
	if err != nil {
		return 0, fmt.Errorf("%s (%s) is invalid, expected either a number or a number with a size suffix (kb, MB, GiB, ...)", flag.GetName(), flagValue)
	}

	return b, nil
}

// Returns a string containing the value of the `flag` in bytes, used for `offset` and `length` flags
func getByteFlagValue(c *cli.Context, flag cli.Flag) (string, error) {
	if flagIsSet(c, flag) {
		offsetInt, err := parseByteFlagToInt(c, flag)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(offsetInt, 10), nil
	}

	return "", nil
}

func checkFlags(c *cli.Context, flag []cli.Flag, message ...string) error {
	missingFlags := make([]string, 0)

	for _, f := range flag {
		if !flagIsSet(c, f) {
			missingFlags = append(missingFlags, f.GetName())
		}
	}

	if len(missingFlags) >= 1 {
		return missingFlagsError(c, missingFlags, message...)
	}

	return nil
}

func calcRefreshRate(c *cli.Context) (time.Duration, error) {
	const (
		refreshRateMin = 500 * time.Millisecond
	)

	refreshRate := refreshRateDefault

	if flagIsSet(c, refreshFlag) {
		flagStr := parseStrFlag(c, refreshFlag)
		flagDuration, err := time.ParseDuration(flagStr)
		if err != nil {
			return 0, fmt.Errorf(durationParseErrorFmt, flagStr, err)
		}

		refreshRate = flagDuration
		if refreshRate < refreshRateMin {
			refreshRate = refreshRateMin
		}
	}

	return refreshRate, nil
}

//
// Long run parameters
//

type longRunParams struct {
	count       int
	refreshRate time.Duration
}

func defaultLongRunParams() *longRunParams {
	return &longRunParams{
		count:       countDefault,
		refreshRate: refreshRateDefault,
	}
}

func (p *longRunParams) isInfiniteRun() bool {
	return p.count == Infinity
}

func updateLongRunParams(c *cli.Context) error {
	params := c.App.Metadata[metadata].(*longRunParams)

	if flagIsSet(c, refreshFlag) {
		rateStr := parseStrFlag(c, refreshFlag)
		rate, err := time.ParseDuration(rateStr)
		if err != nil {
			return fmt.Errorf(durationParseErrorFmt, rateStr, err)
		}
		params.refreshRate = rate
		// Run forever unless `count` is also specified
		params.count = Infinity
	}

	if flagIsSet(c, countFlag) {
		params.count = parseIntFlag(c, countFlag)
		if params.count <= 0 {
			_, _ = fmt.Fprintf(c.App.ErrWriter, "Warning: '%s' set to %d, but expected value >= 1. Assuming '%s' = %d.\n",
				countFlag.Name, params.count, countFlag.Name, countDefault)
			params.count = countDefault
		}
	}

	return nil
}

//
// Utility functions
//

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

func chooseTmpl(tmplShort, tmplLong string, useShort bool) string {
	if useShort {
		return tmplShort
	}
	return tmplLong
}

func splitBucketObject(objname string) (bucket string, object string) {
	s := strings.Split(objname, "/")
	if len(s) > 1 {
		bucket = s[0]
		object = strings.Join(s[1:], "/")
		return
	}
	return s[0], ""
}

func bucketsFromArgsOrEnv(c *cli.Context) ([]string, error) {
	buckets := c.Args()

	var nonEmptyBuckets cli.Args
	for _, bucket := range buckets {
		if bucket != "" {
			nonEmptyBuckets = append(nonEmptyBuckets, cleanBucketName(bucket))
		}
	}

	if len(nonEmptyBuckets) != 0 {
		return nonEmptyBuckets, nil
	}

	return nil, missingArgumentsError(c, "bucket name")
}

func cliAPIParams(proxyURL string) api.BaseParams {
	return api.BaseParams{
		Client: defaultHTTPClient,
		URL:    proxyURL,
		Token:  loggedUserToken.Token,
	}
}

func canReachBucket(bck cmn.Bck) error {
	if _, err := api.HeadBucket(defaultAPIParams, bck); err != nil {
		if httpErr, ok := err.(*cmn.HTTPError); ok {
			if httpErr.Status == http.StatusNotFound {
				return fmt.Errorf("bucket with name %q does not exist", bck)
			}
		}
		return fmt.Errorf("could not reach %q bucket: %v", bck, err)
	}
	return nil
}

// Function returns the bucket provider either from:
// 1) Function argument provider (highest priority)
// 2) Command line flag --provider
// 3) Environment variable (lowest priority)
func bucketProvider(c *cli.Context, providers ...string) string {
	provider := ""
	if len(providers) > 0 {
		provider = providers[0]
	}
	if provider == "" {
		provider = parseStrFlag(c, providerFlag)
	}
	if provider == "" {
		provider = os.Getenv(aisProviderEnvVar)
	}
	return provider
}

//
// AIS cluster discovery
//

// determineClusterURL resolving order
// 1. cfg.Cluster.URL; if empty:
// 2. Proxy docker containter IP address; if not successful:
// 3. Docker default; if not present:
// 4. Default as cfg.Cluster.DefaultAISHost
func determineClusterURL(cfg *config.Config) string {
	if cfg.Cluster.URL != "" {
		return cfg.Cluster.URL
	}

	if containers.DockerRunning() {
		clustersIDs, err := containers.ClusterIDs()
		if err != nil {
			fmt.Fprintf(os.Stderr, dockerErrMsgFmt, err, cfg.Cluster.DefaultDockerHost)
			return cfg.Cluster.DefaultDockerHost
		}

		cmn.AssertMsg(len(clustersIDs) > 0, "there should be at least one cluster running, when docker running detected")

		proxyGateway, err := containers.ClusterProxyURL(clustersIDs[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, dockerErrMsgFmt, err, cfg.Cluster.DefaultDockerHost)
			return cfg.Cluster.DefaultDockerHost
		}

		if len(clustersIDs) > 1 {
			fmt.Fprintf(os.Stderr, "Multiple docker clusters running. Connected to %d via %s.\n", clustersIDs[0], proxyGateway)
		}

		return "http://" + proxyGateway + ":8080"
	}

	fmt.Fprintf(os.Stderr, "Warning! AIStore URL not configured, using default: %s\n", cfg.Cluster.DefaultAISHost)
	return cfg.Cluster.DefaultAISHost
}
