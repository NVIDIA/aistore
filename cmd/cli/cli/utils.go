// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"k8s.io/apimachinery/pkg/util/duration"
)

const (
	infinity             = -1
	keyAndValueSeparator = "="
	fileStdIO            = "-"

	// Error messages
	dockerErrMsgFmt = "Failed to discover docker proxy URL: %v.\nUsing default %q.\n"
	invalidCmdMsg   = "invalid command name %q"

	gsHost = "storage.googleapis.com"
	s3Host = "s3.amazonaws.com"

	sizeArg  = "SIZE"
	unitsArg = "UNITS"

	incorrectCmdDistance = 3
)

const refreshRateMinDur = time.Second

var (
	clusterURL        string
	defaultHTTPClient *http.Client
	authnHTTPClient   *http.Client
	defaultAPIParams  api.BaseParams
	authParams        api.BaseParams
	mu                sync.Mutex
)

type (
	progressBarArgs struct {
		barType string
		barText string
		total   int64
		options []mpb.BarOption
	}
	prop struct {
		Name  string
		Value string
	}
	propDiff struct {
		Name    string
		Current string
		Old     string
	}
)

func argDaemonID(c *cli.Context) string {
	daemonID := c.Args().First()
	return cluster.N2ID(daemonID)
}

func argLast(c *cli.Context) string { return c.Args().Get(c.NArg() - 1) }

func isWebURL(url string) bool { return cos.IsHTTP(url) || cos.IsHTTPS(url) }

func helpMessage(template string, data any) string {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	// Execute the template that generates command usage text
	cli.HelpPrinterCustom(w, template, data, tmpls.HelpTemplateFuncMap)
	_ = w.Flush()

	return buf.String()
}

func objectPropList(bck cmn.Bck, props *cmn.ObjectProps, selection []string) (propList []prop) {
	var propValue string
	for _, currProp := range selection {
		switch currProp {
		case apc.GetPropsName:
			propValue = props.Bck.String() + "/" + props.Name
		case apc.GetPropsSize:
			propValue = cos.B2S(props.Size, 2)
		case apc.GetPropsChecksum:
			propValue = props.Cksum.String()
		case apc.GetPropsAtime:
			propValue = cos.FormatUnixNano(props.Atime, "")
		case apc.GetPropsVersion:
			propValue = props.Ver
		case apc.GetPropsCached:
			if bck.IsAIS() {
				continue
			}
			propValue = tmpls.FmtBool(props.Present)
		case apc.GetPropsCopies:
			propValue = tmpls.FmtCopies(props.Mirror.Copies)
			if len(props.Mirror.Paths) != 0 {
				propValue += fmt.Sprintf(" %v", props.Mirror.Paths)
			}
		case apc.GetPropsEC:
			propValue = tmpls.FmtEC(
				props.EC.Generation, props.EC.DataSlices, props.EC.ParitySlices, props.EC.IsECCopy,
			)
		case apc.GetPropsCustom:
			if custom := props.GetCustomMD(); len(custom) == 0 {
				propValue = tmpls.NotSetVal
			} else {
				propValue = fmt.Sprintf("%+v", custom)
			}
		case apc.GetPropsNode:
			propValue = cluster.Tname(props.DaemonID)
		default:
			continue
		}
		propList = append(propList, prop{currProp, propValue})
	}

	sort.Slice(propList, func(i, j int) bool {
		return propList[i].Name < propList[j].Name
	})
	return
}

func didYouMeanMessage(c *cli.Context, cmd string) string {
	if alike := findCmdByKey(cmd); len(alike) > 0 {
		msg := fmt.Sprintf("%v", alike) //nolint:gocritic // alt formatting
		sb := &strings.Builder{}
		sb.WriteString(msg)
		sbWriteTail(c, sb)
		return fmt.Sprintf("Did you mean: %q?", sb.String())
	}

	closestCommand, distance := findClosestCommand(cmd, c.App.VisibleCommands())
	if distance >= cos.Max(incorrectCmdDistance, len(cmd)/2) {
		// 2nd attempt: check misplaced `show`
		// (that can be typed-in ex post facto as in: `ais object ... show` == `ais show object`)
		// (feature)
		if tail := c.Args().Tail(); len(tail) > 0 && tail[0] == commandShow {
			sb := &strings.Builder{}
			sb.WriteString(c.App.Name)
			sb.WriteString(" " + commandShow)
			sb.WriteString(" " + c.Args()[0])
			for _, f := range c.FlagNames() {
				sb.WriteString(" --" + f)
			}
			return fmt.Sprintf("Did you mean: %q?", sb.String())
		}
		return ""
	}
	sb := &strings.Builder{}
	sb.WriteString(c.App.Name)
	sb.WriteString(" " + closestCommand)
	sbWriteTail(c, sb)
	return fmt.Sprintf("Did you mean: %q?", sb.String())
}

func sbWriteTail(c *cli.Context, sb *strings.Builder) {
	if c.NArg() > 1 {
		for _, a := range c.Args()[1:] { // skip the wrong one
			sb.WriteString(" " + a)
		}
	}
	for _, f := range c.FlagNames() {
		sb.WriteString(" --" + f)
	}
}

func findClosestCommand(cmd string, candidates []cli.Command) (result string, distance int) {
	var (
		minDist     = math.MaxInt64
		closestName string
	)
	for i := 0; i < len(candidates); i++ {
		dist := cos.DamerauLevenstheinDistance(cmd, candidates[i].Name)
		if dist < minDist {
			minDist = dist
			closestName = candidates[i].Name
		}
	}
	return closestName, minDist
}

// Get cluster map and use it to retrieve node status for each clustered node
func fillMap() (*cluster.Smap, error) {
	wg := &sync.WaitGroup{}
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return nil, err
	}
	if smap.Primary.PubNet.URL != defaultAPIParams.URL {
		// TODO: cluster map (Smap) is replicated & synchronized across all nodes, and so
		//       this step can be made optional/configurable with default='not doing it'
		smapPrimary, err := api.GetNodeClusterMap(defaultAPIParams, smap.Primary.ID())
		if err != nil {
			return nil, err
		}
		smap = smapPrimary
	}
	proxyCount := smap.CountProxies()
	targetCount := smap.CountTargets()

	wg.Add(proxyCount + targetCount)
	retrieveStatus(smap.Pmap, pmapStatus, wg)
	retrieveStatus(smap.Tmap, tmapStatus, wg)
	wg.Wait()
	return smap, nil
}

func retrieveStatus(nodeMap cluster.NodeMap, daeMap stats.DaemonStatusMap, wg *sync.WaitGroup) {
	fill := func(node *cluster.Snode) {
		obj, _ := api.GetDaemonStatus(defaultAPIParams, node)
		if node.Flags.IsSet(cluster.NodeFlagMaint) {
			obj.Status = "maintenance"
		} else if node.Flags.IsSet(cluster.NodeFlagDecomm) {
			obj.Status = "decommission"
		}
		mu.Lock()
		daeMap[node.ID()] = obj
		mu.Unlock()
	}

	for _, si := range nodeMap {
		go func(si *cluster.Snode) {
			fill(si)
			wg.Done()
		}(si)
	}
}

// Get config from random target.
func getRandTargetConfig() (*cmn.Config, error) {
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return nil, err
	}
	si, err := smap.GetRandTarget()
	if err != nil {
		return nil, err
	}
	cfg, err := api.GetDaemonConfig(defaultAPIParams, si)
	if err != nil {
		return nil, err
	}
	return cfg, err
}

func isConfigProp(s string) bool {
	props := configPropList()
	for _, p := range props {
		if p == s || strings.HasPrefix(p, s+".") {
			return true
		}
	}
	return cos.StringInSlice(s, props)
}

func parseDest(c *cli.Context, uri string) (bck cmn.Bck, pathSuffix string, err error) {
	bck, pathSuffix, err = parseBckObjectURI(c, uri, true /*optional objName*/)
	if err != nil {
		return
	} else if bck.IsHTTP() {
		err = fmt.Errorf("http bucket is not supported as destination")
		return
	}
	pathSuffix = strings.Trim(pathSuffix, "/")
	return
}

func parseBckURI(c *cli.Context, uri string, requireProviderInURI ...bool) (cmn.Bck, error) {
	if isWebURL(uri) {
		bck := parseURLtoBck(uri)
		return bck, nil
	}
	opts := cmn.ParseURIOpts{}
	if cfg != nil && (len(requireProviderInURI) == 0 || !requireProviderInURI[0]) {
		opts.DefaultProvider = cfg.DefaultProvider
	}
	bck, objName, err := cmn.ParseBckObjectURI(uri, opts)
	if err != nil {
		return bck, err
	}
	if objName != "" {
		return bck, objectNameArgumentNotSupported(c, objName)
	}
	if bck.Name == "" {
		return bck, incorrectUsageMsg(c, "%q: missing bucket name", uri)
	} else if err := bck.Validate(); err != nil {
		return bck, cannotExecuteError(c, err)
	}
	return bck, nil
}

func parseQueryBckURI(c *cli.Context, uri string) (cmn.QueryBcks, error) {
	// allow for `provider:` shortcut
	if l := len(uri); l > 0 && uri[l-1] == ':' {
		provider := uri[0 : l-1]
		if _, err := cmn.NormalizeProvider(provider); err == nil {
			uri = provider + apc.BckProviderSeparator
		}
	}
	if isWebURL(uri) {
		bck := parseURLtoBck(uri)
		return cmn.QueryBcks(bck), nil
	}
	bck, objName, err := cmn.ParseBckObjectURI(uri, cmn.ParseURIOpts{IsQuery: true})
	if err != nil {
		return cmn.QueryBcks(bck), err
	} else if objName != "" {
		return cmn.QueryBcks(bck), objectNameArgumentNotSupported(c, objName)
	}
	return cmn.QueryBcks(bck), nil
}

func parseBckObjectURI(c *cli.Context, uri string, optObjName ...bool) (bck cmn.Bck, objName string, err error) {
	opts := cmn.ParseURIOpts{}
	if isWebURL(uri) {
		hbo, err := cmn.NewHTTPObjPath(uri)
		if err != nil {
			return bck, objName, err
		}
		bck, objName = hbo.Bck, hbo.ObjName
		goto validate
	}
	if cfg != nil {
		opts.DefaultProvider = cfg.DefaultProvider
	}
	bck, objName, err = cmn.ParseBckObjectURI(uri, opts)
	if err != nil {
		if len(uri) > 1 && uri[:2] == "--" { // FIXME: needed smth like c.LooksLikeFlag
			return bck, objName, incorrectUsageMsg(c, "misplaced flag %q", uri)
		}
		return bck, objName, cannotExecuteError(c, err)
	}

validate:
	if bck.Name == "" {
		return bck, objName, incorrectUsageMsg(c, "%q: missing bucket name", uri)
	} else if err := bck.Validate(); err != nil {
		return bck, objName, cannotExecuteError(c, err)
	} else if objName == "" && (len(optObjName) == 0 || !optObjName[0]) {
		return bck, objName, incorrectUsageMsg(c, "%q: missing object name", uri)
	}
	return
}

func getPrefixFromPrimary() string {
	scheme, _ := cmn.ParseURLScheme(clusterURL)
	if scheme == "" {
		scheme = "http"
	}
	return scheme + apc.BckProviderSeparator
}

func calcRefreshRate(c *cli.Context) time.Duration {
	refreshRate := refreshRateDefault
	if flagIsSet(c, refreshFlag) {
		refreshRate = cos.MaxDuration(parseDurationFlag(c, refreshFlag), refreshRateMinDur)
	}
	return refreshRate
}

// Users can pass in a comma-separated list
func makeList(list string) []string {
	cleanList := strings.Split(list, ",")
	for ii, val := range cleanList {
		cleanList[ii] = strings.TrimSpace(val)
	}
	return cleanList
}

// Converts a list of "key value" and "key=value" into map
func makePairs(args []string) (nvs cos.SimpleKVs, err error) {
	var (
		i  int
		ll = len(args)
	)
	nvs = cos.SimpleKVs{}
	for i < ll {
		if args[i] != keyAndValueSeparator && strings.Contains(args[i], keyAndValueSeparator) {
			pairs := strings.SplitN(args[i], keyAndValueSeparator, 2)
			nvs[pairs[0]] = pairs[1]
			i++
		} else if i < ll-2 && args[i+1] == keyAndValueSeparator {
			nvs[args[i]] = args[i+2]
			i += 3
		} else {
			// last name without a value
			if i == ll-1 {
				return nil, fmt.Errorf("invalid key=value pair %q", args[i])
			}
			nvs[args[i]] = args[i+1]
			i += 2
		}
	}
	return
}

func parseAliasURL(c *cli.Context) (alias, remAisURL string, err error) {
	var parts []string
	if c.NArg() == 0 {
		err = missingArgumentsError(c, aliasURLPairArgument)
		return
	}
	if c.NArg() > 1 {
		alias, remAisURL = c.Args().Get(0), c.Args().Get(1)
		goto ret
	}
	parts = strings.SplitN(c.Args().First(), keyAndValueSeparator, 2)
	if len(parts) < 2 {
		err = missingArgumentsError(c, aliasURLPairArgument)
		return
	}
	alias, remAisURL = parts[0], parts[1]
ret:
	_, err = url.ParseRequestURI(remAisURL)
	return
}

// Parses [TARGET_ID] [XACTION_ID|XACTION_NAME] [BUCKET]
func parseXactionFromArgs(c *cli.Context) (nodeID, xactID, xactKind string, bck cmn.Bck, err error) {
	var smap *cluster.Smap
	smap, err = api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return
	}
	shift := 0
	xactKind = argDaemonID(c)
	if node := smap.GetProxy(xactKind); node != nil {
		return "", "", "", bck, fmt.Errorf("daemon %q is a proxy", xactKind)
	}
	if node := smap.GetTarget(xactKind); node != nil {
		nodeID = xactKind
		xactKind = c.Args().Get(1)
		shift++
	}

	uri := c.Args().Get(1 + shift)
	if !xact.IsValidKind(xactKind) {
		xactID = xactKind
		xactKind = ""
		uri = c.Args().Get(1 + shift)
	} else if strings.Contains(xactKind, apc.BckProviderSeparator) {
		uri = xactKind
		xactKind = ""
	}
	if uri != "" {
		switch xact.Table[xactKind].Scope {
		case xact.ScopeBck:
			// Bucket is optional.
			if uri := c.Args().Get(1); uri != "" {
				if bck, err = parseBckURI(c, uri); err != nil {
					return "", "", "", bck, err
				}
				if _, err = headBucket(bck, true /* don't add */); err != nil {
					return "", "", "", bck, err
				}
			}
		default:
			if c.NArg() > 1 {
				fmt.Fprintf(c.App.ErrWriter,
					"Warning: %q is a non bucket-scope xaction, ignoring bucket name\n", xactKind)
			}
		}
	}
	return
}

// Get list of xactions
func listXactions(onlyStartable bool) []string {
	xactKinds := make([]string, 0)
	for kind, dtor := range xact.Table {
		if !onlyStartable || (onlyStartable && dtor.Startable) {
			xactKinds = append(xactKinds, kind)
		}
	}
	sort.Strings(xactKinds)
	return xactKinds
}

func isJSON(arg string) bool {
	possibleJSON := arg
	if possibleJSON == "" {
		return false
	}
	if possibleJSON[0] == '\'' && possibleJSON[len(possibleJSON)-1] == '\'' {
		possibleJSON = possibleJSON[1 : len(possibleJSON)-1]
	}
	if len(possibleJSON) >= 2 {
		return possibleJSON[0] == '{' && possibleJSON[len(possibleJSON)-1] == '}'
	}
	return false
}

func parseBucketAccessValues(values []string, idx int) (access apc.AccessAttrs, newIdx int, err error) {
	var (
		acc apc.AccessAttrs
		val uint64
	)
	if len(values) == 0 {
		return access, idx, nil
	}
	// Case: `access GET,PUT`
	if strings.Index(values[idx], ",") > 0 {
		newIdx = idx + 1
		for _, perm := range strings.Split(values[idx], ",") {
			perm = strings.TrimSpace(perm)
			if perm == "" {
				continue
			}
			acc, err = apc.StrToAccess(perm)
			if err != nil {
				return
			}
			access |= acc
		}
		return
	}
	for newIdx = idx; newIdx < len(values); {
		// Case: `access 0x342`
		val, err = parseHexOrUint(values[newIdx])
		if err == nil {
			access |= apc.AccessAttrs(val)
			newIdx++
			continue
		}
		// Case: `access HEAD`
		acc, err = apc.StrToAccess(values[newIdx])
		if err != nil {
			break
		}
		access |= acc
		newIdx++
	}
	if idx == newIdx {
		return 0, newIdx, err
	}
	return access, newIdx, nil
}

// TODO: support `allow` and `deny` verbs/operations on existing access permissions
func makeBckPropPairs(values []string) (nvs cos.SimpleKVs, err error) {
	props := make([]string, 0, 20)
	err = cmn.IterFields(&cmn.BucketPropsToUpdate{}, func(tag string, _ cmn.IterField) (error, bool) {
		props = append(props, tag)
		return nil, false
	})
	if err != nil {
		return
	}

	var (
		access apc.AccessAttrs
		cmd    string
	)
	nvs = make(cos.SimpleKVs, 8)
	for idx := 0; idx < len(values); {
		pos := strings.Index(values[idx], "=")
		if pos > 0 {
			if cmd != "" {
				return nil, fmt.Errorf("missing property %q value", cmd)
			}
			key := strings.TrimSpace(values[idx][:pos])
			val := strings.TrimSpace(values[idx][pos+1:])
			nvs[key] = val
			cmd = ""
			idx++
			continue
		}
		isCmd := cos.StringInSlice(values[idx], props)
		if cmd != "" && isCmd {
			return nil, fmt.Errorf("missing property %q value", cmd)
		}
		if cmd == "" && !isCmd {
			return nil, fmt.Errorf("invalid property %q", values[idx])
		}
		if cmd != "" {
			nvs[cmd] = values[idx]
			idx++
			cmd = ""
			continue
		}
		cmd = values[idx]
		idx++
		if cmd == apc.PropBucketAccessAttrs {
			access, idx, err = parseBucketAccessValues(values, idx)
			if err != nil {
				return nil, err
			}
			nvs[cmd] = strconv.FormatUint(uint64(access), 10)
			cmd = ""
		}
	}

	if cmd != "" {
		return nil, fmt.Errorf("missing property %q value", cmd)
	}
	return
}

func parseBckPropsFromContext(c *cli.Context) (props *cmn.BucketPropsToUpdate, err error) {
	propArgs := c.Args().Tail()

	if c.Command.Name == commandCreate {
		inputProps := parseStrFlag(c, bucketPropsFlag)
		if isJSON(inputProps) {
			err = jsoniter.Unmarshal([]byte(inputProps), &props)
			return
		}
		propArgs = strings.Split(inputProps, " ")
	}

	if len(propArgs) == 1 && isJSON(propArgs[0]) {
		err = jsoniter.Unmarshal([]byte(propArgs[0]), &props)
		return
	}

	// For setting bucket props via json attributes
	if len(propArgs) == 0 {
		err = missingArgumentsError(c, "property key-value pairs")
		return
	}

	// For setting bucket props via key-value list
	nvs, err := makeBckPropPairs(propArgs)
	if err != nil {
		return
	}

	if err = reformatBackendProps(c, nvs); err != nil {
		return
	}

	props, err = cmn.NewBucketPropsToUpdate(nvs)
	return
}

func bucketsFromArgsOrEnv(c *cli.Context) ([]cmn.Bck, error) {
	uris := c.Args()
	bcks := make([]cmn.Bck, 0, len(uris))

	for _, bckURI := range uris {
		bck, err := parseBckURI(c, bckURI)
		if err != nil {
			return nil, err
		}
		if bckURI != "" {
			bcks = append(bcks, bck)
		}
	}

	if len(bcks) != 0 {
		return bcks, nil
	}

	return nil, missingArgumentsError(c, "bucket")
}

func cliAPIParams(proxyURL string) api.BaseParams {
	return api.BaseParams{
		Client: defaultHTTPClient,
		URL:    proxyURL,
		Token:  loggedUserToken,
	}
}

// NOTE:
// 1. By default, AIStore adds remote buckets to the cluster metadata on the fly.
// Remote bucket that was never accessed before just "shows up" when user performs
// HEAD, PUT, GET, SET-PROPS, and a variety of other operations.
// 2. This is done only once (and after confirming the bucket's existence and accessibility)
// and doesn't require any action from the user.
// However, when we explicitly do not want this (addition to BMD) to be happening,
// we override this behavior with `dontAddBckMD` parameter in the api.HeadBucket() call.
//
// 3. On the client side, we currently resort to an intuitive convention
// that all non-modifying operations (LIST, GET, HEAD) utilize `dontAddBckMD = true`.
func headBucket(bck cmn.Bck, dontAddBckMD bool) (p *cmn.BucketProps, err error) {
	if p, err = api.HeadBucket(defaultAPIParams, bck, dontAddBckMD); err == nil {
		return
	}
	if httpErr, ok := err.(*cmn.ErrHTTP); ok {
		if httpErr.Status == http.StatusNotFound {
			err = fmt.Errorf("bucket %q does not exist", bck)
		} else if httpErr.Message != "" {
			err = errors.New(httpErr.Message)
		} else {
			err = fmt.Errorf("failed to HEAD bucket %q: %s", bck, httpErr.Message)
		}
	} else {
		err = fmt.Errorf("failed to HEAD bucket %q: %v", bck, err)
	}
	return
}

//
// AIS cluster discovery
//

// determineClusterURL resolving order
// 1. cfg.Cluster.URL; if empty:
// 2. Proxy docker container IP address; if not successful:
// 3. Docker default; if not present:
// 4. Default as cfg.Cluster.DefaultAISHost
func determineClusterURL(cfg *config.Config) string {
	if envURL := os.Getenv(env.AIS.Endpoint); envURL != "" {
		return envURL
	}
	if cfg.Cluster.URL != "" {
		return cfg.Cluster.URL
	}

	if docker.IsRunning() {
		clustersIDs, err := docker.ClusterIDs()
		if err != nil {
			fmt.Fprintf(os.Stderr, dockerErrMsgFmt, err, cfg.Cluster.DefaultDockerHost)
			return cfg.Cluster.DefaultDockerHost
		}

		debug.AssertMsg(len(clustersIDs) > 0, "There should be at least one cluster running, when docker running detected.")

		proxyGateway, err := docker.ClusterEndpoint(clustersIDs[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, dockerErrMsgFmt, err, cfg.Cluster.DefaultDockerHost)
			return cfg.Cluster.DefaultDockerHost
		}

		if len(clustersIDs) > 1 {
			fmt.Fprintf(os.Stderr, "Multiple docker clusters running. Connected to %d via %s.\n", clustersIDs[0], proxyGateway)
		}

		return "http://" + proxyGateway + ":8080"
	}

	return cfg.Cluster.DefaultAISHost
}

func printDryRunHeader(c *cli.Context) {
	if flagIsSet(c, dryRunFlag) {
		fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
	}
}

// Prints multiple lines of fmtStr to writer w.
// For line number i, fmtStr is formatted with values of args at index i
// if maxLines >= 0 prints at most maxLines, otherwise prints everything until
// it reaches the end of one of args
func limitedLineWriter(w io.Writer, maxLines int, fmtStr string, args ...[]string) {
	objs := make([]any, 0, len(args))
	if fmtStr == "" || fmtStr[len(fmtStr)-1] != '\n' {
		fmtStr += "\n"
	}

	if maxLines < 0 {
		maxLines = math.MaxInt64
	}
	minLen := math.MaxInt64
	for _, a := range args {
		minLen = cos.Min(minLen, len(a))
	}

	i := 0
	for {
		for _, a := range args {
			objs = append(objs, a[i])
		}
		fmt.Fprintf(w, fmtStr, objs...)
		i++

		for _, a := range args {
			if len(a) <= i {
				return
			}
		}
		if i >= maxLines {
			fmt.Fprintf(w, "(and %d more)\n", minLen-i)
			return
		}
		objs = objs[:0]
	}
}

func simpleProgressBar(args ...progressBarArgs) (*mpb.Progress, []*mpb.Bar) {
	var (
		progress = mpb.New(mpb.WithWidth(progressBarWidth))
		bars     = make([]*mpb.Bar, 0, len(args))
	)

	for _, a := range args {
		var argDecorators []decor.Decorator
		switch a.barType {
		case unitsArg:
			argDecorators = []decor.Decorator{
				decor.Name(a.barText, decor.WC{W: len(a.barText) + 1, C: decor.DidentRight}),
				decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
			}
		case sizeArg:
			argDecorators = []decor.Decorator{
				decor.Name(a.barText, decor.WC{W: len(a.barText) + 1, C: decor.DidentRight}),
				decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncWidth),
			}
		default:
			debug.Assertf(false, "invalid argument: %s", a.barType)
		}
		options := make([]mpb.BarOption, 0, len(a.options)+2)
		options = append(options, a.options...)
		options = append(
			options,
			mpb.PrependDecorators(argDecorators...),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
		)
		bars = append(bars, progress.AddBar(a.total, options...))
	}

	return progress, bars
}

func bckPropList(props *cmn.BucketProps, verbose bool) (propList []prop) {
	if !verbose {
		propList = []prop{
			{"created", time.Unix(0, props.Created).Format(time.RFC3339)},
			{"provider", props.Provider},
			{"access", props.Access.Describe()},
			{"checksum", props.Cksum.String()},
			{"mirror", props.Mirror.String()},
			{"ec", props.EC.String()},
			{"lru", props.LRU.String()},
			{"versioning", props.Versioning.String()},
		}
		if props.Provider == apc.HTTP {
			origURL := props.Extra.HTTP.OrigURLBck
			if origURL != "" {
				propList = append(propList, prop{Name: "original-url", Value: origURL})
			}
		}
	} else {
		err := cmn.IterFields(props, func(tag string, field cmn.IterField) (error, bool) {
			value := fmt.Sprintf("%v", field.Value())
			if tag == apc.PropBucketAccessAttrs {
				value = props.Access.Describe()
			}
			propList = append(propList, prop{Name: tag, Value: value})
			return nil, false
		})
		debug.AssertNoErr(err)
	}

	sort.Slice(propList, func(i, j int) bool {
		return propList[i].Name < propList[j].Name
	})
	return
}

func readValue(c *cli.Context, prompt string) string {
	fmt.Fprintf(c.App.Writer, prompt+": ")
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(line, "\n")
}

func confirm(c *cli.Context, prompt string, warning ...string) (ok bool) {
	prompt += " [Y/N]"
	if len(warning) != 0 {
		fmt.Fprintln(c.App.Writer, "Warning:", warning[0])
	}

	var err error
	for {
		response := strings.ToLower(readValue(c, prompt))
		if ok, err = cos.ParseBool(response); err != nil {
			fmt.Println("Invalid input! Choose 'Y' for 'Yes' or 'N' for 'No'")
			continue
		}
		return
	}
}

// (not to confuse with bck.IsEmpty())
func isBucketEmpty(bck cmn.Bck) (bool, error) {
	msg := &apc.ListObjsMsg{}
	msg.SetFlag(apc.LsCached)
	msg.SetFlag(apc.LsNameOnly)
	objList, err := api.ListObjectsPage(defaultAPIParams, bck, msg)
	if err != nil {
		return false, err
	}
	return len(objList.Entries) == 0, nil
}

func ensureHasProvider(bck cmn.Bck, cmd string) error {
	if !apc.IsProvider(bck.Provider) {
		return fmt.Errorf("missing backend provider in bucket %q for command %q", bck, cmd)
	}
	return nil
}

func parseURLtoBck(strURL string) (bck cmn.Bck) {
	if strURL[len(strURL)-1:] != apc.BckObjnameSeparator {
		strURL += apc.BckObjnameSeparator
	}
	bck.Provider = apc.HTTP
	bck.Name = cmn.OrigURLBck2Name(strURL)
	return
}

// see also authNConfPairs
func flattenConfig(cfg any, section string) (flat []prop) {
	flat = make([]prop, 0, 40)
	cmn.IterFields(cfg, func(tag string, field cmn.IterField) (error, bool) {
		if section == "" || strings.HasPrefix(tag, section) {
			v := _toStr(field.Value())
			flat = append(flat, prop{tag, v})
		}
		return nil, false
	})
	return flat
}

// NOTE: remove secrets if any
func _toStr(v any) (s string) {
	m, ok := v.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	// prune
	for k, vv := range m {
		if strings.Contains(strings.ToLower(k), "secret") {
			delete(m, k)
			continue
		}
		if mm, ok := vv.(map[string]any); ok {
			for kk := range mm {
				if strings.Contains(strings.ToLower(kk), "secret") {
					delete(mm, kk)
				}
			}
		}
	}
	return fmt.Sprintf("%v", m)
}

func diffConfigs(actual, original []prop) []propDiff {
	diff := make([]propDiff, 0, len(actual))
	for _, a := range actual {
		item := propDiff{Name: a.Name, Current: a.Value, Old: "N/A"}
		for _, o := range original {
			if o.Name != a.Name {
				continue
			}
			if o.Value == a.Value {
				item.Old = tmpls.NotSetVal
			} else {
				item.Old = o.Value
			}
			break
		}
		diff = append(diff, item)
	}
	sort.Slice(diff, func(i, j int) bool {
		return diff[i].Name < diff[j].Name
	})
	return diff
}

// First, request cluster's config from the primary node that contains
// default Cksum type. Second, generate default list of properties.
func defaultBckProps(bck cmn.Bck) (*cmn.BucketProps, error) {
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return nil, err
	}
	cfg, err := api.GetDaemonConfig(defaultAPIParams, smap.Primary)
	if err != nil {
		return nil, err
	}
	props := bck.DefaultProps(cfg)
	return props, nil
}

// Wait for an Xaction to complete, and print if it aborted
func waitForXactionCompletion(defaultAPIParams api.BaseParams, args api.XactReqArgs) (err error) {
	if args.Timeout == 0 {
		args.Timeout = time.Minute // TODO: make it a flag and an argument with configurable default
	}
	status, err := api.WaitForXactionIC(defaultAPIParams, args)
	if err != nil {
		return err
	}
	if status.Aborted() {
		return fmt.Errorf("xaction with UUID %q was aborted", status.UUID)
	}

	return nil
}

// see also flattenConfig
func authNConfPairs(conf *authn.Config, prefix string) ([]prop, error) {
	flat := make([]prop, 0, 8)
	err := cmn.IterFields(conf, func(tag string, field cmn.IterField) (error, bool) {
		if prefix != "" && !strings.HasPrefix(tag, prefix) {
			return nil, false
		}
		v := _toStr(field.Value())
		flat = append(flat, prop{Name: tag, Value: v})
		return nil, false
	})
	sort.Slice(flat, func(i, j int) bool {
		return flat[i].Name < flat[j].Name
	})
	return flat, err
}

func formatStatHuman(name string, value int64) string {
	if value == 0 {
		return "0"
	}
	switch {
	case strings.HasSuffix(name, ".ns"):
		dur := time.Duration(value)
		return dur.String()
	case strings.HasSuffix(name, ".time"):
		dur := time.Duration(value)
		return duration.HumanDuration(dur)
	case strings.HasSuffix(name, ".size"):
		return cos.B2S(value, 2)
	case strings.HasSuffix(name, ".bps"):
		return cos.B2S(value, 0) + "/s"
	}
	return fmt.Sprintf("%d", value)
}

//////////////
// dlSource //
//////////////

type dlSourceBackend struct {
	bck    cmn.Bck
	prefix string
}

type dlSource struct {
	link    string
	backend dlSourceBackend
}

// Replace protocol (gs://, s3://, az://) with proper GCP/AWS/Azure URL
func parseSource(rawURL string) (source dlSource, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	var (
		cloudSource dlSourceBackend
		scheme      = u.Scheme
		host        = u.Host
		fullPath    = u.Path
	)

	// If `rawURL` is using `gs` or `s3` scheme ({gs/s3}://<bucket>/...)
	// then <bucket> is considered a `Host` by `url.Parse`.
	switch u.Scheme {
	case apc.GSScheme, apc.GCP:
		cloudSource = dlSourceBackend{
			bck:    cmn.Bck{Name: host, Provider: apc.GCP},
			prefix: strings.TrimPrefix(fullPath, "/"),
		}

		scheme = "https"
		host = gsHost
		fullPath = path.Join(u.Host, fullPath)
	case apc.S3Scheme, apc.AWS:
		cloudSource = dlSourceBackend{
			bck:    cmn.Bck{Name: host, Provider: apc.AWS},
			prefix: strings.TrimPrefix(fullPath, "/"),
		}

		scheme = "http"
		host = s3Host
		fullPath = path.Join(u.Host, fullPath)
	case apc.AZScheme, apc.Azure:
		// NOTE: We don't set the link here because there is no way to translate
		//  `az://bucket/object` into Azure link without account name.
		return dlSource{
			link: "",
			backend: dlSourceBackend{
				bck:    cmn.Bck{Name: host, Provider: apc.Azure},
				prefix: strings.TrimPrefix(fullPath, "/"),
			},
		}, nil
	case apc.AISScheme:
		// TODO: add support for the remote cluster
		scheme = "http" // TODO: How about `https://`?
		if !strings.Contains(host, ":") {
			host += ":8080" // TODO: What if host is listening on `:80` so we don't need port?
		}
		fullPath = path.Join(apc.Version, apc.Objects, fullPath)
	case "":
		scheme = apc.DefaultScheme
	case "https", "http":
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
	link, err := url.QueryUnescape(normalizedURL.String())
	return dlSource{
		link:    link,
		backend: cloudSource,
	}, err
}

///////////////////
// longRunParams //
///////////////////

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
	return p.count == infinity
}

func updateLongRunParams(c *cli.Context) error {
	params := c.App.Metadata[metadata].(*longRunParams)

	if flagIsSet(c, refreshFlag) {
		params.refreshRate = parseDurationFlag(c, refreshFlag)
		// Run forever unless `count` is also specified
		params.count = infinity
	}

	if flagIsSet(c, countFlag) {
		params.count = parseIntFlag(c, countFlag)
		if params.count <= 0 {
			_, _ = fmt.Fprintf(c.App.ErrWriter, "Warning: %q set to %d, but expected value >= 1. Assuming %q = %d.\n",
				countFlag.Name, params.count, countFlag.Name, countDefault)
			params.count = countDefault
		}
	}

	return nil
}

///////////////////
// progIndicator //
///////////////////

// TODO: integrate this with simpleProgressBar
type progIndicator struct {
	objName         string
	sizeTransferred *atomic.Int64
}

func (*progIndicator) start() { fmt.Print("\033[s") }
func (*progIndicator) stop()  { fmt.Println("") }

func (pi *progIndicator) printProgress(incr int64) {
	fmt.Print("\033[u\033[K")
	fmt.Printf("Uploaded %s: %s", pi.objName, cos.B2S(pi.sizeTransferred.Add(incr), 2))
}

func newProgIndicator(objName string) *progIndicator {
	return &progIndicator{objName, atomic.NewInt64(0)}
}

// get xaction progress message
func xactProgressMsg(xactID string) string {
	return fmt.Sprintf("use '%s %s %s %s %s' to monitor progress",
		cliName, commandJob, commandShow, subcmdXaction, xactID)
}

///////////////
// CLI flags //
///////////////

// If the flag has multiple values (separated by comma), take the first one
func cleanFlag(flag string) string {
	return strings.Split(flag, ",")[0]
}

func flagIsSet(c *cli.Context, flag cli.Flag) (v bool) {
	name := cleanFlag(flag.GetName()) // take the first of multiple names
	switch flag.(type) {
	case cli.BoolFlag:
		v = c.Bool(name)
	case cli.BoolTFlag:
		v = c.BoolT(name)
	default:
		v = c.GlobalIsSet(name) || c.IsSet(name)
	}
	return
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
func parseIntFlag(c *cli.Context, flag cli.IntFlag) int {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalInt(flagName)
	}
	return c.Int(flagName)
}

// Returns the value of an duration flag (either parent or local scope)
func parseDurationFlag(c *cli.Context, flag cli.Flag) time.Duration {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalDuration(flagName)
	}
	return c.Duration(flagName)
}

func parseByteFlagToInt(c *cli.Context, flag cli.Flag) (int64, error) {
	flagValue := parseStrFlag(c, flag.(cli.StringFlag))
	b, err := cos.S2B(flagValue)
	if err != nil {
		return 0, fmt.Errorf("%s (%s) is invalid, expected either a number or a number with a size suffix (kb, MB, GiB, ...)",
			flag.GetName(), flagValue)
	}
	return b, nil
}

func parseChecksumFlags(c *cli.Context) []*cos.Cksum {
	cksums := []*cos.Cksum{}
	for _, ckflag := range supportedCksumFlags {
		if flagIsSet(c, ckflag) {
			cksums = append(cksums, cos.NewCksum(ckflag.GetName(), parseStrFlag(c, ckflag)))
		}
	}
	return cksums
}

func flattenXactStats(snap *xact.SnapExt) []*prop {
	props := make([]*prop, 0)
	if snap == nil {
		return props
	}
	fmtTime := func(t time.Time) string {
		if t.IsZero() {
			return tmpls.NotSetVal
		}
		return t.Format("01-02 15:04:05")
	}
	props = append(props,
		// Start xaction properties with a dot to make them first alphabetically
		&prop{Name: ".id", Value: snap.ID},
		&prop{Name: ".kind", Value: snap.Kind},
		&prop{Name: ".bck", Value: snap.Bck.String()},
		&prop{Name: ".start", Value: fmtTime(snap.StartTime)},
		&prop{Name: ".end", Value: fmtTime(snap.EndTime)},
		&prop{Name: ".aborted", Value: fmt.Sprintf("%t", snap.AbortedX)},

		&prop{Name: "loc.obj.n", Value: fmt.Sprintf("%d", snap.Stats.Objs)},
		&prop{Name: "loc.obj.size", Value: formatStatHuman(".size", snap.Stats.Bytes)},
		&prop{Name: "in.obj.n", Value: fmt.Sprintf("%d", snap.Stats.InObjs)},
		&prop{Name: "in.obj.size", Value: formatStatHuman(".size", snap.Stats.InBytes)},
		&prop{Name: "out.obj.n", Value: fmt.Sprintf("%d", snap.Stats.OutObjs)},
		&prop{Name: "out.obj.size", Value: formatStatHuman(".size", snap.Stats.OutBytes)},
	)
	if extStats, ok := snap.Ext.(map[string]any); ok {
		for k, v := range extStats {
			var value string
			if strings.HasSuffix(k, ".size") {
				val := v.(string)
				if i, err := strconv.ParseInt(val, 10, 64); err == nil {
					value = cos.B2S(i, 2)
				}
			}
			if value == "" {
				value = fmt.Sprintf("%v", v)
			}
			props = append(props, &prop{Name: k, Value: value})
		}
	}
	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})
	return props
}

func configPropList(scopes ...string) []string {
	scope := apc.Cluster
	if len(scopes) > 0 {
		scope = scopes[0]
	}
	propList := make([]string, 0, 48)
	err := cmn.IterFields(cmn.Config{}, func(tag string, _ cmn.IterField) (err error, b bool) {
		propList = append(propList, tag)
		return
	}, cmn.IterOpts{Allowed: scope})
	debug.AssertNoErr(err)
	return propList
}

func parseHexOrUint(s string) (uint64, error) {
	const hexPrefix = "0x"
	if strings.HasPrefix(s, hexPrefix) {
		return strconv.ParseUint(s[len(hexPrefix):], 16, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}

func selectProviders(bcks cmn.Bcks) (sorted []string) {
	sorted = make([]string, 0, len(apc.Providers))
	for p := range apc.Providers {
		for _, bck := range bcks {
			debug.Assert(apc.IsProvider(bck.Provider))
			if bck.Provider == p {
				sorted = append(sorted, p)
				break
			}
		}
	}
	sort.Strings(sorted)
	return
}
