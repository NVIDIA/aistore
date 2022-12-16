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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools/docker"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"k8s.io/apimachinery/pkg/util/duration"
)

const (
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

var (
	clusterURL        string
	defaultHTTPClient *http.Client
	authnHTTPClient   *http.Client
	apiBP             api.BaseParams
	authParams        api.BaseParams
)

type (
	progressBarArgs struct {
		barType string
		barText string
		total   int64
		options []mpb.BarOption
	}

	nvpair struct {
		Name  string
		Value string
	}
	nvpairList []nvpair

	propDiff struct {
		Name    string
		Current string
		Old     string
	}
)

func argLast(c *cli.Context) (last string) {
	if l := c.NArg(); l > 0 {
		last = c.Args().Get(l - 1)
	}
	return
}

func isWebURL(url string) bool { return cos.IsHTTP(url) || cos.IsHTTPS(url) }

func helpMessage(template string, data any) string {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	// Execute the template that generates command usage text
	cli.HelpPrinterCustom(w, template, data, tmpls.HelpTemplateFuncMap)
	_ = w.Flush()

	return buf.String()
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

// Get config from a random target.
func getRandTargetConfig(c *cli.Context) (*cmn.Config, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}
	si, err := smap.GetRandTarget()
	if err != nil {
		return nil, err
	}
	cfg, err := api.GetDaemonConfig(apiBP, si)
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

//nolint:unparam // !requireProviderInURI (with default ais://) is a currently never used (feature)
func parseBckURI(c *cli.Context, uri string, requireProviderInURI bool) (cmn.Bck, error) {
	if isWebURL(uri) {
		bck := parseURLtoBck(uri)
		return bck, nil
	}

	opts := cmn.ParseURIOpts{}
	if cfg != nil && !requireProviderInURI {
		opts.DefaultProvider = cfg.DefaultProvider
	}
	bck, objName, err := cmn.ParseBckObjectURI(uri, opts)
	switch {
	case err != nil:
		return cmn.Bck{}, err
	case objName != "":
		return cmn.Bck{}, objectNameArgumentNotSupported(c, objName)
	case bck.Name == "":
		return cmn.Bck{}, incorrectUsageMsg(c, "%q: missing bucket name", uri)
	default:
		if err = bck.Validate(); err != nil {
			return cmn.Bck{}, cannotExecuteError(c, err)
		}
	}
	return bck, nil
}

func parseQueryBckURI(c *cli.Context, uri string) (cmn.QueryBcks, error) {
	uri = preparseBckObjURI(uri)
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

// Convert a list of "key value" and "key=value" pairs into a map
func makePairs(args []string) (nvs cos.StrKVs, err error) {
	var (
		i  int
		ll = len(args)
	)
	nvs = cos.StrKVs{}
	for i < ll {
		if args[i] != keyAndValueSeparator && strings.Contains(args[i], keyAndValueSeparator) {
			pairs := strings.SplitN(args[i], keyAndValueSeparator, 2)
			nvs[pairs[0]] = pairs[1]
			i++
		} else if i < ll-2 && args[i+1] == keyAndValueSeparator {
			nvs[args[i]] = args[i+2]
			i += 3
		} else if args[i] == feat.FeaturesPropName && i < ll-1 {
			nvs[args[i]] = strings.Join(args[i+1:], ",") // NOTE: only features nothing else in the tail
			return
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

func parseRemAliasURL(c *cli.Context) (alias, remAisURL string, err error) {
	var parts []string
	if c.NArg() == 0 {
		err = missingArgumentsError(c, c.Command.ArgsUsage)
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
	if _, err = url.ParseRequestURI(remAisURL); err == nil {
		err = cmn.ValidateRemAlias(alias)
	}
	return
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

func parseFeatureFlags(v string) (res feat.Flags, err error) {
	if v == "" || v == NilValue {
		return 0, nil
	}
	values := strings.Split(v, ",")
	for _, v := range values {
		var f feat.Flags
		if f, err = feat.StrToFeat(v); err != nil {
			return
		}
		res |= f // TODO -- FIXME: use res.Set(f)
	}
	return
}

// TODO: support `allow` and `deny` verbs/operations on existing access permissions
func makeBckPropPairs(values []string) (nvs cos.StrKVs, err error) {
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
	nvs = make(cos.StrKVs, 8)
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
		bck, err := parseBckURI(c, bckURI, true /*require provider*/)
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
	if p, err = api.HeadBucket(apiBP, bck, dontAddBckMD); err == nil {
		return
	}
	if herr, ok := err.(*cmn.ErrHTTP); ok {
		if herr.Status == http.StatusNotFound {
			err = fmt.Errorf("bucket %q does not exist", bck)
		} else if herr.Message != "" {
			err = errors.New(herr.Message)
		} else {
			err = fmt.Errorf("failed to HEAD bucket %q: %s", bck, herr.Message)
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

		debug.Assert(len(clustersIDs) > 0, "There should be at least one cluster running, when docker running detected.")

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

func bckPropList(props *cmn.BucketProps, verbose bool) (propList nvpairList) {
	if !verbose { // i.e., compact
		propList = nvpairList{
			{"created", fmtBucketCreatedTime(props.Created)},
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
				propList = append(propList, nvpair{Name: "original-url", Value: origURL})
			}
		}
	} else {
		err := cmn.IterFields(props, func(tag string, field cmn.IterField) (error, bool) {
			var value string
			switch tag {
			case apc.PropBucketCreated:
				value = fmtBucketCreatedTime(props.Created)
			case apc.PropBucketAccessAttrs:
				value = props.Access.Describe()
			default:
				value = fmt.Sprintf("%v", field.Value())
			}
			propList = append(propList, nvpair{Name: tag, Value: value})
			return nil, false
		})
		debug.AssertNoErr(err)
	}

	sort.Slice(propList, func(i, j int) bool {
		return propList[i].Name < propList[j].Name
	})
	return
}

func fmtBucketCreatedTime(created int64) string {
	if created == 0 {
		return tmpls.NotSetVal
	}
	return time.Unix(0, created).Format(time.RFC3339)
}

// compare with tmpls.isUnsetTime() and fmtBucketCreatedTime() above
func isUnsetTime(c *cli.Context, ts string) bool {
	t, err := time.Parse(time.RFC822, ts)
	if err != nil {
		actionWarn(c, fmt.Sprintf("failed to parse %q using RFC822", ts))
		return false
	}
	if t.IsZero() {
		return true
	}
	tss := t.String()
	return strings.HasPrefix(tss, "1969") || strings.HasPrefix(tss, "1970")
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
	var err error
	prompt += " [Y/N]"
	if len(warning) != 0 {
		actionWarn(c, warning[0])
	}
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
	msg := &apc.LsoMsg{}
	msg.SetFlag(apc.LsObjCached)
	msg.SetFlag(apc.LsNameOnly)
	objList, err := api.ListObjectsPage(apiBP, bck, msg)
	if err != nil {
		return false, err
	}
	return len(objList.Entries) == 0, nil
}

func ensureHasProvider(bck cmn.Bck) error {
	if !apc.IsProvider(bck.Provider) {
		return fmt.Errorf("missing backend provider in %q", bck)
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
func flattenConfig(cfg any, section string) (flat nvpairList) {
	flat = make(nvpairList, 0, 40)
	cmn.IterFields(cfg, func(tag string, field cmn.IterField) (error, bool) {
		if section == "" || strings.HasPrefix(tag, section) {
			v := _toStr(field.Value())
			flat = append(flat, nvpair{tag, v})
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

func diffConfigs(actual, original nvpairList) []propDiff {
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

func printSectionJSON(c *cli.Context, in any, section string) (done bool) {
	if i := strings.LastIndexByte(section, '.'); i > 0 {
		section = section[:i]
	}
	if done = _printSection(c, in, section); !done {
		// e.g. keepalivetracker.proxy.name
		if i := strings.LastIndexByte(section, '.'); i > 0 {
			section = section[:i]
			done = _printSection(c, in, section)
		}
	}
	if !done {
		actionWarn(c, "config section (or section prefix) \""+section+"\" not found.")
	}
	return
}

func _printSection(c *cli.Context, in any, section string) (done bool) {
	var (
		beg       = regexp.MustCompile(`\s+"` + section + `\S*": {`)
		end       = regexp.MustCompile(`},\n`)
		nst       = regexp.MustCompile(`\s+"\S+": {`)
		nonstruct = regexp.MustCompile(`\s+"` + section + `\S*": ".+"[,\n\r]{1}`)
	)
	out, err := jsoniter.MarshalIndent(in, "", "    ")
	if err != nil {
		return
	}

	from := beg.FindIndex(out)
	if from == nil {
		loc := nonstruct.FindIndex(out)
		if loc == nil {
			return
		}
		res := out[loc[0] : loc[1]-1]
		fmt.Fprintln(c.App.Writer, "{"+string(res)+"\n}")
		return true
	}

	to := end.FindIndex(out[from[1]:])
	if to == nil {
		return
	}
	res := out[from[0] : from[1]+to[1]-1]

	if nst.FindIndex(res[from[1]-from[0]+1:]) != nil {
		// resort to counting nested structures
		var cnt, off int
		res = out[from[0]:]
		for off = 0; off < len(res); off++ {
			if res[off] == '{' {
				cnt++
			} else if res[off] == '}' {
				cnt--
				if cnt == 0 {
					res = out[from[0] : from[0]+off+1]
					goto done
				}
			}
		}
		return
	}
done:
	if l := len(res); res[l-1] == ',' {
		res = res[:l-1]
	}
	fmt.Fprintln(c.App.Writer, string(res)+"\n")
	return true
}

// First, request cluster's config from the primary node that contains
// default Cksum type. Second, generate default list of properties.
func defaultBckProps(c *cli.Context, bck cmn.Bck) (*cmn.BucketProps, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}
	cfg, err := api.GetDaemonConfig(apiBP, smap.Primary)
	if err != nil {
		return nil, err
	}
	props := bck.DefaultProps(cfg)
	return props, nil
}

// see also flattenConfig
func authNConfPairs(conf *authn.Config, prefix string) (nvpairList, error) {
	flat := make(nvpairList, 0, 8)
	err := cmn.IterFields(conf, func(tag string, field cmn.IterField) (error, bool) {
		if prefix != "" && !strings.HasPrefix(tag, prefix) {
			return nil, false
		}
		v := _toStr(field.Value())
		flat = append(flat, nvpair{Name: tag, Value: v})
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

// NOTE: as provider, AIS can be (local cluster | remote cluster) -
// return only the former and handle remote case separately
func selectProvidersExclRais(bcks cmn.Bcks) (sorted []string) {
	sorted = make([]string, 0, len(apc.Providers))
	for p := range apc.Providers {
		for _, bck := range bcks {
			if bck.Provider != p {
				continue
			}
			if bck.IsAIS() {
				sorted = append(sorted, p)
			} else if p != apc.AIS {
				sorted = append(sorted, p)
			}
			break
		}
	}
	sort.Strings(sorted)
	return
}

// `ais ls` and friends: allow for `provider:` shortcut
func preparseBckObjURI(uri string) string {
	if uri == "" {
		return uri
	}
	p := strings.TrimSuffix(uri, ":")
	if _, err := cmn.NormalizeProvider(p); err == nil {
		return p + apc.BckProviderSeparator
	}
	return uri // unchanged
}

func actionDone(c *cli.Context, msg string) { fmt.Fprintln(c.App.Writer, msg) }
func actionWarn(c *cli.Context, msg string) { fmt.Fprintln(c.App.ErrWriter, fcyan("Warning: ")+msg) }

func actionCptn(c *cli.Context, prefix, msg string) {
	if prefix == "" {
		fmt.Fprintln(c.App.Writer, fcyan(msg))
	} else {
		fmt.Fprintln(c.App.Writer, fcyan(prefix)+msg)
	}
}
