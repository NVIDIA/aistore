// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bufio"
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
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/cli/hf"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

// This file contains common utilities and low-level helpers.

const (
	keyAndValueSeparator = "="

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
	clusterURL         string
	clientH, clientTLS *http.Client
	apiBP              api.BaseParams
	authParams         api.BaseParams
)

type (
	errInvalidNVpair struct {
		notpair string
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

	dlSourceBackend struct {
		bck    cmn.Bck
		prefix string
	}
	dlSource struct {
		link    string
		backend dlSourceBackend
		headers http.Header // Custom headers for download
	}
)

func joinCommandWords(subcommands ...string) string {
	return strings.Join(subcommands, " ")
}

// TODO: unify, use instead of splitting handlers (that each have different flags)
// reflection possibly can be used but requires way too many lines
func actionIsHandler(action any, handler func(c *cli.Context) error) bool {
	return fmt.Sprintf("%p", action) == fmt.Sprintf("%p", handler)
}

func argLast(c *cli.Context) (last string) {
	if l := c.NArg(); l > 0 {
		last = c.Args().Get(l - 1)
	}
	return
}

// arg[0] only; used together with `optionalTargetIDArgument` & `optionalNodeIDArgument`
func arg0Node(c *cli.Context) (node *meta.Snode, sname string, err error) {
	if c.NArg() > 0 {
		node, sname, err = getNode(c, c.Args().Get(0))
	}
	return
}

func reorderTailArgs(left string, middle []string, right ...string) string {
	var sb strings.Builder
	sb.WriteString(left)
	sb.WriteByte(' ')
	for _, s := range middle {
		sb.WriteString(s)
		sb.WriteByte(' ')
	}
	for _, s := range right {
		sb.WriteString(s)
		sb.WriteByte(' ')
	}
	return strings.TrimSuffix(sb.String(), " ")
}

func isWebURL(url string) bool { return cos.IsHT(url) || cos.IsHTTPS(url) }

func jsonMarshalIndent(v any) ([]byte, error) { return jsoniter.MarshalIndent(v, "", "    ") }

func findClosestCommand(cmd string, candidates []cli.Command) (result string, distance int) {
	var (
		minDist     = math.MaxInt64
		closestName string
	)
	for i := range candidates {
		dist := DamerauLevenstheinDistance(cmd, candidates[i].Name)
		if dist < minDist {
			minDist = dist
			closestName = candidates[i].Name
		}
	}
	return closestName, minDist
}

func briefPause(seconds time.Duration) {
	time.Sleep(seconds * time.Second) //nolint:durationcheck // false positive
}

func isConfigProp(s string) bool {
	props := configPropList()
	for _, p := range props {
		if p == s || strings.HasPrefix(p, s+cmn.IterFieldNameSepa) {
			return true
		}
	}
	return cos.StringInSlice(s, props)
}

func getPrefixFromPrimary() string {
	scheme, _ := cmn.ParseURLScheme(clusterURL)
	if scheme == "" {
		scheme = "http"
	}
	return scheme + apc.BckProviderSeparator
}

// _refreshRate returns the refresh interval for monitoring operations.
// This is a generic utility that only handles the common --refresh flag.
func _refreshRate(c *cli.Context) time.Duration {
	if flagIsSet(c, refreshFlag) {
		return max(parseDurationFlag(c, refreshFlag), refreshRateMinDur)
	}
	return refreshRateDefault
}

// Users can pass in a comma-separated list
func splitCsv(s string) (lst []string) {
	lst = strings.Split(s, ",")
	for i, val := range lst {
		lst[i] = strings.TrimSpace(val)
	}
	return
}

// Convert a list of "key value" and "key=value" pairs into a map
func makePairs(args []string) (nvs cos.StrKVs, err error) {
	var (
		i  int
		ll = len(args)
	)
	nvs = cos.StrKVs{}
	for i < ll {
		switch {
		case args[i] != keyAndValueSeparator && strings.Contains(args[i], keyAndValueSeparator):
			pairs := strings.SplitN(args[i], keyAndValueSeparator, 2)
			nvs[pairs[0]] = pairs[1]
			i++
		case i < ll-2 && args[i+1] == keyAndValueSeparator:
			nvs[args[i]] = args[i+2]
			i += 3
		case args[i] == feat.PropName && i < ll-1:
			nvs[args[i]] = strings.Join(args[i+1:], ",") // NOTE: only features nothing else in the tail
			return
		case args[i] == confLogModules && i < ll-1:
			nvs[args[i]] = strings.Join(args[i+1:], ",") // NOTE: only smodules nothing else in the tail
			return
		default: // last name without a value
			if i == ll-1 {
				return nil, &errInvalidNVpair{args[i]}
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
	parts = strings.SplitN(c.Args().Get(0), keyAndValueSeparator, 2)
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
	if possibleJSON[0] == '\'' && cos.IsLastB(possibleJSON, '\'') {
		possibleJSON = possibleJSON[1 : len(possibleJSON)-1]
	}
	if len(possibleJSON) >= 2 {
		return possibleJSON[0] == '{' && possibleJSON[len(possibleJSON)-1] == '}'
	}
	return false
}

func parseBucketACL(values []string, idx int) (access apc.AccessAttrs, newIdx int, err error) {
	var (
		acc apc.AccessAttrs
		val uint64
	)
	if len(values) == 0 {
		return access, idx, nil
	}
	// 1: `access GET,PUT`
	if strings.Index(values[idx], ",") > 0 {
		newIdx = idx + 1
		lst := splitCsv(values[idx])
		for _, perm := range lst {
			perm = strings.TrimSpace(perm)
			if perm == "" {
				continue
			}
			if acc, err = apc.StrToAccess(perm); err != nil {
				return access, newIdx, err
			}
			access |= acc
		}
		return access, newIdx, nil
	}

	// 2: direct hexadecimal input, e.g. `access 0x342`
	// 3: `access HEAD`
	for newIdx = idx; newIdx < len(values); {
		if val, err = parseHexOrUint(values[newIdx]); err == nil {
			acc = apc.AccessAttrs(val)
		} else if acc, err = apc.StrToAccess(values[newIdx]); err != nil {
			return access, newIdx, err
		}
		access |= acc
		newIdx++
	}
	return access, newIdx, nil
}

func parseFeatureFlags(values []string, idx int) (res feat.Flags, newIdx int, err error) {
	if len(values) == 0 {
		return 0, idx, nil
	}
	if values[idx] == apc.NilValue {
		return 0, idx + 1, nil
	}
	// 1: parse (comma-separated) feat.Flags.String()
	if strings.Index(values[idx], ",") > 0 {
		newIdx = idx + 1
		lst := splitCsv(values[idx])
		for _, vv := range lst {
			var (
				f feat.Flags
				v = strings.TrimSpace(vv)
			)
			if v == "" {
				continue
			}
			if f, err = feat.CSV2Feat(v); err != nil {
				return res, idx, err
			}
			res = res.Set(f)
		}
		return res, newIdx, nil
	}

	// 2: direct hexadecimal input, e.g. `features 0x342`
	// 3: `features F3`
	for newIdx = idx; newIdx < len(values); {
		var (
			f   feat.Flags
			val uint64
		)
		if val, err = parseHexOrUint(values[newIdx]); err == nil {
			f = feat.Flags(val)
		} else if f, err = feat.CSV2Feat(values[newIdx]); err != nil {
			return res, idx, err
		}
		res = res.Set(f)
		newIdx++
	}
	return res, newIdx, nil
}

// TODO: support `allow` and `deny` verbs/operations on existing access permissions
func makeBckPropPairs(values []string) (nvs cos.StrKVs, err error) {
	var (
		cmd   string
		props = make([]string, 0, 20)
	)
	err = cmn.IterFields(&cmn.BpropsToSet{}, func(tag string, _ cmn.IterField) (error, bool) {
		props = append(props, tag)
		return nil, false
	})
	if err != nil {
		return nil, err
	}

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
		if idx >= len(values) {
			break
		}

		// two special cases: access and feature flags
		if cmd == cmn.PropBucketAccessAttrs {
			var access apc.AccessAttrs
			access, idx, err = parseBucketACL(values, idx)
			if err != nil {
				return nil, err
			}
			nvs[cmd] = access.String() // FormatUint
			cmd = ""
			continue
		}
		if cmd == feat.PropName {
			var features feat.Flags
			features, idx, err = parseFeatureFlags(values, idx)
			if err != nil {
				return nil, err
			}
			nvs[cmd] = features.String() // FormatUint
			cmd = ""
		}
	}

	if cmd != "" {
		return nil, fmt.Errorf("missing property %q value", cmd)
	}
	return nvs, nil
}

func bucketsFromArgsOrEnv(c *cli.Context) ([]cmn.Bck, error) {
	uris := c.Args()
	bcks := make([]cmn.Bck, 0, len(uris))

	for _, bckURI := range uris {
		bck, err := parseBckURI(c, bckURI, false)
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
func headBucket(bck cmn.Bck, dontAddBckMD bool) (p *cmn.Bprops, err error) {
	if p, err = api.HeadBucket(apiBP, bck, dontAddBckMD); err == nil {
		return
	}
	if herr, ok := err.(*cmn.ErrHTTP); ok {
		switch {
		case cliConfVerbose():
			herr.Message = herr.StringEx()
			err = errors.New(herr.Message)
		case herr.Status == http.StatusNotFound:
			err = &errDoesNotExist{what: "bucket", name: bck.Cname("")}
		case herr.Message != "":
			err = errors.New(herr.Message)
		default:
			err = fmt.Errorf("failed to HEAD bucket %q: %s", bck.String(), herr.Message)
		}
	} else {
		msg := strings.ToLower(err.Error())
		if !strings.HasPrefix(msg, "head \"http") && !strings.HasPrefix(msg, "head http") {
			err = fmt.Errorf("failed to HEAD bucket %q: %v", bck.String(), err)
		}
	}
	return
}

func shouldHeadRemote(c *cli.Context, bck cmn.Bck) bool {
	return !bck.IsHT() && !flagIsSet(c, dontHeadRemoteFlag)
}

// Prints multiple lines of fmtStr to writer w.
// For line number i, fmtStr is formatted with values of args at index i
// - if maxLines >= 0 prints at most maxLines
// - otherwise, prints everything until the end of one of the args
func limitedLineWriter(w io.Writer, maxLines int, fmtStr string, args ...[]string) {
	objs := make([]any, 0, len(args))
	if fmtStr == "" || !cos.IsLastB(fmtStr, '\n') {
		fmtStr += "\n"
	}

	if maxLines < 0 {
		maxLines = math.MaxInt64
	}
	minLen := math.MaxInt64
	for _, a := range args {
		minLen = min(minLen, len(a))
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

func bckPropList(props *cmn.Bprops, verbose bool) (propList nvpairList) {
	if !verbose { // i.e., compact
		propList = nvpairList{
			{"created", fmtBucketCreatedTime(props.Created)},
			{"provider", props.Provider},
			{"access", props.Access.Describe(true /*incl. all*/)},
			{"checksum", props.Cksum.String()},
			{"mirror", props.Mirror.String()},
			{"ec", props.EC.String()},
			{"lru", props.LRU.String()},
			{"versioning", props.Versioning.String()},
		}
		if props.Provider == apc.HT {
			origURL := props.Extra.HTTP.OrigURLBck
			if origURL != "" {
				propList = append(propList, nvpair{Name: "original-url", Value: origURL})
			}
		}
	} else {
		err := cmn.IterFields(props, func(tag string, field cmn.IterField) (error, bool) {
			var value string
			switch tag {
			case cmn.PropBucketCreated:
				value = fmtBucketCreatedTime(props.Created)
			case cmn.PropBucketAccessAttrs:
				value = props.Access.Describe(true /*incl. all*/)
			default:
				v := field.Value()
				value = _toStr(v)
			}
			propList = append(propList, nvpair{Name: tag, Value: value})
			return nil, false
		})
		debug.AssertNoErr(err)
	}

	// see also: listBucketsSummHdr & listBucketsSummNoHdr
	if props.BID == 0 {
		propList = append(propList, nvpair{Name: "present", Value: "no"})
	} else {
		propList = append(propList, nvpair{Name: "present", Value: "yes"})
	}

	sort.Slice(propList, func(i, j int) bool {
		return propList[i].Name < propList[j].Name
	})
	return propList
}

func fmtBucketCreatedTime(created int64) string {
	if created == 0 {
		return teb.NotSetVal
	}
	return time.Unix(0, created).Format(time.RFC3339)
}

// compare with teb.isUnsetTime() and fmtBucketCreatedTime() above
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
	fmt.Fprint(c.App.Writer, prompt+": ")
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
func isBucketEmpty(bck cmn.Bck, cached bool) (bool, error) {
	msg := &apc.LsoMsg{}
	if cached {
		msg.SetFlag(apc.LsCached)
	}
	msg.SetFlag(apc.LsNameOnly)
	lst, err := api.ListObjectsPage(apiBP, bck, msg, api.ListArgs{})
	if err != nil {
		return false, V(err)
	}
	return len(lst.Entries) == 0, nil
}

func ensureRemoteProvider(bck cmn.Bck) error {
	if !apc.IsProvider(bck.Provider) {
		return fmt.Errorf("invalid bucket %q: missing backend provider", bck.String())
	}
	if bck.IsRemote() {
		return nil
	}
	if bck.Props == nil {
		// double-take: ais:// bucket with remote backend?
		p, err := headBucket(bck, true)
		if err != nil {
			return err
		}
		bck.Props = p
		if bck.IsRemote() {
			return nil // yes it is
		}
	}
	return fmt.Errorf("invalid bucket %q: expecting remote backend", bck.String())
}

func parseURLtoBck(strURL string) (bck cmn.Bck) {
	if !cos.IsLastB(strURL, '/') {
		strURL += "/"
	}
	bck.Provider = apc.HT
	bck.Name = cmn.OrigURLBck2Name(strURL)
	return
}

// see also authNConfPairs
func flattenJSON(jstruct any, section string) (flat nvpairList) {
	flat = make(nvpairList, 0, 40)
	cmn.IterFields(jstruct, func(tag string, field cmn.IterField) (error, bool) {
		if section == "" || strings.HasPrefix(tag, section) {
			v := _toStr(field.Value())
			flat = append(flat, nvpair{tag, v})
		}
		return nil, false
	})
	return flat
}

func flattenBackends(backends []string) (flat nvpairList) {
	for _, b := range backends {
		nv := nvpair{Name: b}
		switch b {
		case apc.AWS:
			nv.Value = "Amazon S3"
		case apc.GCP:
			nv.Value = "Google Cloud Storage"
		case apc.Azure:
			nv.Value = "Azure Blob Storage"
		case apc.OCI:
			nv.Value = "Oracle Cloud Infrastructure (OCI) Object Storage"
		}
		flat = append(flat, nv)
	}
	return flat
}

// remove secrets, if any
func _toStr(v any) (s string) {
	m, ok := v.(map[string]any)
	if !ok {
		// feature flags: custom formatting
		if f, ok := v.(feat.Flags); ok {
			if f == 0 {
				v = apc.NilValue
			} else {
				v = strings.Join(f.Names(), "\n\t ")
			}
		}
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
	return fmt.Sprintf("%v", m) // for custom formatting, see e.g. AliasConfig.String()
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
				item.Old = teb.NotSetVal
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

// showSectionNotFoundError displays error when a config section is not found
func showSectionNotFoundError(c *cli.Context, section string, config any, helpCmd string) {
	availableSections := extractAvailableSections(config)
	actionWarn(c, fmt.Sprintf("config section %q not found. Available sections: %s",
		section, strings.Join(availableSections, ", ")))
	actionNote(c, helpCmd)
}

// extractAvailableSections extracts root-level configuration sections from any config structure
func extractAvailableSections(config any) []string {
	var availableSections []string
	seen := make(map[string]bool)

	cmn.IterFields(config, func(tag string, _ cmn.IterField) (error, bool) {
		root := strings.Split(tag, ".")[0]
		if !seen[root] {
			availableSections = append(availableSections, root)
			seen[root] = true
		}
		return nil, false
	}, cmn.IterOpts{VisitAll: true})

	sort.Strings(availableSections)
	return availableSections
}

func printSectionJSON(c *cli.Context, in any, section string) bool {
	var result any
	found := false

	if section == "" {
		// Show entire config
		result = in
		found = true
	} else {
		// Find specific section
		cmn.IterFields(in, func(tag string, field cmn.IterField) (error, bool) {
			if tag == section {
				found = true
				result = field.Value()
				return nil, true // Stop after finding exact match
			}
			return nil, false
		}, cmn.IterOpts{VisitAll: true})

		// Wrap the result with the section name as the root key
		if found {
			result = map[string]any{section: result}
		}
	}

	if !found {
		return false
	}

	data, err := jsonMarshalIndent(result)
	if err != nil {
		actionWarn(c, fmt.Sprintf("failed to marshal section %q: %v", section, err))
		return false
	}

	fmt.Fprintln(c.App.Writer, string(data))
	return true
}

// First, request cluster's config from the primary node that contains
// default Cksum type. Second, generate default list of properties.
func defaultBckProps(bck cmn.Bck) (*cmn.Bprops, error) {
	cfg, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return nil, V(err)
	}
	props := bck.DefaultProps(cfg)
	return props, nil
}

// see also flattenJSON
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

// loosely, wildcard or range
func isPattern(ptrn string) bool {
	return strings.Contains(ptrn, "*") || strings.Contains(ptrn, "?") || strings.Contains(ptrn, "\\") ||
		(strings.Contains(ptrn, "{") && strings.Contains(ptrn, "}"))
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

// see related: `verboseWarnings()`

func actionDone(c *cli.Context, msg string) { fmt.Fprintln(c.App.Writer, msg) }
func actionWarn(c *cli.Context, msg string) { fmt.Fprintln(c.App.ErrWriter, fcyan("Warning: ")+msg) }
func actionNote(c *cli.Context, msg string) { fmt.Fprintln(c.App.ErrWriter, fblue("Note: ")+msg) }

func actionX(c *cli.Context, xargs *xact.ArgsMsg, s string) {
	if flagIsSet(c, nonverboseFlag) {
		fmt.Fprintln(c.App.Writer, xargs.ID)
		return
	}
	msg := fmt.Sprintf("Started %s%s. %s", xact.Cname(xargs.Kind, xargs.ID), s, toMonitorMsg(c, xargs.ID, ""))
	actionDone(c, msg)
}

func actionCptn(c *cli.Context, prefix string, msgs ...any) {
	if prefix == "" {
		msgs[0] = fcyan(msgs[0])
		fmt.Fprintln(c.App.Writer, msgs...)
	} else {
		out := make([]any, len(msgs)+1)
		out[0] = fcyan(prefix)
		copy(out[1:], msgs)
		fmt.Fprintln(c.App.Writer, out...)
	}
}

func dryRunHeader() string {
	return fcyan("[DRY RUN]")
}

func dryRunCptn(c *cli.Context) {
	fmt.Fprintln(c.App.Writer, dryRunHeader()+" with no modifications to the cluster")
}

//////////////////////////
// HuggingFace wrappers //
//////////////////////////

// hasHuggingFaceRepoFlags checks if HF model or dataset flags are set
func hasHuggingFaceRepoFlags(c *cli.Context) bool {
	hasModel := flagIsSet(c, hfModelFlag)
	hasDataset := flagIsSet(c, hfDatasetFlag)
	return hf.HasHuggingFaceRepoFlags(hasModel, hasDataset)
}

// buildHuggingFaceURL extracts CLI flags and calls HF package
func buildHuggingFaceURL(c *cli.Context) (string, error) {
	model := parseStrFlag(c, hfModelFlag)
	dataset := parseStrFlag(c, hfDatasetFlag)
	file := parseStrFlag(c, hfFileFlag)
	revision := parseStrFlag(c, hfRevisionFlag)
	return hf.BuildHuggingFaceURL(model, dataset, file, revision)
}

//////////////
// dlSource //
//////////////

func parseDlSource(c *cli.Context, rawURL string) (dlSource, error) {
	var source dlSource
	var err error
	var needHFAuth bool // Check if HuggingFace auth should be added (if available)

	switch {
	case c != nil && hasHuggingFaceRepoFlags(c):
		// Case 1: Using HF convenience flags (--hf-model or --hf-dataset)
		// Example: ais download --hf-model bert-base-uncased --hf-file pytorch_model.bin ais://nnn

		if hf.IsHuggingFaceURL(rawURL) {
			return dlSource{}, fmt.Errorf("cannot use %s or %s flags with direct HuggingFace URL; use flags OR direct URL, not both",
				qflprn(hfModelFlag), qflprn(hfDatasetFlag))
		}

		hfURL, err := buildHuggingFaceURL(c)
		if err != nil {
			return dlSource{}, err
		}
		source, err = parseURLToSource(hfURL)
		if err != nil {
			return dlSource{}, err
		}
		needHFAuth = true

	default:
		// Direct URL case
		source, err = parseURLToSource(rawURL)
		if err != nil {
			return dlSource{}, err
		}
		needHFAuth = hf.IsHuggingFaceURL(rawURL) // Only HF-related if direct URL is HF
	}

	// Add HuggingFace auth header if this is HF-related AND auth is available
	if needHFAuth {
		if token := parseStrFlag(c, hfAuthFlag); token != "" {
			source.headers = http.Header{apc.HdrAuthorization: []string{apc.AuthenticationTypeBearer + " " + token}}
		}
	}

	return source, nil
}

// parseURLToSource handles the actual URL parsing logic
func parseURLToSource(rawURL string) (dlSource, error) {
	// Check for HuggingFace full repository download marker
	if strings.HasPrefix(rawURL, hf.HfFullRepoMarker) {
		// HuggingFace dataset download - pass marker to job handler
		return dlSource{link: rawURL}, nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return dlSource{}, err
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

		scheme = apc.DefaultScheme
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
	case apc.OCIScheme, apc.OCI:
		cloudSource = dlSourceBackend{
			bck:    cmn.Bck{Name: host, Provider: apc.OCI},
			prefix: strings.TrimPrefix(fullPath, "/"),
		}
	case apc.AISScheme:
		// TODO: add support for the remote cluster
		scheme = "http" // TODO: How about `https://`?
		if !strings.Contains(host, ":") {
			host += ":8080" // TODO: What if host is listening on `:80` so we don't need port?
		}
		fullPath = path.Join(apc.Version, apc.Objects, fullPath)
	case "":
		scheme = apc.DefaultScheme
	case apc.DefaultScheme, "http":
	default:
		return dlSource{}, fmt.Errorf("invalid scheme: %s", scheme)
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

//////////////////////
// errInvalidNVpair //
//////////////////////

func (e *errInvalidNVpair) Error() string { return fmt.Sprintf("invalid key=value pair %q", e.notpair) }

func openFileOrURL(source string) (io.ReadCloser, error) {
	if isWebURL(source) {
		// Download from HTTP URL
		resp, err := http.Get(source) //nolint:noctx // want to use http.NewRequest and default client
		if err != nil {
			return nil, fmt.Errorf("failed to download from %q: %v", source, err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d error downloading from %q", resp.StatusCode, source)
		}

		return resp.Body, nil
	}

	// Read from local file
	return os.Open(source)
}
