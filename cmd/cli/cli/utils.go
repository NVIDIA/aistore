// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
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
)

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

func errMisplacedFlag(c *cli.Context, arg string) (err error) {
	if len(arg) > 1 && arg[0] == '-' {
		err = incorrectUsageMsg(c, "missing command line argument (hint: flag '%s' misplaced?)", arg)
	}
	return err
}

func isWebURL(url string) bool { return cos.IsHTTP(url) || cos.IsHTTPS(url) }

func jsonMarshalIndent(v any) ([]byte, error) { return jsoniter.MarshalIndent(v, "", "    ") }

func helpMessage(template string, data any) string {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	// Execute the template that generates command usage text
	cli.HelpPrinterCustom(w, template, data, teb.HelpTemplateFuncMap)
	_ = w.Flush()

	return buf.String()
}

func findClosestCommand(cmd string, candidates []cli.Command) (result string, distance int) {
	var (
		minDist     = math.MaxInt64
		closestName string
	)
	for i := 0; i < len(candidates); i++ {
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

// Get config from a random target.
func getRandTargetConfig(c *cli.Context) (*cmn.Config, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}
	tsi, err := smap.GetRandTarget()
	if err != nil {
		return nil, err
	}
	cfg, err := api.GetDaemonConfig(apiBP, tsi)
	if err != nil {
		return nil, V(err)
	}
	return cfg, err
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

func _refreshRate(c *cli.Context) time.Duration {
	refreshRate := refreshRateDefault
	if flagIsSet(c, refreshFlag) {
		duration := parseDurationFlag(c, refreshFlag)
		refreshRate = max(duration, refreshRateMinDur)
	}
	return refreshRate // aka sleep
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
	if possibleJSON[0] == '\'' && possibleJSON[len(possibleJSON)-1] == '\'' {
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
		return
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
		return
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
	return
}

func parseBpropsFromContext(c *cli.Context) (props *cmn.BpropsToSet, err error) {
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

	props, err = cmn.NewBpropsToSet(nvs)
	return
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
			err = fmt.Errorf("failed to HEAD bucket %q: %s", bck, herr.Message)
		}
	} else {
		msg := strings.ToLower(err.Error())
		if !strings.HasPrefix(msg, "head \"http") && !strings.HasPrefix(msg, "head http") {
			err = fmt.Errorf("failed to HEAD bucket %q: %v", bck, err)
		}
	}
	return
}

// Prints multiple lines of fmtStr to writer w.
// For line number i, fmtStr is formatted with values of args at index i
// - if maxLines >= 0 prints at most maxLines
// - otherwise, prints everything until the end of one of the args
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
			case cmn.PropBucketCreated:
				value = fmtBucketCreatedTime(props.Created)
			case cmn.PropBucketAccessAttrs:
				value = props.Access.Describe(true /*incl. all*/)
			default:
				value = fmt.Sprintf("%v", field.Value())
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
	return
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
func isBucketEmpty(bck cmn.Bck, cached bool) (bool, error) {
	msg := &apc.LsoMsg{}
	if cached {
		msg.SetFlag(apc.LsObjCached)
	}
	msg.SetFlag(apc.LsNameOnly)
	objList, err := api.ListObjectsPage(apiBP, bck, msg)
	if err != nil {
		return false, V(err)
	}
	return len(objList.Entries) == 0, nil
}

func ensureRemoteProvider(bck cmn.Bck) error {
	if !apc.IsProvider(bck.Provider) {
		return fmt.Errorf("invalid bucket %q: missing backend provider", bck)
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
	return fmt.Errorf("invalid bucket %q: expecting remote backend", bck)
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
	out, err := jsonMarshalIndent(in)
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

func actionCptn(c *cli.Context, prefix, msg string) {
	if prefix == "" {
		fmt.Fprintln(c.App.Writer, fcyan(msg))
	} else {
		fmt.Fprintln(c.App.Writer, fcyan(prefix)+msg)
	}
}

func dryRunHeader() string {
	return fcyan("[DRY RUN]")
}

func dryRunCptn(c *cli.Context) {
	fmt.Fprintln(c.App.Writer, dryRunHeader()+" with no modifications to the cluster")
}

//////////////
// dlSource //
//////////////

type (
	dlSourceBackend struct {
		bck    cmn.Bck
		prefix string
	}
	dlSource struct {
		link    string
		backend dlSourceBackend
	}
)

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

//////////////////////
// errInvalidNVpair //
//////////////////////

func (e *errInvalidNVpair) Error() string { return fmt.Sprintf("invalid key=value pair %q", e.notpair) }
