// Package templates provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package templates

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"k8s.io/apimachinery/pkg/util/duration"
)

// Templates for output
// ** Changing the structure of the objects server side needs to make sure that this will still work **
const (
	primarySuffix      = "[P]"
	nonElectableSuffix = "[-]"

	// Smap
	SmapHeader = "NODE\t TYPE\t PUBLIC URL" +
		"{{ if (eq $.ExtendedURLs true) }}\t INTRA CONTROL URL\t INTRA DATA URL{{end}}" +
		"\n"
	SmapBody = "{{FormatDaemonID $value.ID $.Smap}}\t {{$value.DaemonType}}\t {{$value.PublicNet.DirectURL}}" +
		"{{ if (eq $.ExtendedURLs true) }}\t {{$value.IntraControlNet.DirectURL}}\t {{$value.IntraDataNet.DirectURL}}{{end}}" +
		"\n"

	SmapTmpl = SmapHeader +
		"{{ range $key, $value := .Smap.Pmap }}" + SmapBody + "{{end}}\n" +
		SmapHeader +
		"{{ range $key, $value := .Smap.Tmap }}" + SmapBody + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $si := .Smap.Pmap }} " +
		"{{ $nonElect := $.Smap.NonElectable $si }}" +
		"{{ if (eq $nonElect true) }} ProxyID: {{$key}}\n{{end}}{{end}}\n" +
		"Primary Proxy: {{.Smap.Primary.ID}}\nProxies: {{len .Smap.Pmap}}\t Targets: {{len .Smap.Tmap}}\t Smap Version: {{.Smap.Version}}\n"

	//////////////////
	// Cluster info //
	//////////////////

	ClusterSummary = "Summary:\n Proxies:\t{{len .Smap.Pmap}} ({{ .Smap.CountNonElectable }} unelectable)\n " +
		"Targets:\t{{len .Smap.Tmap}}\n Primary Proxy:\t{{.Smap.Primary.ID}}\n Smap Version:\t{{.Smap.Version}}\n " +
		"Deployment:\t{{ ( Deployments .Status) }}\n"

	// Disk Stats
	DiskStatsHeader = "TARGET\t DISK\t READ\t WRITE\t UTIL %\n"

	DiskStatsBody = "{{ $value.TargetID }}\t " +
		"{{ $value.DiskName }}\t " +
		"{{ $stat := $value.Stat }}" +
		"{{ FormatBytesSigned $stat.RBps 2 }}/s\t " +
		"{{ FormatBytesSigned $stat.WBps 2 }}/s\t " +
		"{{ $stat.Util }}%\n"

	DiskStatBodyTmpl  = "{{ range $key, $value := . }}" + DiskStatsBody + "{{ end }}"
	DiskStatsFullTmpl = DiskStatsHeader + DiskStatBodyTmpl

	// Config
	ConfigTmpl = "PROPERTY\t VALUE\n{{range $item := .}}" +
		"{{ $item.Name }}\t {{ $item.Value }}\n" +
		"{{end}}\n"

	DaemonConfigTmpl = "{{ if .ClusterConfig }}PROPERTY\t VALUE\t DEFAULT\n{{range $item := .ClusterConfig }}" +
		"{{ $item.Name }}\t {{ $item.Current }}\t {{ $item.Old }}\n" +
		"{{end}}\n{{end}}" +
		"{{ if .LocalConfig }}PROPERTY\t VALUE\n" +
		"{{range $item := .LocalConfig }}" +
		"{{ $item.Name }}\t {{ $item.Value }}\n" +
		"{{end}}\n{{end}}"

	PropsSimpleTmpl = "PROPERTY\t VALUE\n" +
		"{{range $p := . }}" +
		"{{$p.Name}}\t {{$p.Value}}\n" +
		"{{end}}"

	DownloadListHeader = "JOB ID\t STATUS\t ERRORS\t DESCRIPTION\n"
	DownloadListBody   = "{{$value.ID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else}}{{if $value.JobFinished}}Finished{{else}}{{$value.PendingCnt}} pending{{end}}" +
		"{{end}}\t {{$value.ErrorCnt}}\t {{$value.Description}}\n"
	DownloadListTmpl = DownloadListHeader + "{{ range $key, $value := . }}" + DownloadListBody + "{{end}}"

	DSortListHeader = "JOB ID\t STATUS\t START\t FINISH\t DESCRIPTION\n"
	DSortListBody   = "{{$value.ID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else if $value.Archived}}Finished" +
		"{{else}}Running" +
		"{{end}}\t {{FormatTime $value.StartedTime}}\t {{FormatTime $value.FinishTime}} \t {{$value.Description}}\n"
	DSortListTmpl = DSortListHeader + "{{ range $value := . }}" + DSortListBody + "{{end}}"

	// Xactions templates
	XactionsBodyTmpl     = XactionsBaseBodyTmpl + XactionsExtBodyTmpl
	XactionsBaseBodyTmpl = XactionStatsHeader +
		"{{range $daemon := $.Stats }}" + XactionBody + "{{end}}"
	XactionStatsHeader = "NODE\t ID\t KIND\t BUCKET\t OBJECTS\t BYTES\t START\t END\t ABORTED\n"
	XactionBody        = "{{range $key, $xact := $daemon.Stats}}" + XactionStatsBody + "{{end}}" +
		"{{if $daemon.Stats}}\t \t \t \t \t \t \t \t{{if $.Verbose}} \t {{end}}\n{{end}}"
	XactionStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xact.IDX}}{{$xact.IDX}}{{else}}-{{end}}\t " +
		"{{$xact.KindX}}\t " +
		"{{if $xact.BckX.Name}}{{$xact.BckX.Name}}{{else}}-{{end}}\t " +
		"{{if (eq $xact.ObjCountX 0) }}-{{else}}{{$xact.ObjCountX}}{{end}}\t " +
		"{{if (eq $xact.BytesCountX 0) }}-{{else}}{{FormatBytesSigned $xact.BytesCountX 2}}{{end}}\t " +
		"{{FormatTime $xact.StartTimeX}}\t " +
		"{{if (IsUnsetTime $xact.EndTimeX)}}-{{else}}{{FormatTime $xact.EndTimeX}}{{end}}\t " +
		"{{$xact.AbortedX}}\n"
	XactionsExtBodyTmpl = "{{if $.Verbose }}" + // if not nil
		"\n{{range $daemon := $.Stats }}" +
		"{{if $daemon.Stats}}NODE\t {{$daemon.DaemonID}}\n" +
		"{{range $key, $xact := $daemon.Stats}}" +
		"{{range $name,$val := $xact.Ext}}{{ $name }}\t {{$val}}\n{{end}}" +
		"{{end}}\n" +
		"{{end}}" +
		"{{end}}{{end}}"

	XactionECGetStatsHeader = "NODE\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t START\t END\t ABORTED\n"
	XactionECGetBodyTmpl    = XactionECGetStatsHeader +
		"{{range $daemon := $.Stats }}" + XactionECGetBody + "{{end}}"
	XactionECGetBody      = "{{range $key, $xact := $daemon.Stats}}" + XactionECGetStatsBody + "{{end}}"
	XactionECGetStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xact.BckX.Name}}{{$xact.BckX.Name}}{{else}}-{{end}}\t " +
		"{{if (eq $xact.ObjCountX 0) }}-{{else}}{{$xact.ObjCountX}}{{end}}\t " +
		"{{if (eq $xact.BytesCountX 0) }}-{{else}}{{FormatBytesSigned $xact.BytesCountX 2}}{{end}}\t " +

		"{{ $ext := ExtECGetStats $xact }}" +
		"{{if (eq $ext.ErrCount 0) }}-{{else}}{{$ext.ErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +

		"{{FormatTime $xact.StartTimeX}}\t " +
		"{{if (IsUnsetTime $xact.EndTimeX)}}-{{else}}{{FormatTime $xact.EndTimeX}}{{end}}\t " +
		"{{$xact.AbortedX}}\n"

	XactionECPutStatsHeader = "NODE\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t ENC TIME\t START\t END\t ABORTED\n"
	XactionECPutBodyTmpl    = XactionECPutStatsHeader +
		"{{range $daemon := $.Stats }}" + XactionECPutBody + "{{end}}"
	XactionECPutBody      = "{{range $key, $xact := $daemon.Stats}}" + XactionECPutStatsBody + "{{end}}"
	XactionECPutStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xact.BckX.Name}}{{$xact.BckX.Name}}{{else}}-{{end}}\t " +
		"{{if (eq $xact.ObjCountX 0) }}-{{else}}{{$xact.ObjCountX}}{{end}}\t " +
		"{{if (eq $xact.BytesCountX 0) }}-{{else}}{{FormatBytesSigned $xact.BytesCountX 2}}{{end}}\t " +

		"{{ $ext := ExtECPutStats $xact }}" +
		"{{if (eq $ext.EncodeErrCount 0) }}-{{else}}{{$ext.EncodeErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +
		"{{if (eq $ext.AvgEncodeTime 0) }}-{{else}}{{FormatMilli $ext.AvgEncodeTime}}{{end}}\t " +

		"{{FormatTime $xact.StartTimeX}}\t " +
		"{{if (IsUnsetTime $xact.EndTimeX)}}-{{else}}{{FormatTime $xact.EndTimeX}}{{end}}\t " +
		"{{$xact.AbortedX}}\n"

	// Buckets templates
	BucketsSummariesFastTmpl = "NAME\t EST. OBJECTS\t EST. SIZE\t EST. USED %\n" + bucketsSummariesBody
	BucketsSummariesTmpl     = "NAME\t OBJECTS\t SIZE \t USED %\n" + bucketsSummariesBody
	bucketsSummariesBody     = "{{range $k, $v := . }}" +
		"{{$v.Bck}}\t {{$v.ObjCount}}\t {{FormatBytesUnsigned $v.Size 2}}\t {{FormatFloat $v.UsedPct}}%\n" +
		"{{end}}"

	// Bucket summary validate templates
	BucketSummaryValidateTmpl = "BUCKET\t OBJECTS\t MISPLACED\t MISSING COPIES\n" + bucketSummaryValidateBody
	bucketSummaryValidateBody = "{{range $v := . }}" +
		"{{$v.Name}}\t {{$v.ObjectCnt}}\t {{$v.Misplaced}}\t {{$v.MissingCopies}}\n" +
		"{{end}}"

	// For `object put` mass uploader. A caller adds to the template
	// total count and size. That is why the template ends with \t
	ExtensionTmpl = "Files to upload:\nEXTENSION\t COUNT\t SIZE\n" +
		"{{range $k, $v := . }}" +
		"{{$k}}\t {{$v.Cnt}}\t {{FormatBytesSigned $v.Size 2}}\n" +
		"{{end}}" +
		"TOTAL\t"

	ShortUsageTmpl = "{{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}}{{if .VisibleFlags}} [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{end}} - {{.Usage}}\n" +
		"\n\tCOMMANDS:\t" +
		"{{range .VisibleCategories}}" +
		"{{ range $index, $element := .VisibleCommands}}" +
		"{{if $index}}, {{end}}" +
		"{{if ( eq ( Mod $index 13 ) 12 ) }}\n\t\t{{end}}" + // limit the number printed per line
		"{{$element.Name}}" +
		"{{if ( eq $element.Name \"search\" ) }}\n\t\t{{end}}" + // circumvent $index wrap around for aliases
		"{{end}}{{end}}\n" +
		"{{if .VisibleFlags}}\tOPTIONS:\t" +
		"{{ range $index, $flag := .VisibleFlags}}" +
		"{{if $index}}, {{end}}" +
		"--{{FlagName $flag }}" +
		"{{end}}{{end}}\n"

	AuthNClusterTmpl = "CLUSTER ID\tALIAS\tURLs\n" +
		"{{ range $clu := . }}" +
		"{{ $clu.ID }}\t{{ $clu.Alias }}\t{{ JoinList $clu.URLs }}\n" +
		"{{end}}"

	AuthNRoleTmpl = "ROLE\tDESCRIPTION\n" +
		"{{ range $role := . }}" +
		"{{ $role.Name }}\t{{ $role.Desc }}\n" +
		"{{end}}"

	AuthNUserTmpl = "NAME\tROLES\n" +
		"{{ range $user := . }}" +
		"{{ $user.ID }}\t{{ JoinList $user.Roles }}\n" +
		"{{end}}"

	AuthNUserVerboseTmpl = "Name\t{{ .ID }}\n" +
		"Roles\t{{ JoinList .Roles }}\n" +
		"{{ if ne (len .Clusters) 0 }}" +
		"CLUSTER ID\tALIAS\tPERMISSIONS\n" +
		"{{ range $clu := .Clusters}}" +
		"{{ $clu.ID}}\t{{ $clu.Alias }}\t{{ FormatACL $clu.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ if ne (len .Buckets) 0 }}" +
		"BUCKET\tPERMISSIONS\n" +
		"{{ range $bck := .Buckets}}" +
		"{{ $bck }}\t{{ FormatACL $bck.Access }}\n" +
		"{{end}}{{end}}"

	AuthNRoleVerboseTmpl = "Role\t{{ .Name }}\n" +
		"Description\t{{ .Desc }}\n" +
		"{{ if ne (len .Roles) 0 }}" +
		"Roles\t{{ JoinList .Roles }}\n" +
		"{{ end }}" +
		"{{ if ne (len .Clusters) 0 }}" +
		"CLUSTER ID\tALIAS\tPERMISSIONS\n" +
		"{{ range $clu := .Clusters}}" +
		"{{ $clu.ID}}\t{{ $clu.Alias }}\t{{ FormatACL $clu.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ if ne (len .Buckets) 0 }}" +
		"BUCKET\tPERMISSIONS\n" +
		"{{ range $bck := .Buckets}}" +
		"{{ $bck }}\t{{ FormatACL $bck.Access }}\n" +
		"{{end}}{{end}}"

	// Command `search`
	SearchTmpl = "{{ JoinListNL . }}\n"

	// Command `transform`
	TransformListTmpl = "ID\n" +
		"{{range $transform := .}}" +
		"{{$transform.ID}}\n" +
		"{{end}}"

	// Command `show mountpath`
	TargetMpathListTmpl = "{{range $p := . }}" +
		"{{ $p.DaemonID }}\n" +
		"{{if and (eq (len $p.Mpl.Available) 0) (eq (len $p.Mpl.Disabled) 0)}}" +
		"\tNo mountpaths\n" +
		"{{else}}" +
		"{{if ne (len $p.Mpl.Available) 0}}" +
		"\tAvailable:\n" +
		"{{range $mp := $p.Mpl.Available }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{if ne (len $p.Mpl.Disabled) 0}}" +
		"\tDisabled:\n" +
		"{{range $mp := $p.Mpl.Disabled }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{if ne (len $p.Mpl.WaitingDD) 0}}" +
		"\tTransitioning to disabled or detached pending resilver:\n" +
		"{{range $mp := $p.Mpl.WaitingDD }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{end}}{{end}}"
)

var (
	// ObjectPropsMap matches BucketEntry field
	ObjectPropsMap = map[string]string{
		"name":       "{{FormatNameArch $obj.Name $obj.Flags}}",
		"size":       "{{FormatBytesSigned $obj.Size 2}}",
		"checksum":   "{{$obj.Checksum}}",
		"type":       "{{$obj.Type}}",
		"atime":      "{{$obj.Atime}}",
		"version":    "{{$obj.Version}}",
		"target_url": "{{$obj.TargetURL}}",
		"status":     "{{FormatObjStatus $obj}}",
		"copies":     "{{$obj.Copies}}",
		"cached":     "{{FormatObjIsCached $obj}}",
	}

	ObjStatMap = map[string]string{
		"name":     "{{.Bck.String}}/{{.Name}}",
		"cached":   "{{FormatBool .Present}}",
		"size":     "{{FormatBytesSigned .Size 2}}",
		"version":  "{{.Version}}",
		"custom":   "{{.Custom}}",
		"atime":    "{{if (eq .Atime 0)}}-{{else}}{{FormatUnixNano .Atime}}{{end}}",
		"copies":   "{{if .NumCopies}}{{.NumCopies}}{{else}}-{{end}}",
		"checksum": "{{if .Checksum.Value}}{{.Checksum.Value}}{{else}}-{{end}}",
		"ec":       "{{if (eq .DataSlices 0)}}-{{else}}{{FormatEC .Generation .DataSlices .ParitySlices .IsECCopy}}{{end}}",
	}

	funcMap = template.FuncMap{
		"FormatBytesSigned":   cos.B2S,
		"FormatBytesUnsigned": cos.UnsignedB2S,
		"IsUnsetTime":         isUnsetTime,
		"FormatTime":          fmtTime,
		"FormatUnixNano":      func(t int64) string { return cos.FormatUnixNano(t, "") },
		"FormatEC":            FmtEC,
		"FormatDur":           fmtDuration,
		"FormatObjStatus":     fmtObjStatus,
		"FormatObjIsCached":   fmtObjIsCached,
		"FormatDaemonID":      fmtDaemonID,
		"FormatFloat":         func(f float64) string { return fmt.Sprintf("%.2f", f) },
		"FormatBool":          FmtBool,
		"FormatMilli":         fmtMilli,
		"JoinList":            fmtStringList,
		"JoinListNL":          func(lst []string) string { return fmtStringListGeneric(lst, "\n") },
		"FormatFeatureFlags":  fmtFeatureFlags,
		"Deployments":         func(h DaemonStatusTemplateHelper) string { return strings.Join(h.Deployments().ToSlice(), ",") },
		"FormatACL":           fmtACL,
		"ExtECGetStats":       extECGetStats,
		"ExtECPutStats":       extECPutStats,
		"FormatNameArch":      fmtNameArch,
	}

	AliasTemplate = "ALIAS\tCOMMAND\n{{range $alias, $command := .}}" +
		"{{ $alias }}\t{{ $command }}\n" +
		"{{end}}"

	HelpTemplateFuncMap = template.FuncMap{
		"FlagName": func(f cli.Flag) string { return strings.SplitN(f.GetName(), ",", 2)[0] },
		"Mod":      func(a, mod int) int { return a % mod },
	}
)

type (
	// Used to return specific fields/objects for marshaling (MarshalIdent).
	forMarshaler interface {
		forMarshal() interface{}
	}
	DiskStatsTemplateHelper struct {
		TargetID string
		DiskName string
		Stat     ios.DiskStats
	}
	SmapTemplateHelper struct {
		Smap         *cluster.Smap
		ExtendedURLs bool
	}
	DaemonStatusTemplateHelper struct {
		Pmap map[string]*stats.DaemonStatus `json:"pmap"`
		Tmap map[string]*stats.DaemonStatus `json:"tmap"`
	}
	StatusTemplateHelper struct {
		Smap   *cluster.Smap              `json:"smap"`
		Status DaemonStatusTemplateHelper `json:"status"`
	}
)

// interface guard
var _ forMarshaler = SmapTemplateHelper{}

func (sth SmapTemplateHelper) forMarshal() interface{} {
	return sth.Smap
}

// Gets the associated value from CoreStats
func extractStat(daemon *stats.CoreStats, statName string) int64 {
	if daemon == nil {
		return 0
	}
	return daemon.Tracker[statName].Value
}

func allNodesOnline(stats map[string]*stats.DaemonStatus) bool {
	for _, stat := range stats {
		if stat.Status != api.StatusOnline {
			return false
		}
	}
	return true
}

func calcCapPercentage(daemon *stats.DaemonStatus) (total float64) {
	for _, fs := range daemon.Capacity {
		total += float64(fs.PctUsed)
	}

	if len(daemon.Capacity) == 0 {
		return 0
	}
	return total / float64(len(daemon.Capacity))
}

func calcCap(daemon *stats.DaemonStatus) (total uint64) {
	for _, fs := range daemon.Capacity {
		total += fs.Avail
	}
	return total
}

func fmtXactStatus(tStatus *stats.TargetStatus) string {
	if tStatus == nil || tStatus.RebalanceStats == nil {
		return "-"
	}

	tStats := tStatus.RebalanceStats
	if tStats.Aborted() {
		return "aborted"
	}
	if tStats.EndTime().IsZero() {
		return "running"
	}
	return "finished"
}

func fmtObjStatus(obj *cmn.BucketEntry) string {
	if obj.IsStatusOK() {
		return "ok"
	}
	return "moved"
}

var ConfigSectionTmpl = []string{
	"global", "mirror", "log", "client", "periodic", "timeout", "proxy",
	"lru", "disk", "rebalance", "checksum", "versioning", "fspath",
	"testfs", "network", "fshc", "auth", "keepalive", "downloader",
	cmn.DSortNameLowercase, "compression", "ec", "replication",
}

func fmtObjIsCached(obj *cmn.BucketEntry) string {
	return FmtBool(obj.CheckExists())
}

// FmtBool returns "yes" if true, else "no"
func FmtBool(t bool) string {
	if t {
		return "yes"
	}
	return "no"
}

func isUnsetTime(t time.Time) bool {
	return t.IsZero()
}

func fmtTime(t time.Time) string {
	return t.Format("01-02 15:04:05")
}

// FmtChecksum formats a checksum into a string, where nil becomes "-"
func FmtChecksum(checksum string) string {
	if checksum != "" {
		return checksum
	}
	return "-"
}

// FmtCopies formats an int to a string, where 0 becomes "-"
func FmtCopies(copies int) string {
	if copies == 0 {
		return "-"
	}
	return fmt.Sprint(copies)
}

// FmtEC formats EC data (DataSlices, ParitySlices, IsECCopy) into a
// readable string for CLI, e.g. "1:2[encoded]"
func FmtEC(gen int64, data, parity int, isCopy bool) string {
	if data == 0 {
		return "-"
	}
	info := fmt.Sprintf("%d:%d (gen %d)", data, parity, gen)
	if isCopy {
		info += "[replicated]"
	} else {
		info += "[encoded]"
	}
	return info
}

func fmtDuration(ns int64) string { return duration.HumanDuration(time.Duration(ns)) }

func fmtDaemonID(id string, smap cluster.Smap) string {
	si := smap.GetNode(id)
	if id == smap.Primary.ID() {
		return id + primarySuffix
	}
	if smap.NonElectable(si) {
		return id + nonElectableSuffix
	}
	return id
}

// Displays the output in either JSON or tabular form
// if formatJSON == true, outputTemplate is omitted
func DisplayOutput(object interface{}, writer io.Writer, outputTemplate string, formatJSON ...bool) error {
	useJSON := false
	if len(formatJSON) > 0 {
		useJSON = formatJSON[0]
	}

	if useJSON {
		if o, ok := object.(forMarshaler); ok {
			object = o.forMarshal()
		}
		out, err := jsoniter.MarshalIndent(object, "", "    ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(writer, string(out))
		return err
	}

	// Template
	tmpl, err := template.New("DisplayTemplate").Funcs(funcMap).Parse(outputTemplate)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(writer, 0, 8, 1, '\t', 0)
	if err := tmpl.Execute(w, object); err != nil {
		return err
	}

	return w.Flush()
}

func fmtStringList(lst []string) string {
	if len(lst) == 0 {
		return "-"
	}
	return fmtStringListGeneric(lst, ",")
}

func fmtStringListGeneric(lst []string, sep string) string {
	var s strings.Builder
	for idx, url := range lst {
		if idx != 0 {
			fmt.Fprint(&s, sep)
		}
		fmt.Fprint(&s, url)
	}
	return s.String()
}

func fmtFeatureFlags(flags cmn.FeatureFlags) string {
	if flags == 0 {
		return "-"
	}
	return fmt.Sprintf("%s(%s)", flags, flags.Describe())
}

func daemonsDeployments(ds map[string]*stats.DaemonStatus) cos.StringSet {
	deployments := cos.NewStringSet()
	for _, s := range ds {
		deployments.Add(s.DeployedOn)
	}
	return deployments
}

func (h *DaemonStatusTemplateHelper) Deployments() cos.StringSet {
	p := daemonsDeployments(h.Pmap)
	p.Add(daemonsDeployments(h.Tmap).ToSlice()...)
	return p
}

func fmtACL(acl cmn.AccessAttrs) string {
	if acl == 0 {
		return "-"
	}
	return acl.Describe()
}

func extECGetStats(base *xaction.BaseStatsExt) *ec.ExtECGetStats {
	ecGet := &ec.ExtECGetStats{}
	if err := cos.MorphMarshal(base.Ext, ecGet); err != nil {
		return &ec.ExtECGetStats{}
	}
	return ecGet
}

func extECPutStats(base *xaction.BaseStatsExt) *ec.ExtECPutStats {
	ecPut := &ec.ExtECPutStats{}
	if err := cos.MorphMarshal(base.Ext, ecPut); err != nil {
		return &ec.ExtECPutStats{}
	}
	return ecPut
}

func fmtMilli(val cos.Duration) string {
	return cos.FormatMilli(time.Duration(val))
}

func fmtNameArch(val string, flags uint16) string {
	if flags&cmn.EntryInArch == 0 {
		return val
	}
	return "    " + val
}
