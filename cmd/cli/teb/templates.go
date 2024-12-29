// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

//
// static templates (compare with daeclu and other users/usages of teb/table)
//

const (
	// Smap
	smapHdr = "NODE\t TYPE\t PUBLIC URL" +
		"{{ if (eq $.ExtendedURLs true) }}\t INTRA CONTROL URL\t INTRA DATA URL{{end}}" +
		"\n"
	smapNode = "{{FormatDaemonID $value.ID $.Smap \"\"}}\t {{$value.DaeType}}\t {{$value.PubNet.URL}}" +
		"{{ if (eq $.ExtendedURLs true) }}\t {{$value.ControlNet.URL}}\t {{$value.DataNet.URL}}{{end}}" +
		"\n"

	SmapTmpl = smapHdr + "{{ range $key, $value := .Smap.Pmap }}" + smapNode + "{{end}}\n" +
		smapHdr + smapBody

	SmapTmplNoHdr = "{{ range $key, $value := .Smap.Pmap }}" + smapNode + "{{end}}\n" + smapBody

	smapBody = "{{ range $key, $value := .Smap.Tmap }}" + smapNode + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $si := .Smap.Pmap }} " +
		"{{ $nonElect := $.Smap.NonElectable $si }}" +
		"{{ if (eq $nonElect true) }} ProxyID: {{$key}}\n{{end}}{{end}}\n" +
		"Primary Proxy:\t{{.Smap.Primary.ID}}\n" +
		"Summary:\tproxies({{len .Smap.Pmap}}), targets({{len .Smap.Tmap}}), cluster map(v{{.Smap.Version}}), cluster ID(\"{{.Smap.UUID}}\")\n"

	//
	// Cluster
	// TODO: consider showing `err.io.get.n` counters - generally, all metrics that are stats.IsIOErrMetric()
	//
	indent1 = "   "

	ClusterSummary = indent1 + "Proxies:\t{{FormatProxiesSumm .Smap}}\n" +
		indent1 + "Targets:\t{{FormatTargetsSumm .Smap .NumDisks}}\n" +
		indent1 + "Capacity:\t{{.Capacity}}\n" +
		indent1 + "Cluster Map:\t{{FormatSmap .Smap}}\n" +
		indent1 + "Software:\t{{FormatCluSoft .Version .BuildTime}}\n" +
		indent1 + "Deployment:\t{{ ( Deployments .Stst) }}\n" +
		indent1 + "Status:\t{{ ( OnlineStatus .Stst) }}\n" +
		indent1 + "Rebalance:\t{{FormatRebalance .Stst .CluConfig}}\n" +
		indent1 + "Authentication:\t{{if .CluConfig.Auth.Enabled}}enabled{{else}}disabled{{end}}\n" +
		indent1 + "Version:\t{{ ( Versions .Stst) }}\n" +
		indent1 + "Build:\t{{ ( BuildTimes .Stst) }}\n"

	// Config
	DaemonConfigTmpl = "{{ if .ClusterConfigDiff }}PROPERTY\t VALUE\t DEFAULT\n{{range $item := .ClusterConfigDiff }}" +
		"{{ $item.Name }}\t {{ $item.Current }}\t {{ $item.Old }}\n" +
		"{{end}}\n{{end}}" +
		"{{ if .LocalConfigPairs }}PROPERTY\t VALUE\n" +
		"{{range $item := .LocalConfigPairs }}" +
		"{{ $item.Name }}\t {{ $item.Value }}\n" +
		"{{end}}\n{{end}}"

	// generic prop/val (name/val, key/val)
	propValTmplHdr   = "PROPERTY\t VALUE\n"
	PropValTmpl      = propValTmplHdr + PropValTmplNoHdr
	PropValTmplNoHdr = "{{range $p := . }}" + "{{$p.Name}}\t {{$p.Value}}\n" + "{{end}}"

	//
	// special xactions & dsort
	//

	downloadListHdr  = "JOB ID\t XACTION\t STATUS\t ERRORS\t DESCRIPTION\n"
	downloadListBody = "{{$value.ID}}\t " +
		"{{$value.XactID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else}}{{if $value.JobFinished}}Finished{{else}}{{$value.PendingCnt}} pending{{end}}" +
		"{{end}}\t {{$value.ErrorCnt}}\t {{$value.Description}}\n"
	DownloadListNoHdrTmpl = "{{ range $key, $value := . }}" + downloadListBody + "{{end}}"
	DownloadListTmpl      = downloadListHdr + DownloadListNoHdrTmpl

	dsortListHdr  = "JOB ID\t STATUS\t START\t FINISH\t SRC BUCKET\t DST BUCKET\t SRC SHARDS\n"
	dsortListBody = "{{$value.ID}}\t " +
		"{{FormatDsortStatus $value}}\t " +
		"{{FormatStart $value.StartedTime}}\t " +
		"{{FormatEnd $value.FinishTime}}\t " +
		"{{FormatBckName $value.SrcBck}}\t " +
		"{{FormatBckName $value.DstBck}}\t " +
		"{{if (eq $value.Objs 0) }}-{{else}}{{$value.Objs}}{{end}}\n"
	DsortListNoHdrTmpl = "{{ range $value := . }}" + dsortListBody + "{{end}}"
	DsortListTmpl      = dsortListHdr + DsortListNoHdrTmpl

	DsortListVerboseTmpl = dsortListHdr +
		"{{ range $value := . }}" + dsortListBody +
		indent1 + "Total Extracted Bytes:\t{{if (eq $value.Bytes 0) }}-{{else}}{{FormatBytesSig $value.Bytes 2}}{{end}}\n" +
		indent1 + "Extraction Time:\t{{if (eq $value.ExtractedDuration 0) }}-{{else}}{{FormatDuration $value.ExtractedDuration}}{{end}}\n" +
		indent1 + "Sorting Time:\t{{if (eq $value.SortingDuration 0) }}-{{else}}{{FormatDuration $value.SortingDuration}}{{end}}\n" +
		indent1 + "Creation Time:\t{{if (eq $value.CreationDuration 0) }}-{{else}}{{FormatDuration $value.CreationDuration}}{{end}}\n" +
		indent1 + "Description:\t{{$value.Metrics.Description}}\n" +
		"{{end}}"

	transformListHdr  = "ETL NAME\t XACTION\t OBJECTS\n"
	transformListBody = "{{$value.Name}}\t {{$value.XactID}}\t " +
		"{{if (eq $value.ObjCount 0) }}-{{else}}{{$value.ObjCount}}{{end}}\n"
	TransformListNoHdrTmpl = "{{ range $value := . }}" + transformListBody + "{{end}}"
	TransformListTmpl      = transformListHdr + TransformListNoHdrTmpl

	//
	// BEGIN: xactions as `nodeSnaps` ------------------------------------------------------------------------------
	//

	XactBucketTmpl      = xactBucketHdr + XactNoHdrBucketTmpl
	XactNoHdrBucketTmpl = "{{range $nodeSnaps := . }}" + xactBucketBodyAll + "{{end}}"

	xactBucketHdr     = "NODE\t ID\t KIND\t BUCKET\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	xactBucketBodyAll = "{{range $key, $xctn := $nodeSnaps.XactSnaps}}" + xactBucketBodyOne + "{{end}}"
	xactBucketBodyOne = "{{ $nodeSnaps.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{FormatBckName $xctn.Bck}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatStart $xctn.StartTime}}\t " +
		"{{FormatEnd $xctn.EndTime}}\t " +
		"{{FormatXactRunFinAbrt $xctn}}\n"

	// same as above except for: no bucket column

	XactNoBucketTmpl      = xactNoBucketHdr + XactNoHdrNoBucketTmpl
	XactNoHdrNoBucketTmpl = "{{range $nodeSnaps := . }}" + xactNoBucketBodyAll + "{{end}}"

	xactNoBucketHdr     = "NODE\t ID\t KIND\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	xactNoBucketBodyAll = "{{range $key, $xctn := $nodeSnaps.XactSnaps}}" + xactNoBucketBodyOne + "{{end}}"
	xactNoBucketBodyOne = "{{ $nodeSnaps.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatStart $xctn.StartTime}}\t " +
		"{{FormatEnd $xctn.EndTime}}\t " +
		"{{FormatXactRunFinAbrt $xctn}}\n"

	// same as above except for: src-bck, dst-bck columns

	XactFromToTmpl      = xactFromToHdr + XactNoHdrFromToTmpl
	XactNoHdrFromToTmpl = "{{range $nodeSnaps := . }}" + xactFromToBodyAll + "{{end}}"

	xactFromToHdr     = "NODE\t ID\t KIND\t SRC BUCKET\t DST BUCKET\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	xactFromToBodyAll = "{{range $key, $xctn := $nodeSnaps.XactSnaps}}" + xactFromToBodyOne + "{{end}}"
	xactFromToBodyOne = "{{ $nodeSnaps.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{FormatBckName $xctn.SrcBck}}\t " +
		"{{FormatBckName $xctn.DstBck}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatStart $xctn.StartTime}}\t " +
		"{{FormatEnd $xctn.EndTime}}\t " +
		"{{FormatXactRunFinAbrt $xctn}}\n"

	// EC: get, put

	XactECGetTmpl      = xactECGetStatsHdr + XactECGetNoHdrTmpl
	XactECGetNoHdrTmpl = "{{range $daemon := . }}" + xactECGetBody + "{{end}}"

	xactECGetStatsHdr  = "NODE\t ID\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t START\t END\t STATE\n"
	xactECGetBody      = "{{range $key, $xctn := $nodeSnaps.XactSnaps}}" + xactECGetStatsBody + "{{end}}"
	xactECGetStatsBody = "{{ $nodeSnaps.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{FormatBckName $xctn.Bck}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +

		"{{ $ext := ExtECGetStats $xctn }}" +
		"{{if (eq $ext.ErrCount 0) }}-{{else}}{{$ext.ErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +

		"{{FormatStart $xctn.StartTime}}\t " +
		"{{FormatEnd $xctn.EndTime}}\t " +
		"{{FormatXactRunFinAbrt $xctn}}\n"

	XactECPutTmpl      = xactECPutStatsHdr + XactECPutNoHdrTmpl
	XactECPutNoHdrTmpl = "{{range $nodeSnaps := . }}" + xactECPutBody + "{{end}}"

	xactECPutStatsHdr  = "NODE\t ID\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t ENC TIME\t START\t END\t STATE\n"
	xactECPutBody      = "{{range $key, $xctn := $nodeSnaps.XactSnaps}}" + xactECPutStatsBody + "{{end}}"
	xactECPutStatsBody = "{{ $nodeSnaps.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{FormatBckName $xctn.Bck}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +

		"{{ $ext := ExtECPutStats $xctn }}" +
		"{{if (eq $ext.EncodeErrCount 0) }}-{{else}}{{$ext.EncodeErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +
		"{{if (eq $ext.AvgEncodeTime 0) }}-{{else}}{{FormatMilli $ext.AvgEncodeTime}}{{end}}\t " +

		"{{FormatStart $xctn.StartTime}}\t " +
		"{{FormatEnd $xctn.EndTime}}\t " +
		"{{FormatXactRunFinAbrt $xctn}}\n"

	//
	// END: xactions as `nodeSnaps` ------------------------------------------------------------------------------
	//

	listBucketsSummHdr  = "NAME\t PRESENT\t OBJECTS\t SIZE (apparent, objects, remote)\t USAGE(%)\n"
	ListBucketsSummBody = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBool $v.Info.IsBckPresent}}\t " +
		"{{$v.Info.ObjCount.Present}} {{$v.Info.ObjCount.Remote}}\t " +
		"{{FormatBytesUns $v.Info.TotalSize.OnDisk 2}} {{FormatBytesUns $v.Info.TotalSize.PresentObjs 2}} {{FormatBytesUns $v.Info.TotalSize.RemoteObjs 2}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{$v.Info.UsedPct}}%{{end}}\n" +
		"{{end}}"
	ListBucketsSummTmpl = listBucketsSummHdr + ListBucketsSummBody

	ListBucketsHdrNoSummary  = "NAME\t PRESENT\n"
	ListBucketsBodyNoSummary = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBool $v.Info.IsBckPresent}}\n" +
		"{{end}}"
	ListBucketsTmplNoSummary = ListBucketsHdrNoSummary + ListBucketsBodyNoSummary

	// Bucket summary templates
	BucketsSummariesTmpl = "NAME\t OBJECTS (cached, remote)\t OBJECT SIZES (min, avg, max)\t TOTAL OBJECT SIZE (cached, remote)\t USAGE(%)\n" +
		BucketsSummariesBody
	BucketsSummariesBody = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{$v.ObjCount.Present}} {{$v.ObjCount.Remote}}\t " +
		"{{FormatMAM $v.ObjSize.Min}} {{FormatMAM $v.ObjSize.Avg}} {{FormatMAM $v.ObjSize.Max}}\t " +
		"{{FormatBytesUns $v.TotalSize.PresentObjs 2}} {{FormatBytesUns $v.TotalSize.RemoteObjs 2}}\t {{$v.UsedPct}}%\n" +
		"{{end}}"

	// For `object put` mass uploader. A caller adds to the template
	// total count and size. That is why the template ends with \t
	MultiPutTmpl = "Files to upload:\nEXTENSION\t COUNT\t SIZE\n" +
		"{{range $k, $v := . }}" +
		"{{$k}}\t {{$v.Cnt}}\t {{FormatBytesSig $v.Size 2}}\n" +
		"{{end}}" +
		"TOTAL\t "

	ExtendedUsageTmpl = "{{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}}{{if .VisibleFlags}} [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{end}} - {{.Usage}}\n" +
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

	ShortUsageTmpl = `{{.HelpName}} - {{.Usage}}
   {{.UsageText}}
USAGE:
   {{.HelpName}} {{.ArgsUsage}}

See '--help' and docs/cli for details.`

	AuthNClusterTmpl = "CLUSTER ID\tALIAS\tURLs\n" +
		"{{ range $clu := . }}" +
		"{{ $clu.ID }}\t{{ $clu.Alias }}\t{{ JoinList $clu.URLs }}\n" +
		"{{end}}"

	AuthNRoleTmpl = "ROLE\tDESCRIPTION\n" +
		"{{ range $role := . }}" +
		"{{ $role.Name }}\t{{ $role.Description }}\n" +
		"{{end}}"

	AuthNUserTmpl = "NAME\tROLES\n" +
		"{{ range $user := . }}" +
		"{{ $user.ID }}\t{{ range $i, $role := $user.Roles }}" +
		"{{ if $i }}, {{ end }}{{ $role.Name }}" +
		"{{end}}\n" +
		"{{end}}"

	AuthNUserVerboseTmpl = "Name\t{{ .ID }}\n" +
		"Roles\t{{ range $i, $role := .Roles }}{{ if $i }}, {{ end }}{{ $role.Name }}{{ end }}\n" +
		"{{ range $role := .Roles }}" +
		"{{ if ne (len $role.ClusterACLs) 0 }}" +
		"CLUSTER ID\tALIAS\tPERMISSIONS\n" +
		"{{ range $clu := $role.ClusterACLs }}" +
		"{{ $clu.ID }}\t{{ $clu.Alias }}\t{{ FormatACL $clu.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ if ne (len $role.BucketACLs) 0 }}" +
		"BUCKET\tPERMISSIONS\n" +
		"{{ range $bck := $role.BucketACLs }}" +
		"{{ FormatBckName $bck.Bck }}\t{{ FormatACL $bck.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ end }}"

	AuthNRoleVerboseTmpl = "Role\t{{ .Name }}\n" +
		"Description\t{{ .Description }}\n" +
		"{{ if ne (len .ClusterACLs) 0 }}" +
		"CLUSTER ID\tALIAS\tPERMISSIONS\n" +
		"{{ range $clu := .ClusterACLs }}" +
		"{{ $clu.ID }}\t{{ $clu.Alias }}\t{{ FormatACL $clu.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ if ne (len .BucketACLs) 0 }}" +
		"BUCKET\tPERMISSIONS\n" +
		"{{ range $bck := .BucketACLs }}" +
		"{{ FormatBckName $bck.Bck }}\t{{ FormatACL $bck.Access }}\n" +
		"{{end}}{{end}}"

	// `search`
	SearchTmpl = "{{ JoinListNL . }}\n"

	// `show mountpath`
	MpathListTmpl = "{{range $p := . }}" +
		"{{ $p.DaemonID }}\n" +
		"{{if and (eq (len $p.Mpl.Available) 0) (eq (len $p.Mpl.Disabled) 0)}}" +
		"\tNo mountpaths\n" +
		"{{else}}" +
		"{{if ne (len $p.Mpl.Available) 0}}" +
		"\tUsed: {{FormatCapPctMAM $p.Tcdf true}}\t " +
		"{{if (IsEqS $p.Tcdf.CsErr \"\")}}{{else}}{{$p.Tcdf.CsErr}}{{end}}\n" +
		"{{range $mp := $p.Mpl.Available }}" +
		"\t\t{{ $mp }} " +

		"{{range $k, $v := $p.Tcdf.Mountpaths}}" +
		"{{if (IsEqS $k $mp)}}{{FormatCDFDisks $v}}{{end}}" +
		"{{end}}\n" +

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
