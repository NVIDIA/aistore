// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// to be used by external watchdogs (Kubernetes, etc.)
// (compare with api.Health below)
func GetProxyReadiness(params BaseParams) error {
	params.Method = http.MethodGet
	q := url.Values{cmn.URLParamHealthReadiness: []string{"true"}}
	reqParams := &ReqParams{BaseParams: params, Path: cmn.URLPathHealth.S, Query: q}
	return reqParams.DoHTTPRequest()
}

func Health(baseParams BaseParams, readyToRebalance ...bool) error {
	var q url.Values
	baseParams.Method = http.MethodGet
	if len(readyToRebalance) > 0 && readyToRebalance[0] {
		q = url.Values{cmn.URLParamPrimaryReadyReb: []string{"true"}}
	}
	reqParams := &ReqParams{BaseParams: baseParams, Path: cmn.URLPathHealth.S, Query: q}
	return reqParams.DoHTTPRequest()
}

// GetClusterMap retrieves AIStore cluster map.
func GetClusterMap(baseParams BaseParams) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
	}
	err = reqParams.DoHTTPReqResp(&smap)
	return
}

// GetNodeClusterMap retrieves AIStore cluster map from specific node.
func GetNodeClusterMap(baseParams BaseParams, nodeID string) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
		Header:     http.Header{cmn.HdrNodeID: []string{nodeID}},
	}
	err = reqParams.DoHTTPReqResp(&smap)
	return
}

// GetClusterSysInfo retrieves AIStore system info.
func GetClusterSysInfo(baseParams BaseParams) (sysInfo cmn.ClusterSysInfo, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}},
	}
	err = reqParams.DoHTTPReqResp(&sysInfo)
	return
}

// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy).
func GetClusterStats(baseParams BaseParams) (clusterStats stats.ClusterStats, err error) {
	var rawStats stats.ClusterStatsRaw
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStats}},
	}
	err = reqParams.DoHTTPReqResp(&rawStats)
	if err != nil {
		return
	}

	clusterStats.Proxy = rawStats.Proxy
	clusterStats.Target = make(map[string]*stats.DaemonStats)
	for tid := range rawStats.Target {
		var ts stats.DaemonStats
		if err := jsoniter.Unmarshal(rawStats.Target[tid], &ts); err == nil {
			clusterStats.Target[tid] = &ts
		}
	}

	return
}

func GetTargetDiskStats(baseParams BaseParams, targetID string) (diskStats ios.AllDiskStats, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDiskStats}},
		Header:     http.Header{cmn.HdrNodeID: []string{targetID}},
	}
	err = reqParams.DoHTTPReqResp(&diskStats)
	return
}

func GetRemoteAIS(baseParams BaseParams) (aisInfo cmn.BackendInfoAIS, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatRemoteAIS}},
	}
	err = reqParams.DoHTTPReqResp(&aisInfo)
	return
}

// JoinCluster add a node to a cluster.
func JoinCluster(baseParams BaseParams, nodeInfo *cluster.Snode) (rebID, daemonID string, err error) {
	var info cmn.JoinNodeResult
	baseParams.Method = http.MethodPost
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathClusterUserReg.S,
		Body:       cos.MustMarshal(nodeInfo),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err = reqParams.DoHTTPReqResp(&info)
	return info.RebalanceID, info.DaemonID, err
}

// SetPrimaryProxy given a daemonID sets that corresponding proxy as the
// primary proxy of the cluster.
func SetPrimaryProxy(baseParams BaseParams, newPrimaryID string, force bool) error {
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathClusterProxy.Join(newPrimaryID),
		Query:      url.Values{cmn.URLParamForce: []string{strconv.FormatBool(force)}},
	}
	return reqParams.DoHTTPRequest()
}

// SetClusterConfig given key-value pairs of cluster configuration parameters,
// sets the cluster-wide configuration accordingly. Setting cluster-wide
// configuration requires sending the request to a proxy.
func SetClusterConfig(baseParams BaseParams, nvs cos.SimpleKVs, transient ...bool) error {
	q := url.Values{}
	for key, val := range nvs {
		q.Add(key, val)
	}
	if len(transient) > 0 {
		q.Add(cmn.ActTransient, strconv.FormatBool(transient[0]))
	}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterSetConf.S, Query: q}
	return reqParams.DoHTTPRequest()
}

// SetClusterConfigUsingMsg sets the cluster-wide configuration
// using the `cmn.ConfigToUpdate` parameter provided.
func SetClusterConfigUsingMsg(baseParams BaseParams, configToUpdate *cmn.ConfigToUpdate, transient ...bool) error {
	q := url.Values{}
	msg := cmn.ActionMsg{
		Action: cmn.ActSetConfig,
		Value:  configToUpdate,
	}
	if len(transient) > 0 {
		q.Add(cmn.ActTransient, strconv.FormatBool(transient[0]))
	}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      q,
	}
	return reqParams.DoHTTPRequest()
}

// ResetClusterConfig resets the configuration of all nodes to the cluster configuration
func ResetClusterConfig(baseParams BaseParams) error {
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetConfig}),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	return reqParams.DoHTTPRequest()
}

// GetClusterConfig returns cluster-wide configuration
func GetClusterConfig(baseParams BaseParams) (*cmn.ClusterConfig, error) {
	cluConfig := &cmn.ClusterConfig{}
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatClusterConfig}},
	}
	err := reqParams.DoHTTPReqResp(cluConfig)
	if err != nil {
		return nil, err
	}
	return cluConfig, nil
}

// GetBMD returns bucket metadata
func GetBMD(baseParams BaseParams) (*cluster.BMD, error) {
	bmd := &cluster.BMD{}
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatBMD}},
	}
	err := reqParams.DoHTTPReqResp(bmd)
	if err != nil {
		return nil, err
	}
	return bmd, nil
}

func AttachRemoteAIS(baseParams BaseParams, alias, u string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, u)
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterAttach.S, Query: q}
	return reqParams.DoHTTPRequest()
}

func DetachRemoteAIS(baseParams BaseParams, alias string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, "")
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterDetach.S, Query: q}
	return reqParams.DoHTTPRequest()
}

// Maintenance API
//
func StartMaintenance(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActStartMaintenance,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err = reqParams.DoHTTPReqResp(&id)
	return id, err
}

func DecommissionNode(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActDecommissionNode,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err = reqParams.DoHTTPReqResp(&id)
	return id, err
}

func StopMaintenance(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActStopMaintenance,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err = reqParams.DoHTTPReqResp(&id)
	return id, err
}

// ShutdownCluster shuts down the whole cluster
func ShutdownCluster(baseParams BaseParams) error {
	msg := cmn.ActionMsg{Action: cmn.ActShutdown}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	return reqParams.DoHTTPRequest()
}

// DecommissionCluster decommissions whole cluster
func DecommissionCluster(baseParams BaseParams) error {
	msg := cmn.ActionMsg{Action: cmn.ActDecommission}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err := reqParams.DoHTTPRequest()
	if cos.IsEOF(err) {
		err = nil
	}
	return err
}

// ShutdownNode shuts down a specific node
func ShutdownNode(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActShutdownNode,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err = reqParams.DoHTTPReqResp(&id)
	return id, err
}

// Remove a node from Smap immediately.
// Immediately removes a node from Smap (advanced usage - potential data loss)
func RemoveNodeFromSmap(baseParams BaseParams, sid string) error {
	baseParams.Method = http.MethodDelete
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathClusterDaemon.Join(sid),
	}
	return reqParams.DoHTTPRequest()
}
