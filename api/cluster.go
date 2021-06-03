// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
)

type (
	JoinNodeResult struct {
		DaemonID    string `json:"daemon_id"`
		RebalanceID string `json:"rebalance_id"`
	}
)

func GetProxyReadiness(params BaseParams) error {
	params.Method = http.MethodGet
	return DoHTTPRequest(ReqParams{
		BaseParams: params,
		Path:       cmn.URLPathHealth.S,
		Query:      url.Values{cmn.URLParamHealthReadiness: []string{"true"}},
	})
}

// GetClusterMap retrieves AIStore cluster map.
func GetClusterMap(baseParams BaseParams) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
	}, &smap)
	return
}

// GetNodeClusterMap retrieves AIStore cluster map from specific node.
func GetNodeClusterMap(baseParams BaseParams, nodeID string) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
		Header:     http.Header{cmn.HdrNodeID: []string{nodeID}},
	}, &smap)
	return
}

// GetClusterSysInfo retrieves AIStore system info.
func GetClusterSysInfo(baseParams BaseParams) (sysInfo cmn.ClusterSysInfo, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}},
	}, &sysInfo)
	return
}

// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy).
func GetClusterStats(baseParams BaseParams) (clusterStats stats.ClusterStats, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStats}},
	}, &clusterStats)
	return
}

func GetTargetDiskStats(baseParams BaseParams, targetID string) (diskStats ios.AllDiskStats, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDiskStats}},
		Header:     http.Header{cmn.HdrNodeID: []string{targetID}},
	}, &diskStats)
	return
}

func GetRemoteAIS(baseParams BaseParams) (aisInfo cmn.BackendInfoAIS, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatRemoteAIS}},
	}, &aisInfo)
	return
}

// JoinCluster add a node to a cluster.
func JoinCluster(baseParams BaseParams, nodeInfo *cluster.Snode) (rebID, daemonID string, err error) {
	var info JoinNodeResult
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathClusterUserReg.S,
		Body:       cos.MustMarshal(nodeInfo),
	}, &info)
	return info.RebalanceID, info.DaemonID, err
}

// SetPrimaryProxy given a daemonID sets that corresponding proxy as the
// primary proxy of the cluster.
func SetPrimaryProxy(baseParams BaseParams, newPrimaryID string) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathClusterProxy.Join(newPrimaryID),
	})
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
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterSetConf.S, Query: q})
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
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathCluster.S, Body: cos.MustMarshal(msg), Query: q})
}

// ResetClusterConfig resets the configuration of all nodes to the cluster configuration
func ResetClusterConfig(baseParams BaseParams) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetConfig}),
	})
}

// GetClusterConfig returns cluster-wide configuration
func GetClusterConfig(baseParams BaseParams) (*cmn.ClusterConfig, error) {
	cluConfig := &cmn.ClusterConfig{}
	baseParams.Method = http.MethodGet
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatClusterConfig}},
	}, cluConfig)
	if err != nil {
		return nil, err
	}
	return cluConfig, nil
}

// GetBMD returns bucket metadata
func GetBMD(baseParams BaseParams) (*cluster.BMD, error) {
	bmd := &cluster.BMD{}
	baseParams.Method = http.MethodGet
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatBMD}},
	}, bmd)
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
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterAttach.S, Query: q})
}

func DetachRemoteAIS(baseParams BaseParams, alias string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, "")
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterDetach.S, Query: q})
}

// Maintenance API
//
func StartMaintenance(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActStartMaintenance,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathCluster.S, Body: cos.MustMarshal(msg)}, &id)
	return id, err
}

func Decommission(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActDecommissionNode,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathCluster.S, Body: cos.MustMarshal(msg)}, &id)
	return id, err
}

func StopMaintenance(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActStopMaintenance,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathCluster.S, Body: cos.MustMarshal(msg)}, &id)
	return id, err
}

func Health(baseParams BaseParams) error {
	baseParams.Method = http.MethodGet
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathHealth.S})
}

// ShutdownCluster shuts down the whole cluster
func ShutdownCluster(baseParams BaseParams) error {
	msg := cmn.ActionMsg{Action: cmn.ActShutdown}
	baseParams.Method = http.MethodPut
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
	})
	return err
}

// DecommissionCluster decommissions whole cluster
func DecommissionCluster(baseParams BaseParams) error {
	msg := cmn.ActionMsg{Action: cmn.ActDecommission}
	baseParams.Method = http.MethodPut
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
	})
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
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathCluster.S, Body: cos.MustMarshal(msg)}, &id)
	return id, err
}
