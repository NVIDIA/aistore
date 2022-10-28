// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

const ua = "aisnode/backend"

type (
	remAis struct {
		smap *cluster.Smap
		m    *AISBackendProvider
		url  string
		uuid string
		bp   api.BaseParams
	}
	AISBackendProvider struct {
		t      cluster.Target
		mu     *sync.RWMutex
		remote map[string]*remAis // by UUID:  1 to 1
		alias  map[string]string  // by alias: many to 1 UUID
	}
)

// interface guard
var _ cluster.BackendProvider = (*AISBackendProvider)(nil)

// TODO: house-keep refreshing remote Smap
// TODO: utilize m.remote[uuid].smap to load balance and retry disconnects

func NewAIS(t cluster.Target) *AISBackendProvider {
	return &AISBackendProvider{
		t:      t,
		mu:     &sync.RWMutex{},
		remote: make(map[string]*remAis),
		alias:  make(map[string]string),
	}
}

func (r *remAis) String() string {
	var aliases []string
	for alias, uuid := range r.m.alias {
		if uuid == r.smap.UUID {
			aliases = append(aliases, alias)
		}
	}
	return fmt.Sprintf("remote cluster (url: %s, aliases: %q, uuid: %v, smap: %s)", r.url, aliases, r.smap.UUID, r.smap)
}

// NOTE: this and the next method are part of the of the *extended* AIS cloud API
//       in addition to the basic GetObj, et al.

// apply new or updated (attach, detach) cmn.BackendConfAIS configuration
func (m *AISBackendProvider) Apply(v any, action string) error {
	var (
		cfg         = cmn.GCO.Get()
		clusterConf = cmn.BackendConfAIS{}
	)
	if err := cos.MorphMarshal(v, &clusterConf); err != nil {
		return fmt.Errorf("invalid ais backend config (%+v, %T), err: %v", v, v, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// detach
	if action == apc.ActDetachRemote {
		for alias, uuid := range m.alias {
			if _, ok := clusterConf[alias]; !ok {
				if _, ok = clusterConf[uuid]; !ok {
					delete(m.alias, alias)
					delete(m.remote, uuid)
				}
			}
		}
		return nil
	}

	// validate aliases
	for alias := range clusterConf {
		if err := cmn.ValidateRemAlias(alias); err != nil {
			return err
		}
	}

	// init and attach
	for alias, clusterURLs := range clusterConf {
		remAis := &remAis{}
		if offline, err := remAis.init(alias, clusterURLs, cfg); err != nil { // and check connectivity
			if offline {
				continue
			}
			return err
		}
		if err := m.add(remAis, alias); err != nil {
			return err
		}
	}
	return nil
}

// At the same time a cluster may have registered both types of remote AIS
// clusters(HTTP and HTTPS). So, the method must use both kind of clients and
// select the correct one at the moment it sends a request.
func (m *AISBackendProvider) GetInfo(clusterConf cmn.BackendConfAIS) (cia cmn.BackendInfoAIS) {
	var (
		cfg         = cmn.GCO.Get()
		httpClient  = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout.D()})
		httpsClient = cmn.NewClient(cmn.TransportArgs{
			Timeout:    cfg.Client.Timeout.D(),
			UseHTTPS:   true,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
	)
	cia = make(cmn.BackendInfoAIS, len(m.remote))

	m.mu.RLock()
	for uuid, remAis := range m.remote {
		var (
			aliases []string
			out     = &cmn.RemoteAIS{} // remAis (type) external representation
			client  = httpClient
		)
		if cos.IsHTTPS(remAis.url) {
			client = httpsClient
		}
		out.URL = remAis.url
		for a, u := range m.alias {
			if uuid == u {
				aliases = append(aliases, a)
			}
		}
		out.Alias = strings.Join(aliases, cmn.RemAisAliasSeparator)

		// online?
		if smap, err := api.GetClusterMap(api.BaseParams{Client: client, URL: remAis.url, UA: ua}); err == nil {
			if smap.UUID != uuid {
				glog.Errorf("%s: UUID has changed %q", remAis, smap.UUID)
				continue
			}
			out.Online = true
			if smap.Version < remAis.smap.Version {
				glog.Errorf("%s: detected older Smap %s - proceeding to override anyway", remAis, smap)
			}
			remAis.smap = smap
		}
		out.Primary = remAis.smap.Primary.String()
		out.Smap = remAis.smap.Version
		out.Targets = int32(remAis.smap.CountActiveTargets())
		cia[uuid] = out
	}
	// defunct
	for alias, clusterURLs := range clusterConf {
		if _, ok := m.alias[alias]; !ok {
			if _, ok = m.remote[alias]; !ok {
				info := &cmn.RemoteAIS{}
				info.URL = fmt.Sprintf("%v", clusterURLs)
				cia[alias] = info
			}
		}
	}
	m.mu.RUnlock()

	return
}

// A list of remote AIS URLs can contains both HTTP and HTTPS links at the
// same time. So, the method must use both kind of clients and select the
// correct one at the moment it sends a request. First successful request
// saves the good client for the future usage.
func (r *remAis) init(alias string, confURLs []string, cfg *cmn.Config) (offline bool, err error) {
	var (
		url           string
		remSmap, smap *cluster.Smap
		httpClient    = cmn.NewClient(cmn.TransportArgs{Timeout: cfg.Client.Timeout.D()})
		httpsClient   = cmn.NewClient(cmn.TransportArgs{
			Timeout:    cfg.Client.Timeout.D(),
			UseHTTPS:   true,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
	)
	for _, u := range confURLs {
		client := httpClient
		if cos.IsHTTPS(u) {
			client = httpsClient
		}
		if smap, err = api.GetClusterMap(api.BaseParams{Client: client, URL: u, UA: ua}); err != nil {
			glog.Warningf("remote cluster failing to reach %q via %s, err: %v", alias, u, err)
			continue
		}
		if remSmap == nil {
			remSmap, url = smap, u
			continue
		}
		if remSmap.UUID != smap.UUID {
			err = fmt.Errorf("%q(%v) references two different clusters: uuid=%q vs uuid=%q",
				alias, confURLs, remSmap.UUID, smap.UUID)
			return
		}
		if remSmap.Version < smap.Version {
			remSmap, url = smap, u
		}
	}
	if remSmap == nil {
		err = fmt.Errorf("remote cluster failed to reach %q via any/all of the configured URLs %v", alias, confURLs)
		offline = true
		return
	}
	r.smap, r.url = remSmap, url
	if cos.IsHTTPS(url) {
		r.bp = api.BaseParams{Client: httpsClient, URL: url, UA: ua}
	} else {
		r.bp = api.BaseParams{Client: httpClient, URL: url, UA: ua}
	}
	r.uuid = remSmap.UUID
	return
}

// NOTE: supporting remote attachments both by alias and by UUID interchangeably,
// with mappings: 1(uuid) to 1(cluster) and 1(alias) to 1(cluster)
func (m *AISBackendProvider) add(newAis *remAis, newAlias string) (err error) {
	if remAis, ok := m.remote[newAlias]; ok {
		return fmt.Errorf("cannot attach %s: alias %q is already in use as uuid for %s",
			newAlias, newAlias, remAis)
	}
	newAis.m = m
	tag := "added"
	if newAlias == newAis.smap.UUID {
		// not an alias
		goto ad
	}
	// existing
	if remAis, ok := m.remote[newAis.smap.UUID]; ok {
		// can re-alias existing remote cluster
		for alias, uuid := range m.alias {
			if uuid == newAis.smap.UUID {
				delete(m.alias, alias)
			}
		}
		m.alias[newAlias] = newAis.smap.UUID // alias
		if newAis.url != remAis.url {
			glog.Warningf("%s: different new URL %s - overriding", remAis, newAis)
		}
		if newAis.smap.Version < remAis.smap.Version {
			glog.Errorf("%s: detected older Smap %s - proceeding to override anyway", remAis, newAis)
		}
		tag = "updated"
		goto ad
	}
	if uuid, ok := m.alias[newAlias]; ok {
		remAis, ok := m.remote[uuid]
		if ok {
			return fmt.Errorf("cannot attach %s: alias %q is already in use for %s",
				newAis, newAlias, remAis)
		}
		delete(m.alias, newAlias)
	}
	m.alias[newAlias] = newAis.smap.UUID
ad:
	m.remote[newAis.smap.UUID] = newAis
	glog.Infof("%s %s", newAis, tag)
	return
}

func (m *AISBackendProvider) getRemAis(aliasOrUUID string) (remAis *remAis, err error) {
	m.mu.RLock()
	remAis, _, err = m.resolve(aliasOrUUID)
	m.mu.RUnlock()
	return
}

func (m *AISBackendProvider) headRemAis(aliasOrUUID string) (remAis *remAis, uuid string, aliases []string, err error) {
	m.mu.RLock()
	remAis, uuid, err = m.resolve(aliasOrUUID)
	if err != nil {
		m.mu.RUnlock()
		return
	}
	for alias, id := range m.alias {
		if id == uuid {
			aliases = append(aliases, alias)
		}
	}
	m.mu.RUnlock()
	return
}

// resolve (alias | UUID) => remAis, UUID
// is called under lock
func (m *AISBackendProvider) resolve(uuid string) (*remAis, string, error) {
	remAis, ok := m.remote[uuid]
	if ok {
		return remAis, uuid, nil
	}
	alias := uuid
	if uuid, ok = m.alias[alias]; !ok {
		return nil, "", cmn.NewErrNotFound("%s: remote cluster %q", m.t, alias)
	}
	remAis, ok = m.remote[uuid]
	debug.Assertf(ok, "%q vs %q", alias, uuid)
	return remAis, uuid, nil
}

func unsetUUID(bck *cmn.Bck) { bck.Ns.UUID = "" }

func extractErrCode(e error) (int, error) {
	if e == nil {
		return http.StatusOK, nil
	}
	if httpErr := cmn.Err2HTTPErr(e); httpErr != nil {
		return httpErr.Status, httpErr
	}
	return http.StatusInternalServerError, e
}

/////////////////////
// BackendProvider //
/////////////////////

func (*AISBackendProvider) Provider() string  { return apc.AIS }
func (*AISBackendProvider) MaxPageSize() uint { return apc.DefaultPageSizeAIS }

func (*AISBackendProvider) CreateBucket(_ *cluster.Bck) (errCode int, err error) {
	debug.Assert(false) // Bucket creation happens only with reverse proxy to AIS cluster.
	return 0, nil
}

// TODO: remote AIS clusters provide native frontend API with additional capabilities
// that, in particular, include `dontAddRemote` = (true | false).
// Here we have to hardcode the value to keep HeadBucket() consistent across all backends.
// For similar limitations, see also ListBuckets() below.
func (m *AISBackendProvider) HeadBucket(_ ctx, remoteBck *cluster.Bck) (bckProps cos.StrKVs, errCode int, err error) {
	var (
		remAis  *remAis
		p       *cmn.BucketProps
		uuid    string
		aliases []string
	)
	if remAis, uuid, aliases, err = m.headRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	debug.Assert(uuid == remAis.uuid)
	bck := remoteBck.Clone()
	unsetUUID(&bck)
	if p, err = api.HeadBucket(remAis.bp, bck, false /*dontAddRemote*/); err != nil {
		errCode, err = extractErrCode(err)
		return
	}

	bckProps = make(cos.StrKVs, 32)
	err = cmn.IterFields(p, func(uniqueTag string, field cmn.IterField) (e error, b bool) {
		bckProps[uniqueTag] = fmt.Sprintf("%v", field.Value())
		return nil, false
	})
	// an extra
	bckProps[apc.HdrBackendProvider] = apc.AIS
	bckProps[apc.HdrRemAisUUID] = remAis.uuid
	bckProps[apc.HdrRemAisAlias] = strings.Join(aliases, cmn.RemAisAliasSeparator)
	bckProps[apc.HdrRemAisURL] = remAis.url

	return
}

func (m *AISBackendProvider) ListObjects(remoteBck *cluster.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (errCode int, err error) {
	var remAis *remAis
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	remoteMsg := msg.Clone()
	remoteMsg.PageSize = calcPageSize(remoteMsg.PageSize, m.MaxPageSize())

	// TODO: Currently, we are not encoding xaction (aka request) `UUID` from the remote cluster
	// in the `ContinuationToken` (see note below).
	remoteMsg.UUID = ""

	// likewise, let remote ais gateway decide
	remoteMsg.SID = ""

	bck := remoteBck.Clone()
	unsetUUID(&bck)

	var lstRes *cmn.LsoResult
	if lstRes, err = api.ListObjectsPage(remAis.bp, bck, remoteMsg); err != nil {
		errCode, err = extractErrCode(err)
		return
	}
	*lst = *lstRes

	// Restore the original request UUID (UUID of the remote cluster is already inside `ContinuationToken`).
	lst.UUID = msg.UUID
	return
}

func (m *AISBackendProvider) ListBuckets(qbck cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	if !qbck.Ns.IsAnyRemote() {
		// user may have provided an alias
		if remAis, errV := m.getRemAis(qbck.Ns.UUID); errV == nil {
			qbck.Ns.UUID = remAis.uuid
		}
		bcks, err = m._listBcks(qbck.Ns.UUID, qbck)
	} else {
		for uuid := range m.remote {
			remoteBcks, tryErr := m._listBcks(uuid, qbck)
			bcks = append(bcks, remoteBcks...)
			if tryErr != nil {
				err = tryErr
			}
		}
	}
	errCode, err = extractErrCode(err)
	return
}

// TODO: remote AIS clusters provide native frontend API with additional capabilities
// in part including apc.Flt* location specifier.
// Here we hardcode its value to keep ListBuckets() consistent across backends.
func (m *AISBackendProvider) _listBcks(uuid string, qbck cmn.QueryBcks) (bcks cmn.Bcks, err error) {
	var (
		remAis      *remAis
		remoteQuery = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.Ns{Name: qbck.Ns.Name}}
	)
	if remAis, err = m.getRemAis(uuid); err != nil {
		return
	}
	bcks, err = api.ListBuckets(remAis.bp, remoteQuery, apc.FltExists)
	if err != nil {
		_, err = extractErrCode(err)
		return nil, err
	}
	for i, bck := range bcks {
		bck.Ns.UUID = uuid // if `uuid` is alias we need to preserve it
		bcks[i] = bck
	}
	return bcks, nil
}

// TODO: remote AIS clusters provide native frontend API with additional capabilities
// in part including apc.Flt* location specifier.
// Here, and elsewhere down below, we hardcode (the default) `apc.FltPresent` to, eesentially,
// keep HeadObj() consistent across backends.
func (m *AISBackendProvider) HeadObj(_ ctx, lom *cluster.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		remAis    *remAis
		op        *cmn.ObjectProps
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	if op, err = api.HeadObject(remAis.bp, remoteBck, lom.ObjName, apc.FltPresent); err != nil {
		errCode, err = extractErrCode(err)
		return
	}
	oa = &cmn.ObjAttrs{}
	*oa = op.ObjAttrs
	oa.SetCustomKey(cmn.SourceObjMD, apc.AIS)
	return
}

func (m *AISBackendProvider) GetObj(_ ctx, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	var (
		remAis    *remAis
		r         io.ReadCloser
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	if r, err = api.GetObjectReader(remAis.bp, remoteBck, lom.ObjName); err != nil {
		return extractErrCode(err)
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = r
		params.OWT = owt
		params.Atime = time.Now()
	}
	err = m.t.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	return extractErrCode(err)
}

func (m *AISBackendProvider) GetObjReader(_ ctx, lom *cluster.LOM) (r io.ReadCloser, expCksum *cos.Cksum, errCode int, err error) {
	var (
		remAis    *remAis
		op        *cmn.ObjectProps
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	if op, err = api.HeadObject(remAis.bp, remoteBck, lom.ObjName, apc.FltPresent); err != nil {
		errCode, err = extractErrCode(err)
		return
	}
	oa := lom.ObjAttrs()
	*oa = op.ObjAttrs
	oa.SetCustomKey(cmn.SourceObjMD, apc.AIS)
	expCksum = oa.Cksum
	lom.SetCksum(nil)
	// reader
	r, err = api.GetObjectReader(remAis.bp, remoteBck, lom.ObjName)
	errCode, err = extractErrCode(err)
	return
}

func (m *AISBackendProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (errCode int, err error) {
	var (
		remAis    *remAis
		op        *cmn.ObjectProps
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		cos.Close(r)
		return
	}
	unsetUUID(&remoteBck)
	args := api.PutObjectArgs{
		BaseParams: remAis.bp,
		Bck:        remoteBck,
		Object:     lom.ObjName,
		Cksum:      lom.Checksum(),
		Reader:     r.(cos.ReadOpenCloser),
		Size:       uint64(lom.SizeBytes(true)), // _special_ because it's still workfile.
	}
	if err = api.PutObject(args); err != nil {
		errCode, err = extractErrCode(err)
		return
	}
	// TODO: piggy-back props on PUT request to optimize-out HEAD call
	if op, err = api.HeadObject(remAis.bp, remoteBck, lom.ObjName, apc.FltPresent); err != nil {
		errCode, err = extractErrCode(err)
		return
	}
	oa := lom.ObjAttrs()
	*oa = op.ObjAttrs
	oa.SetCustomKey(cmn.SourceObjMD, apc.AIS)
	return
}

func (m *AISBackendProvider) DeleteObj(lom *cluster.LOM) (errCode int, err error) {
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	err = api.DeleteObject(remAis.bp, remoteBck, lom.ObjName)
	return extractErrCode(err)
}
