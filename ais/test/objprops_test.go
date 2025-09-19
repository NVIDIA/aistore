// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

func propsStats(t *testing.T, proxyURL string) (objChanged, bytesChanged int64) {
	cstats := tools.GetClusterStats(t, proxyURL)
	objChanged = 0
	bytesChanged = 0

	for _, v := range cstats.Target {
		objChanged += tools.GetNamedStatsVal(v, stats.VerChangeCount)
		bytesChanged += tools.GetNamedStatsVal(v, stats.VerChangeSize)
	}
	return
}

func propsUpdateObjects(t *testing.T, proxyURL string, bck cmn.Bck, oldVersions map[string]string,
	msg *apc.LsoMsg, versionEnabled bool, cksumType string) (newVersions map[string]string) {
	newVersions = make(map[string]string, len(oldVersions))
	tlog.Logln("Updating...")
	r, err := readers.NewRand(int64(fileSize), cksumType)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	for fname := range oldVersions {
		putArgs := api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    fname,
			Cksum:      r.Cksum(),
			Reader:     r,
		}
		_, err = api.PutObject(&putArgs)
		if err != nil {
			t.Errorf("Failed to PUT new data to object %s: %v", bck.Cname(fname), err)
		}
	}

	reslist := testListObjects(t, proxyURL, bck, msg)
	tassert.Errorf(t, len(oldVersions) == len(reslist.Entries), "len(oldVersions) %d != %d len(reslist.Entries)",
		len(oldVersions), len(reslist.Entries))

	var (
		ver string
		ok  bool
	)
	for _, m := range reslist.Entries {
		if ver, ok = oldVersions[m.Name]; !ok {
			continue
		}
		newVersions[m.Name] = m.Version

		if !m.IsPresent() && bck.IsRemote() {
			t.Errorf("%s: not marked as cached one", bck.Cname(m.Name))
		}
		if !versionEnabled {
			continue
		}
		if ver == m.Version {
			t.Fatalf("%s: version was expected to update", bck.Cname(m.Name))
		} else if m.Version == "" {
			t.Fatalf("%s: version is empty", bck.Cname(m.Name))
		}
	}
	tlog.Logfln("All %d object versions updated", len(reslist.Entries))

	return
}

func propsReadObjects(t *testing.T, proxyURL string, bck cmn.Bck, lst map[string]string) {
	versChanged, bytesChanged := propsStats(t, proxyURL)
	baseParams := tools.BaseAPIParams(proxyURL)
	for objName := range lst {
		_, err := api.GetObject(baseParams, bck, objName, nil)
		if err != nil {
			t.Errorf("Failed to GET %s: %v", bck.Cname(objName), err)
			continue
		}
	}
	versChangedFinal, bytesChangedFinal := propsStats(t, proxyURL)
	if versChangedFinal-versChanged > 0 {
		tlog.Logfln("Versions changed: %d (%s)", versChangedFinal-versChanged, cos.ToSizeIEC(bytesChangedFinal-bytesChanged, 1))
	}
	if versChanged != versChangedFinal || bytesChanged != bytesChangedFinal {
		t.Fatalf("All objects must be retrieved from the cache but cold get happened: %d times (%d bytes)",
			versChangedFinal-versChanged, bytesChangedFinal-bytesChanged)
	}
}

func propsEvict(t *testing.T, proxyURL string, bck cmn.Bck, objMap map[string]string, msg *apc.LsoMsg, versionEnabled bool) {
	// generate object list to evict 1/3rd of all objects - random selection
	toEvict := len(objMap) / 3
	if toEvict == 0 {
		toEvict = 1
	}
	toEvictList := make([]string, 0, toEvict)
	evictMap := make(map[string]bool, toEvict)
	tlog.Logfln("Evicting %v objects:", toEvict)

	for fname := range objMap {
		evictMap[fname] = true
		toEvictList = append(toEvictList, fname)
		tlog.Logfln("    %s", bck.Cname(fname))
		if len(toEvictList) >= toEvict {
			break
		}
	}

	baseParams := tools.BaseAPIParams(proxyURL)
	evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: toEvictList}}
	xid, err := api.EvictMultiObj(baseParams, bck, evdMsg)
	if err != nil {
		t.Errorf("Failed to evict objects: %v\n", err)
	}
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	tlog.Logfln("Reading object list...")

	// read a new object list and check that evicted objects do not have atime and cached==false
	// version must be the same
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		return
	}

	for _, m := range reslist.Entries {
		oldVersion, ok := objMap[m.Name]
		if !ok {
			continue
		}
		tlog.Logfln("%s: fl [%d], cached [%t], atime [%v], version [%s]", bck.Cname(m.Name), m.Flags, m.IsPresent(), m.Atime, m.Version)

		// e.g. misplaced replica
		if !m.IsStatusOK() {
			continue
		}

		if _, wasEvicted := evictMap[m.Name]; wasEvicted {
			if m.Atime != "" {
				t.Errorf("Evicted %s still has atime %q", bck.Cname(m.Name), m.Atime)
			}
			if m.IsPresent() {
				t.Errorf("Evicted %s is still marked as _cached_", bck.Cname(m.Name))
			}
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("%s: version is empty", bck.Cname(m.Name))
		} else if m.Version != oldVersion {
			t.Errorf("%s: version has changed from %s to %s", bck.Cname(m.Name), oldVersion, m.Version)
		}
	}
}

func propsRecacheObjects(t *testing.T, proxyURL string, bck cmn.Bck, objs map[string]string, msg *apc.LsoMsg, versionEnabled bool) {
	tlog.Logfln("Reading...")
	propsReadObjects(t, proxyURL, bck, objs)

	tlog.Logfln("Listing objects...")
	reslist := testListObjects(t, proxyURL, bck, msg)
	tassert.Fatalf(t, reslist != nil && len(reslist.Entries) > 0, "Unexpected: no objects in the bucket %s", bck.String())

	var (
		version string
		ok      bool
	)
	tlog.Logfln("Checking object properties...")
	for _, m := range reslist.Entries {
		if version, ok = objs[m.Name]; !ok {
			continue
		}
		if !m.IsPresent() {
			t.Errorf("%s: not marked as cached one", bck.Cname(m.Name))
		}
		if m.Atime == "" {
			t.Errorf("%s: access time is empty", bck.Cname(m.Name))
		}
		if !versionEnabled {
			continue
		}
		if m.Version == "" {
			t.Errorf("Failed to read %s version", bck.Cname(m.Name))
		} else if version != m.Version {
			t.Errorf("%s versions mismatch: expected [%s], have[%s]", bck.Cname(m.Name), version, m.Version)
		}
	}
}

func propsRebalance(t *testing.T, proxyURL string, bck cmn.Bck, objects map[string]string, msg *apc.LsoMsg,
	versionEnabled bool, cksumType string) {
	baseParams := tools.BaseAPIParams(proxyURL)
	propsCleanupObjects(t, proxyURL, bck, objects)

	smap := tools.GetClusterMap(t, proxyURL)
	origActiveTargetCnt := smap.CountActiveTs()
	if origActiveTargetCnt < 2 {
		t.Skipf("Only %d targets found, need at least 2", origActiveTargetCnt)
	}

	removeTarget, _ := smap.GetRandTarget()

	args := &apc.ActValRmNode{DaemonID: removeTarget.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(
		proxyURL,
		"target removed",
		smap.Version,
		smap.CountActivePs(),
		smap.CountActiveTs()-1,
	)
	tassert.CheckFatal(t, err)

	// rewrite objects and compare versions - they should change
	newobjs := propsUpdateObjects(t, proxyURL, bck, objects, msg, versionEnabled, cksumType)

	args = &apc.ActValRmNode{DaemonID: removeTarget.ID()}
	rebID, err := api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(
		proxyURL,
		"target joined",
		smap.Version,
		smap.CountActivePs(),
		smap.CountActiveTs()+1,
	)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, baseParams, rebID)

	tlog.Logfln("Listing objects...")
	reslist := testListObjects(t, proxyURL, bck, msg)
	tassert.Fatalf(t, reslist != nil && len(reslist.Entries) > 0, "Unexpected: no objects in the bucket %s", bck.String())

	var (
		version  string
		ok       bool
		objFound int
	)
	tlog.Logfln("Checking object properties...")
	for _, m := range reslist.Entries {
		if version, ok = newobjs[m.Name]; !ok {
			continue
		}
		if !m.IsStatusOK() {
			continue
		}
		objFound++
		if !m.IsPresent() && bck.IsRemote() {
			t.Errorf("%s: not marked as cached one", bck.Cname(m.Name))
		}
		if m.Atime == "" {
			t.Errorf("%s: access time is empty", bck.Cname(m.Name))
		}
		if !versionEnabled {
			continue
		}
		if version != m.Version {
			t.Errorf("%s post-rebalance version mismatch: have [%s], expected [%s]", bck.Cname(m.Name), m.Version, version)
		}
	}

	if objFound != len(objects) {
		t.Errorf("Wrong number of objects after rebalance: have %d, expected %d", objFound, len(objects))
	}
}

func propsCleanupObjects(t *testing.T, proxyURL string, bck cmn.Bck, newVersions map[string]string) {
	errCh := make(chan error, 100)
	wg := &sync.WaitGroup{}
	for objName := range newVersions {
		wg.Add(1)
		go tools.Del(proxyURL, bck, objName, wg, errCh, true /*silent*/)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", true)
	close(errCh)
}

func TestObjPropsVersion(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	for _, versioning := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", versioning), func(t *testing.T) {
			propsVersionAllProviders(t, versioning)
		})
	}
}

func TestObjChunkedOverride(t *testing.T) {
	var (
		proxyURL = tools.RandomProxyURL()
		// baseParams = tools.BaseAPIParams(proxyURL)
		bck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Test all 4 permutations of chunked vs monolithic uploads and overrides
	testCases := []struct {
		name              string
		firstUploadChunks bool
		overrideChunks    bool
	}{
		{"monolithic-to-monolithic", false, false},
		{"chunked-to-monolithic", true, false},
		{"monolithic-to-chunked", false, true},
		{"chunked-to-chunked", true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testChunkedOverride(t, baseParams, bck, tc.firstUploadChunks, tc.overrideChunks)
		})
	}
}

// testChunkedOverride tests object upload and override with different chunk configurations
func testChunkedOverride(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, firstChunked, overrideChunked bool) {
	const (
		objPrefix = "test-chunked-override"
		numObjs   = 100
	)

	// Create ioContext for first upload
	m := ioContext{
		t:             t,
		bck:           bck,
		num:           numObjs,
		prefix:        objPrefix + trand.String(10),
		fileSizeRange: [2]uint64{32 * cos.KiB, 8 * cos.MiB},
	}

	if testing.Short() {
		m.num /= 10
		m.fileSizeRange[1] /= 64
	}

	// Set chunking configuration for first upload
	if firstChunked {
		m.chunksConf = &ioCtxChunksConf{
			numChunks: 4, // Split into 4 chunks
			multipart: true,
		}
	} else {
		m.chunksConf = &ioCtxChunksConf{multipart: false} // explicitly disable chunking
	}

	m.init(true /*cleanup*/)
	initMountpaths(t, proxyURL)
	m.puts()

	if overrideChunked {
		m.chunksConf = &ioCtxChunksConf{
			numChunks: 4, // Split into 4 chunks
			multipart: true,
		}
	} else {
		m.chunksConf = &ioCtxChunksConf{multipart: false} // explicitly disable chunking
	}

	p, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	for i := range len(m.objNames) {
		m.updateAndValidate(baseParams, i, p.Cksum.Type)

		// verify that the object's version is incremented after being overridden
		op, err := api.HeadObject(baseParams, bck, m.objNames[i], api.HeadArgs{FltPresence: apc.FltPresent})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, op.Version() == "2", "Expected version 2 for %s, got %s", m.objNames[i], op.Version())

		// after update, we should have exactly `numChunks-1` number of chunks on disk; previous chunks associated with this object should be cleaned up
		if overrideChunked {
			fqns := m.findObjChunksOnDisk(bck, m.objNames[i])
			tassert.Fatalf(t, len(fqns) == m.chunksConf.numChunks-1, "Expected %d chunks on disk for %s, got %d", m.chunksConf.numChunks-1, m.objNames[i], len(fqns))
		}
	}

	m.gets(nil, true)
	m.ensureNoGetErrors()

	tlog.Logfln("Successfully completed test: first_chunked=%t, override_chunked=%t", firstChunked, overrideChunked)
}

func propsVersionAllProviders(t *testing.T, versioning bool) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		config := tools.GetClusterConfig(t)

		oldChkVersion := config.Versioning.ValidateWarmGet
		oldVersioning := config.Versioning.Enabled

		newConfig := make(cos.StrKVs)
		if oldVersioning != versioning {
			newConfig[cmn.PropBucketVerEnabled] = strconv.FormatBool(versioning)
		}
		warmCheck := versioning
		if oldChkVersion != warmCheck {
			newConfig["versioning.validate_warm_get"] = strconv.FormatBool(warmCheck)
		}
		if len(newConfig) != 0 {
			tools.SetClusterConfig(t, newConfig)
		}

		defer func() {
			// restore configuration
			newConfig := make(cos.StrKVs)
			oldWarmCheck := oldChkVersion && oldVersioning
			if oldWarmCheck != warmCheck {
				newConfig["versioning.validate_warm_get"] = strconv.FormatBool(oldWarmCheck)
			}
			if oldVersioning != versioning {
				newConfig[cmn.PropBucketVerEnabled] = strconv.FormatBool(oldVersioning)
			}
			if len(newConfig) != 0 {
				tools.SetClusterConfig(t, newConfig)
			}
		}()

		if b := bck.RemoteBck(); b != nil && b.Provider == apc.AWS {
			// needed for the test
			// reminder:
			// "when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)"
			var (
				of = bck.Props.Features
				nf = feat.S3ListObjectVersions
			)
			props := &cmn.BpropsToSet{Features: &nf}
			_, err := api.SetBucketProps(baseParams, bck.Clone(), props)
			tassert.CheckFatal(t, err)
			defer func() {
				props := &cmn.BpropsToSet{Features: &of}
				_, err := api.SetBucketProps(baseParams, bck.Clone(), props)
				tassert.CheckFatal(t, err)
			}()
		}

		propsVersion(t, bck.Clone(), bck.Props.Versioning.Enabled, bck.Props.Cksum.Type)
	})
}

func propsVersion(t *testing.T, bck cmn.Bck, versionEnabled bool, cksumType string) {
	var (
		m = ioContext{
			t:                   t,
			bck:                 bck,
			num:                 15,
			fileSize:            cos.KiB,
			prefix:              "props/obj-",
			deleteRemoteBckObjs: true,
		}
		proxyURL = tools.RandomProxyURL()
	)

	m.init(true /*cleanup*/)
	if m.bck.IsRemote() {
		m.del(-1 /* delete all */)
	}
	m.puts()
	// Read object versions.
	msg := &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsVersion, apc.GetPropsAtime, apc.GetPropsStatus)
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no objects in the bucket %s", bck.String())
		return
	}

	// PUT objects must have all properties set: atime, cached, version
	filesList := make(map[string]string)
	for _, m := range reslist.Entries {
		tlog.Logfln("%s initial version:\t%q", bck.Cname(m.Name), m.Version)

		if !m.IsPresent() && bck.IsRemote() {
			t.Errorf("%s: not marked as _cached_", bck.Cname(m.Name))
		}
		if m.Atime == "" {
			t.Errorf("%s: access time is empty", bck.Cname(m.Name))
		}
		filesList[m.Name] = m.Version
		if !versionEnabled {
			continue
		}
		if m.Version == "" {
			t.Fatalf("Failed to read %s version", bck.Cname(m.Name))
		}
	}

	// rewrite objects and compare versions - they should change
	newVersions := propsUpdateObjects(t, proxyURL, bck, filesList, msg, versionEnabled, cksumType)

	// check that files are read from cache
	propsReadObjects(t, proxyURL, bck, filesList)

	// TODO: this should work for the remote cluster as well
	if bck.IsCloud() {
		// try to evict some files and check if they are gone
		propsEvict(t, proxyURL, bck, newVersions, msg, versionEnabled)

		// read objects to put them to the cache. After that all objects must have cached=true
		propsRecacheObjects(t, proxyURL, bck, newVersions, msg, versionEnabled)
	}

	// test rebalance should keep object versions
	propsRebalance(t, proxyURL, bck, newVersions, msg, versionEnabled, cksumType)

	// cleanup
	propsCleanupObjects(t, proxyURL, bck, newVersions)
}

func TestObjProps(t *testing.T) {
	const (
		typeLocal     = "local"
		typeRemoteAIS = "remoteAIS"
		typeCloud     = "cloud"
	)
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)

		tests = []struct {
			bucketType   string
			checkPresent bool
			verEnabled   bool
			evict        bool
		}{
			{checkPresent: true, bucketType: typeLocal},
			{checkPresent: true, bucketType: typeCloud, evict: false},
			{checkPresent: true, bucketType: typeCloud, evict: true},

			{checkPresent: false, verEnabled: false, bucketType: typeLocal},
			{checkPresent: false, verEnabled: true, bucketType: typeLocal},

			{checkPresent: false, verEnabled: false, bucketType: typeRemoteAIS, evict: false},
			{checkPresent: false, verEnabled: false, bucketType: typeRemoteAIS, evict: true},

			{checkPresent: false, verEnabled: false, bucketType: typeLocal},
			{checkPresent: false, verEnabled: true, bucketType: typeLocal},

			{checkPresent: false, verEnabled: false, bucketType: typeCloud, evict: false},
			{checkPresent: false, verEnabled: false, bucketType: typeCloud, evict: true},
			// valid only if the cloud bucket has versioning enabled
			{checkPresent: false, verEnabled: true, bucketType: typeCloud, evict: false},
			{checkPresent: false, verEnabled: true, bucketType: typeCloud, evict: true},
		}
	)

	for _, test := range tests {
		name := fmt.Sprintf(
			"checkPresent=%t/verEnabled=%t/type=%s/evict=%t",
			test.checkPresent, test.verEnabled, test.bucketType, test.evict,
		)
		t.Run(name, func(t *testing.T) {
			m := ioContext{
				t:         t,
				num:       10,
				fileSize:  512,
				fixedSize: true,
				prefix:    "props/obj-",
			}

			m.init(true /*cleanup*/)

			switch test.bucketType {
			case typeCloud:
				m.bck = cliBck
				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
			case typeLocal:
				tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)
			case typeRemoteAIS:
				tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
				m.bck.Ns.UUID = tools.RemoteCluster.UUID
				tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)
			default:
				tassert.CheckFatal(t, fmt.Errorf("unknown type %q", test.bucketType))
			}

			defaultBckProp, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
			tassert.CheckFatal(t, err)

			_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
				Versioning: &cmn.VersionConfToSet{
					Enabled: apc.Ptr(test.verEnabled),
				},
			})
			if test.bucketType == typeCloud && test.verEnabled != defaultBckProp.Versioning.Enabled {
				s := "versioned"
				if !defaultBckProp.Versioning.Enabled {
					s = "unversioned"
				}
				tassert.Errorf(
					t, err != nil,
					"Cloud bucket %s is %s - expecting set-props to fail", m.bck.String(), s)
			} else {
				tassert.CheckFatal(t, err)
			}
			if test.bucketType == typeCloud || test.bucketType == typeRemoteAIS {
				m.remotePuts(test.evict)
				defer api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
					Versioning: &cmn.VersionConfToSet{
						Enabled: apc.Ptr(defaultBckProp.Versioning.Enabled),
					},
				})
			} else {
				m.puts()
				m.gets(nil, false) // set the access time
			}

			bckProps, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
			tassert.CheckFatal(t, err)

			for _, objName := range m.objNames {
				tlog.Logfln("checking %s props...", m.bck.Cname(objName))

				flt := apc.FltPresent
				if test.checkPresent {
					flt = apc.FltPresentNoProps
				}
				if test.bucketType != typeLocal && test.evict && !test.checkPresent {
					flt = apc.FltExistsOutside
				}

				props, err := api.HeadObject(baseParams, m.bck, objName, api.HeadArgs{FltPresence: flt})
				if test.checkPresent {
					if test.bucketType != typeLocal && test.evict {
						tassert.Fatalf(t, err != nil,
							"object should be marked as 'not exists' (it is not cached)")
					} else {
						tassert.CheckFatal(t, err)
					}
					tassert.Errorf(t, props == nil, "props should be empty")
					continue
				}
				tassert.CheckFatal(t, err)

				tassert.Errorf(
					t, props.Bck.Provider == bckProps.Provider,
					"expected provider (%s) to be %s", props.Bck.Provider, bckProps.Provider,
				)
				tassert.Errorf(
					t, uint64(props.Size) == m.fileSize,
					"object size (%d) is different from expected (%d)", props.Size, m.fileSize,
				)
				if test.bucketType != typeLocal {
					if test.evict {
						tassert.Errorf(t, !props.Present, "object should not be present (cached)")
					} else {
						tassert.Errorf(t, props.Present, "object should be present (cached)")
					}
					v := props.Version()
					if defaultBckProp.Versioning.Enabled && (test.verEnabled || test.evict) {
						tassert.Errorf(t, v != "", "%s object version should not be empty", test.bucketType)
					} else {
						tassert.Errorf(t, v == "" || defaultBckProp.Versioning.Enabled ||
							test.bucketType == typeRemoteAIS,
							"%s object version should be empty, have %q (enabled=%t)",
							test.bucketType, v, defaultBckProp.Versioning.Enabled)
					}
					if test.evict {
						tassert.Errorf(t, props.Atime == 0,
							"expected %s access time to be empty (not cached)", m.bck.Cname(objName))
					} else {
						tassert.Errorf(t, props.Atime != 0, "expected access time to be set (cached)")
					}
				} else {
					tassert.Errorf(t, props.Present, "object seems to be not present")
					tassert.Errorf(
						t, props.Mirror.Copies == 1,
						"number of copies (%d) is different than 1", props.Mirror.Copies,
					)
					v := props.Version()
					if test.verEnabled {
						tassert.Errorf(
							t, v == "1",
							"object version (%s) different than expected (1)", v,
						)
					} else {
						tassert.Errorf(t, v == "", "object version should be empty")
					}
					tassert.Errorf(t, props.Atime != 0, "expected access time to be set")
				}
				tassert.Errorf(t, !props.EC.IsECCopy, "expected object not to be ec copy")
				tassert.Errorf(
					t, props.EC.DataSlices == 0,
					"expected data slices (%d) to be 0", props.EC.DataSlices,
				)
				tassert.Errorf(
					t, props.EC.ParitySlices == 0,
					"expected parity slices (%d) to be 0", props.EC.ParitySlices,
				)
			}
		})
	}
}

func testListObjects(t *testing.T, proxyURL string, bck cmn.Bck, msg *apc.LsoMsg) *cmn.LsoRes {
	switch {
	case msg == nil:
		tlog.Logfln("LIST %s []", bck.String())
	case msg.Prefix == "" && msg.PageSize == 0 && msg.ContinuationToken == "":
		tlog.Logfln("LIST %s [cached: %t]", bck.String(), msg.IsFlagSet(apc.LsCached))
	default:
		tlog.Logfln("LIST %s [prefix: %q, page_size: %d, cached: %t, token: %q]",
			bck.String(), msg.Prefix, msg.PageSize, msg.IsFlagSet(apc.LsCached), msg.ContinuationToken)
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	resList, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.Fatalf(t, err == nil, "%s: list-objects failed: %v", bck.String(), err)
	return resList
}
