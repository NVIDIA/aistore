// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
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
	r, err := readers.NewRandReader(int64(fileSize), cksumType)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	for fname := range oldVersions {
		putArgs := api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     fname,
			Cksum:      r.Cksum(),
			Reader:     r,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			t.Errorf("Failed to put new data to object %s/%s, err: %v", bck, fname, err)
		}
	}

	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		return
	}

	var (
		ver string
		ok  bool
	)
	for _, m := range reslist.Entries {
		if ver, ok = oldVersions[m.Name]; !ok {
			continue
		}
		newVersions[m.Name] = m.Version

		if !m.CheckExists() && bck.IsRemote() {
			t.Errorf("%s/%s: not marked as cached one", bck, m.Name)
		}
		if !versionEnabled {
			continue
		}
		if ver == m.Version {
			t.Fatalf("%s/%s: version was expected to update", bck, m.Name)
		} else if m.Version == "" {
			t.Fatalf("%s/%s: version is empty", bck, m.Name)
		}
	}

	return
}

func propsReadObjects(t *testing.T, proxyURL string, bck cmn.Bck, objList map[string]string) {
	versChanged, bytesChanged := propsStats(t, proxyURL)
	baseParams := tools.BaseAPIParams(proxyURL)
	for objName := range objList {
		_, err := api.GetObject(baseParams, bck, objName, nil)
		if err != nil {
			t.Errorf("Failed to read %s/%s: %v", bck, objName, err)
			continue
		}
	}
	versChangedFinal, bytesChangedFinal := propsStats(t, proxyURL)
	if versChanged != versChangedFinal || bytesChanged != bytesChangedFinal {
		t.Fatalf("All objects must be retreived from the cache but cold get happened: %d times (%d bytes)",
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
	tlog.Logf("Evicting %v objects:\n", toEvict)

	for fname := range objMap {
		evictMap[fname] = true
		toEvictList = append(toEvictList, fname)
		tlog.Logf("    %s/%s\n", bck, fname)
		if len(toEvictList) >= toEvict {
			break
		}
	}

	baseParams := tools.BaseAPIParams(proxyURL)
	xactID, err := api.EvictList(baseParams, bck, toEvictList)
	if err != nil {
		t.Errorf("Failed to evict objects: %v\n", err)
	}
	args := api.XactReqArgs{ID: xactID, Kind: apc.ActEvictObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)

	tlog.Logf("Reading object list...\n")

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
		tlog.Logf("%s/%s [%d] - cached: [%v], atime [%v]\n", bck, m.Name, m.Flags, m.CheckExists(), m.Atime)

		// e.g. misplaced replica
		if !m.IsStatusOK() {
			continue
		}

		if _, wasEvicted := evictMap[m.Name]; wasEvicted {
			if m.Atime != "" {
				t.Errorf("Evicted object %s/%s still has atime %q", bck, m.Name, m.Atime)
			}
			if m.CheckExists() {
				t.Errorf("Evicted object %s/%s is still marked as cached one", bck, m.Name)
			}
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("%s/%s: version is empty", bck, m.Name)
		} else if m.Version != oldVersion {
			t.Errorf("%s/%s: version has changed from %s to %s", bck, m.Name, oldVersion, m.Version)
		}
	}
}

func propsRecacheObjects(t *testing.T, proxyURL string, bck cmn.Bck, objs map[string]string, msg *apc.LsoMsg, versionEnabled bool) {
	tlog.Logf("Reading...\n")
	propsReadObjects(t, proxyURL, bck, objs)
	tlog.Logf("Checking object properties...\n")
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no objects in the bucket %s", bck)
	}
	var (
		version string
		ok      bool
	)
	for _, m := range reslist.Entries {
		if version, ok = objs[m.Name]; !ok {
			continue
		}
		if !m.CheckExists() {
			t.Errorf("%s/%s:not marked as cached one", bck, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("%s/%s: access time is empty", bck, m.Name)
		}
		if !versionEnabled {
			continue
		}
		if m.Version == "" {
			t.Errorf("Failed to read %s/%s version", bck, m.Name)
		} else if version != m.Version {
			t.Errorf("%s/%s versions mismatch: expected [%s], have[%s]", bck, m.Name, version, m.Version)
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
	tools.WaitForRebalanceByID(t, origActiveTargetCnt, baseParams, rebID, rebalanceTimeout)

	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no objects in the bucket %s", bck)
	}
	var (
		version  string
		ok       bool
		objFound int
	)
	for _, m := range reslist.Entries {
		if version, ok = newobjs[m.Name]; !ok {
			continue
		}
		if !m.IsStatusOK() {
			continue
		}
		objFound++
		if !m.CheckExists() && bck.IsRemote() {
			t.Errorf("%s/%s: not marked as cached one", bck, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("%s/%s: access time is empty", bck, m.Name)
		}
		if !versionEnabled {
			continue
		}
		if version != m.Version {
			t.Errorf("%s/%s post-rebalance version mismatch: have [%s], expected [%s]", bck, m.Name, m.Version, version)
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
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	for _, versioning := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", versioning), func(t *testing.T) {
			propsVersionAllProviders(t, versioning)
		})
	}
}

func propsVersionAllProviders(t *testing.T, versioning bool) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		config := tools.GetClusterConfig(t)

		oldChkVersion := config.Versioning.ValidateWarmGet
		oldVersioning := config.Versioning.Enabled

		newConfig := make(cos.StrKVs)
		if oldVersioning != versioning {
			newConfig[apc.PropBucketVerEnabled] = strconv.FormatBool(versioning)
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
				newConfig[apc.PropBucketVerEnabled] = strconv.FormatBool(oldVersioning)
			}
			if len(newConfig) != 0 {
				tools.SetClusterConfig(t, newConfig)
			}
		}()

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

	m.initWithCleanup()
	if m.bck.IsRemote() {
		m.del(-1 /* delete all */)
	}
	m.puts()
	// Read object versions.
	msg := &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsVersion, apc.GetPropsAtime, apc.GetPropsStatus)
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no objects in the bucket %s", bck)
		return
	}

	// PUT objects must have all properties set: atime, cached, version
	filesList := make(map[string]string)
	for _, m := range reslist.Entries {
		tlog.Logf("%s/%s initial version:\t%q\n", bck, m.Name, m.Version)

		if !m.CheckExists() && bck.IsRemote() {
			t.Errorf("%s/%s: not marked as cached one", bck, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("%s/%s: access time is empty", bck, m.Name)
		}
		filesList[m.Name] = m.Version
		if !versionEnabled {
			continue
		}
		if m.Version == "" {
			t.Fatalf("Failed to read %s/%s version", bck, m.Name)
		}
	}

	// rewrite objects and compare versions - they should change
	newVersions := propsUpdateObjects(t, proxyURL, bck, filesList, msg, versionEnabled, cksumType)
	if len(newVersions) != len(filesList) {
		t.Errorf("Wrong number of objects: expected %d, have %d", len(filesList), len(newVersions))
	}

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

			m.initWithCleanup()

			switch test.bucketType {
			case typeCloud:
				m.bck = cliBck
				tools.CheckSkip(t, tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
			case typeLocal:
				tools.CreateBucketWithCleanup(t, proxyURL, m.bck, nil)
			case typeRemoteAIS:
				tools.CheckSkip(t, tools.SkipTestArgs{RequiresRemoteCluster: true})
				m.bck.Ns.UUID = tools.RemoteCluster.UUID
				tools.CreateBucketWithCleanup(t, proxyURL, m.bck, nil)
			default:
				tassert.CheckFatal(t, fmt.Errorf("unknown type %q", test.bucketType))
			}

			defaultBckProp, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
			tassert.CheckFatal(t, err)

			_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BucketPropsToUpdate{
				Versioning: &cmn.VersionConfToUpdate{
					Enabled: api.Bool(test.verEnabled),
				},
			})
			if test.bucketType == typeCloud && test.verEnabled != defaultBckProp.Versioning.Enabled {
				s := "versioned"
				if !defaultBckProp.Versioning.Enabled {
					s = "unversioned"
				}
				tassert.Errorf(
					t, err != nil,
					"Cloud bucket %s is %s - expecting set-props to fail", m.bck, s)
			} else {
				tassert.CheckFatal(t, err)
			}
			if test.bucketType == typeCloud || test.bucketType == typeRemoteAIS {
				m.remotePuts(test.evict)
				defer api.SetBucketProps(baseParams, m.bck, &cmn.BucketPropsToUpdate{
					Versioning: &cmn.VersionConfToUpdate{
						Enabled: api.Bool(defaultBckProp.Versioning.Enabled),
					},
				})
			} else {
				m.puts()
				m.gets() // set the access time
			}

			bckProps, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
			tassert.CheckFatal(t, err)

			for _, objName := range m.objNames {
				tlog.Logf("checking %s/%s object props...\n", m.bck, objName)

				flt := apc.FltPresent
				if test.checkPresent {
					flt = apc.FltPresentNoProps
				}
				if test.bucketType != typeLocal && test.evict && !test.checkPresent {
					flt = apc.FltExistsOutside
				}

				props, err := api.HeadObject(baseParams, m.bck, objName, flt)
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
					"object size (%d) is different than expected (%d)", props.Size, m.fileSize,
				)
				if test.bucketType != typeLocal {
					if test.evict {
						tassert.Errorf(t, !props.Present, "object should not be present (cached)")
					} else {
						tassert.Errorf(t, props.Present, "object should be present (cached)")
					}
					if defaultBckProp.Versioning.Enabled && (test.verEnabled || test.evict) {
						tassert.Errorf(t, props.Ver != "", "%s object version should not be empty", test.bucketType)
					} else {
						tassert.Errorf(t, props.Ver == "" || defaultBckProp.Versioning.Enabled ||
							test.bucketType == typeRemoteAIS,
							"%s object version should be empty, have %q (enabled=%t)",
							test.bucketType, props.Ver, defaultBckProp.Versioning.Enabled)
					}
					if test.evict {
						tassert.Errorf(t, props.Atime == 0,
							"expected %s/%s access time to be empty (not cached)", m.bck, objName)
					} else {
						tassert.Errorf(t, props.Atime != 0, "expected access time to be set (cached)")
					}
				} else {
					tassert.Errorf(t, props.Present, "object seems to be not present")
					tassert.Errorf(
						t, props.Mirror.Copies == 1,
						"number of copies (%d) is different than 1", props.Mirror.Copies,
					)
					if test.verEnabled {
						tassert.Errorf(
							t, props.Ver == "1",
							"object version (%s) different than expected (1)", props.Ver,
						)
					} else {
						tassert.Errorf(t, props.Ver == "", "object version should be empty")
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

func testListObjects(t *testing.T, proxyURL string, bck cmn.Bck, msg *apc.LsoMsg) *cmn.LsoResult {
	if msg == nil {
		tlog.Logf("LIST %s []\n", bck)
	} else if msg.Prefix == "" && msg.PageSize == 0 && msg.ContinuationToken == "" {
		tlog.Logf("LIST %s [cached: %t]\n", bck, msg.IsFlagSet(apc.LsObjCached))
	} else {
		tlog.Logf("LIST %s [prefix: %q, page_size: %d, cached: %t, token: %q]\n",
			bck, msg.Prefix, msg.PageSize, msg.IsFlagSet(apc.LsObjCached), msg.ContinuationToken)
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	resList, err := api.ListObjects(baseParams, bck, msg, 0)
	if err != nil {
		t.Errorf("List objects %s failed, err = %v", bck, err)
		return nil
	}
	return resList
}
