// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/stats"
)

func propsStats(t *testing.T, proxyURL string) (objChanged, bytesChanged int64) {
	cstats := tutils.GetClusterStats(t, proxyURL)
	objChanged = 0
	bytesChanged = 0

	for _, v := range cstats.Target {
		objChanged += tutils.GetNamedTargetStats(v, stats.VerChangeCount)
		bytesChanged += tutils.GetNamedTargetStats(v, stats.VerChangeSize)
	}
	return
}

func propsUpdateObjects(t *testing.T, proxyURL string, bck cmn.Bck, oldVersions map[string]string,
	msg *cmn.SelectMsg, versionEnabled bool, cksumType string) (newVersions map[string]string) {
	newVersions = make(map[string]string, len(oldVersions))
	tlog.Logf("Updating objects...\n")
	r, err := readers.NewRandReader(int64(fileSize), cksumType)
	if err != nil {
		t.Errorf("Failed to create reader: %v", err)
		t.Fail()
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for fname := range oldVersions {
		putArgs := api.PutObjectArgs{
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
		tlog.Logf("Object %s new version %s\n", m.Name, m.Version)
		newVersions[m.Name] = m.Version

		if !m.CheckExists() && bck.IsRemote() {
			t.Errorf("Object %s/%s is not marked as cached one", bck, m.Name)
		}
		if !versionEnabled {
			continue
		}

		if ver == m.Version {
			t.Errorf("Object %s/%s version has not changed", bck, m.Name)
			t.Fail()
		} else if m.Version == "" {
			t.Errorf("Object %s/%s version is empty", bck, m.Name)
			t.Fail()
		}
	}

	return
}

func propsReadObjects(t *testing.T, proxyURL string, bck cmn.Bck, objList map[string]string) {
	versChanged, bytesChanged := propsStats(t, proxyURL)
	tlog.Logf("Version mismatch stats before test. Objects: %d, bytes fetched: %d\n", versChanged, bytesChanged)

	baseParams := tutils.BaseAPIParams(proxyURL)
	for object := range objList {
		_, err := api.GetObject(baseParams, bck, object)
		if err != nil {
			t.Errorf("Failed to read %s/%s, err: %v", bck, object, err)
			continue
		}
	}

	versChangedFinal, bytesChangedFinal := propsStats(t, proxyURL)
	tlog.Logf("Version mismatch stats after test. Objects: %d, bytes fetched: %d\n", versChangedFinal, bytesChangedFinal)
	if versChanged != versChangedFinal || bytesChanged != bytesChangedFinal {
		t.Errorf("All objects must be retreived from the cache but cold get happened: %d times (%d bytes)",
			versChangedFinal-versChanged, bytesChangedFinal-bytesChanged)
		t.Fail()
	}
}

func propsEvict(t *testing.T, proxyURL string, bck cmn.Bck, objMap map[string]string, msg *cmn.SelectMsg, versionEnabled bool) {
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

	baseParams := tutils.BaseAPIParams(proxyURL)
	xactID, err := api.EvictList(baseParams, bck, toEvictList)
	if err != nil {
		t.Errorf("Failed to evict objects: %v\n", err)
		t.Fail()
	}
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActEvictObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
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

		// invalid object: rebalance leftover or uploaded directly to target
		if !m.IsStatusOK() {
			continue
		}

		if _, wasEvicted := evictMap[m.Name]; wasEvicted {
			if m.Atime != "" {
				t.Errorf("Evicted object %s/%s still has atime %q", bck, m.Name, m.Atime)
				t.Fail()
			}
			if m.CheckExists() {
				t.Errorf("Evicted object %s/%s is still marked as cached one", bck, m.Name)
				t.Fail()
			}
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("Object %s/%s version is empty", bck, m.Name)
			t.Fail()
		} else if m.Version != oldVersion {
			t.Errorf("Object %s/%s version has changed from %s to %s", bck, m.Name, oldVersion, m.Version)
			t.Fail()
		}
	}
}

func propsRecacheObjects(t *testing.T, proxyURL string, bck cmn.Bck, objs map[string]string, msg *cmn.SelectMsg, versionEnabled bool) {
	tlog.Logf("Refetching objects...\n")
	propsReadObjects(t, proxyURL, bck, objs)
	tlog.Logf("Checking objects properties after refetching...\n")
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no object in the bucket %s", bck)
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
			t.Errorf("Object %s/%s is not marked as cached one", bck, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bck, m.Name)
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("Failed to read object %s/%s version", bck, m.Name)
			t.Fail()
		} else if version != m.Version {
			t.Errorf("Object %s/%s versions mismatch: old[%s], new[%s]", bck, m.Name, version, m.Version)
			t.Fail()
		}
	}
}

func propsRebalance(t *testing.T, proxyURL string, bck cmn.Bck, objects map[string]string, msg *cmn.SelectMsg,
	versionEnabled bool, cksumType string) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	propsCleanupObjects(t, proxyURL, bck, objects)

	smap := tutils.GetClusterMap(t, proxyURL)
	l := smap.CountActiveTargets()
	if l < 2 {
		t.Skipf("Only %d targets found, need at least 2", l)
	}

	removeTarget, _ := smap.GetRandTarget()

	tlog.Logf("Removing a target: %s\n", removeTarget.ID())
	args := &cmn.ActValRmNode{DaemonID: removeTarget.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForClusterState(
		proxyURL,
		"target is gone",
		smap.Version,
		smap.CountActiveProxies(),
		smap.CountActiveTargets()-1,
	)
	tassert.CheckFatal(t, err)

	tlog.Logf("Target %s [%s] is removed\n", removeTarget.ID(), removeTarget.URL(cmn.NetworkPublic))

	// rewrite objects and compare versions - they should change
	newobjs := propsUpdateObjects(t, proxyURL, bck, objects, msg, versionEnabled, cksumType)

	tlog.Logf("Reregistering target...\n")
	args = &cmn.ActValRmNode{DaemonID: removeTarget.ID()}
	rebID, err := api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForClusterState(
		proxyURL,
		"to join target back",
		smap.Version,
		smap.CountActiveProxies(),
		smap.CountActiveTargets()+1,
	)
	tassert.CheckFatal(t, err)
	tutils.WaitForRebalanceByID(t, baseParams, rebID, rebalanceTimeout)

	tlog.Logf("Reading file versions...\n")
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no object in the bucket %s", bck)
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
			t.Errorf("Object %s/%s is not marked as cached one", bck, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bck, m.Name)
		}

		if !versionEnabled {
			continue
		}

		tlog.Logf("Object %s/%s, version before rebalance [%s], after [%s]\n", bck, m.Name, version, m.Version)
		if version != m.Version {
			t.Errorf("Object %s/%s version mismatch: existing [%s], expected [%s]", bck, m.Name, m.Version, version)
		}
	}

	if objFound != len(objects) {
		t.Errorf("The number of objects after rebalance differs for the number before it. Current: %d, expected %d", objFound, len(objects))
	}
}

func propsCleanupObjects(t *testing.T, proxyURL string, bck cmn.Bck, newVersions map[string]string) {
	errCh := make(chan error, 100)
	wg := &sync.WaitGroup{}
	for objName := range newVersions {
		wg.Add(1)
		go tutils.Del(proxyURL, bck, objName, wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", true)
	close(errCh)
}

func propsTestCore(t *testing.T, bck cmn.Bck, versionEnabled bool, cksumType string) {
	var (
		m = ioContext{
			t:        t,
			bck:      bck,
			num:      15,
			fileSize: cos.KiB,
		}
		proxyURL = tutils.RandomProxyURL()
	)

	m.init()
	m.puts()

	t.Cleanup(func() {
		m.del()
	})

	// Read object versions.
	msg := &cmn.SelectMsg{}
	msg.AddProps(cmn.GetPropsVersion, cmn.GetPropsAtime, cmn.GetPropsStatus)
	reslist := testListObjects(t, proxyURL, bck, msg)
	if reslist == nil {
		t.Fatalf("Unexpected error: no object in the bucket %s", bck)
		return
	}

	// PUT objects must have all properties set: atime, cached, version
	filesList := make(map[string]string)
	for _, m := range reslist.Entries {
		tlog.Logf("Initial version %s - %v\n", m.Name, m.Version)

		if !m.CheckExists() && bck.IsRemote() {
			t.Errorf("Object %s/%s is not marked as cached one", bck, m.Name)
		}

		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bck, m.Name)
		}

		filesList[m.Name] = m.Version
		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("Failed to read object %q version", m.Name)
			t.Fail()
		}
	}

	// rewrite objects and compare versions - they should change
	newVersions := propsUpdateObjects(t, proxyURL, bck, filesList, msg, versionEnabled, cksumType)
	if len(newVersions) != len(filesList) {
		t.Errorf("Number of objects mismatch. Expected: %d objects, after update: %d", len(filesList), len(newVersions))
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

func propsMainTest(t *testing.T, versioning bool) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		config := tutils.GetClusterConfig(t)

		oldChkVersion := config.Versioning.ValidateWarmGet
		oldVersioning := config.Versioning.Enabled

		newConfig := make(cos.SimpleKVs)
		if oldVersioning != versioning {
			newConfig[cmn.PropBucketVerEnabled] = strconv.FormatBool(versioning)
		}
		warmCheck := versioning
		if oldChkVersion != warmCheck {
			newConfig["versioning.validate_warm_get"] = strconv.FormatBool(warmCheck)
		}
		if len(newConfig) != 0 {
			tutils.SetClusterConfig(t, newConfig)
		}

		defer func() {
			// restore configuration
			newConfig := make(cos.SimpleKVs)
			oldWarmCheck := oldChkVersion && oldVersioning
			if oldWarmCheck != warmCheck {
				newConfig["versioning.validate_warm_get"] = strconv.FormatBool(oldWarmCheck)
			}
			if oldVersioning != versioning {
				newConfig[cmn.PropBucketVerEnabled] = strconv.FormatBool(oldVersioning)
			}
			if len(newConfig) != 0 {
				tutils.SetClusterConfig(t, newConfig)
			}
		}()

		propsTestCore(t, bck.Bck, bck.Props.Versioning.Enabled, bck.Props.Cksum.Type)
	})
}

func TestObjPropsVersion(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	for _, versioning := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", versioning), func(t *testing.T) {
			propsMainTest(t, versioning)
		})
	}
}

func TestObjProps(t *testing.T) {
	const (
		typeLocal     = "local"
		typeRemoteAIS = "remoteAIS"
		typeCloud     = "cloud"
	)
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)

		tests = []struct {
			bucketType  string
			checkExists bool
			verEnabled  bool
			evict       bool
		}{
			{checkExists: true, bucketType: typeLocal},
			{checkExists: true, bucketType: typeCloud, evict: false},
			{checkExists: true, bucketType: typeCloud, evict: true},

			{checkExists: false, verEnabled: false, bucketType: typeLocal},
			{checkExists: false, verEnabled: true, bucketType: typeLocal},

			{checkExists: false, verEnabled: false, bucketType: typeRemoteAIS, evict: false},
			{checkExists: false, verEnabled: false, bucketType: typeRemoteAIS, evict: true},

			{checkExists: false, verEnabled: false, bucketType: typeLocal},
			{checkExists: false, verEnabled: true, bucketType: typeLocal},

			{checkExists: false, verEnabled: false, bucketType: typeCloud, evict: false},
			{checkExists: false, verEnabled: false, bucketType: typeCloud, evict: true},
			// valid only if the cloud bucket has versioning enabled
			{checkExists: false, verEnabled: true, bucketType: typeCloud, evict: false},
			{checkExists: false, verEnabled: true, bucketType: typeCloud, evict: true},
		}
	)

	for _, test := range tests {
		name := fmt.Sprintf(
			"checkExists=%t/verEnabled=%t/type=%s/evict=%t",
			test.checkExists, test.verEnabled, test.bucketType, test.evict,
		)
		t.Run(name, func(t *testing.T) {
			m := ioContext{
				t:         t,
				num:       10,
				fileSize:  512,
				fixedSize: true,
			}

			m.init()

			switch test.bucketType {
			case typeCloud:
				m.bck = cliBck
				tutils.CheckSkip(t, tutils.SkipTestArgs{RemoteBck: true, Bck: m.bck})
			case typeLocal:
				tutils.CreateFreshBucket(t, proxyURL, m.bck, nil)
			case typeRemoteAIS:
				tutils.CheckSkip(t, tutils.SkipTestArgs{RequiresRemoteCluster: true})
				m.bck.Ns.UUID = tutils.RemoteCluster.UUID
				tutils.CreateFreshBucket(t, proxyURL, m.bck, nil)
			default:
				tassert.CheckFatal(t, fmt.Errorf("unknown type %q", test.bucketType))

				defaultBckProp, err := api.HeadBucket(baseParams, m.bck)
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
				if test.bucketType == typeCloud {
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

				bckProps, err := api.HeadBucket(baseParams, m.bck)
				tassert.CheckFatal(t, err)

				tlog.Logln("checking object props...")
				for _, objName := range m.objNames {
					props, err := api.HeadObject(baseParams, m.bck, objName, test.checkExists)
					if test.checkExists {
						if test.bucketType != typeLocal && test.evict {
							tassert.Fatalf(t, err != nil, "object should be marked as 'not exists' (it is not cached)")
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
							tassert.Errorf(t, !props.Present, "object should not be present (not cached)")
						} else {
							tassert.Errorf(t, props.Present, "object should be present (cached)")
						}
						if defaultBckProp.Versioning.Enabled && (test.verEnabled || test.evict) {
							tassert.Errorf(t, props.Version != "", "cloud object version should not be empty")
						} else {
							tassert.Errorf(t, props.Version == "" || defaultBckProp.Versioning.Enabled,
								"cloud object version should be empty")
						}
						if test.evict {
							tassert.Errorf(t, props.Atime == 0, "expected access time to be empty (not cached)")
						} else {
							tassert.Errorf(t, props.Atime != 0, "expected access time to be set (cached)")
						}
					} else {
						tassert.Errorf(t, props.Present, "object seems to be not present")
						tassert.Errorf(
							t, props.NumCopies == 1,
							"number of copies (%d) is different than 1", props.NumCopies,
						)
						if test.verEnabled {
							tassert.Errorf(
								t, props.Version == "1",
								"object version (%s) different than expected (1)", props.Version,
							)
						} else {
							tassert.Errorf(t, props.Version == "", "object version should be empty")
						}
						tassert.Errorf(t, props.Atime != 0, "expected access time to be set")
					}
					tassert.Errorf(t, !props.IsECCopy, "expected object not to be ec copy")
					tassert.Errorf(
						t, props.DataSlices == 0,
						"expected data slices (%d) to be 0", props.DataSlices,
					)
					tassert.Errorf(
						t, props.ParitySlices == 0,
						"expected parity slices (%d) to be 0", props.ParitySlices,
					)
				}
			}
		})
	}
}
