// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"crypto/rand"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("CommunicatorTest", func() {
	var (
		tmpDir            string
		comm              Communicator
		tMock             cluster.Target
		transformerServer *httptest.Server
		targetServer      *httptest.Server
		proxyServer       *httptest.Server

		dataSize      = int64(cmn.MiB * 50)
		transformData = make([]byte, dataSize)

		bck        = cmn.Bck{Name: "commBck", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		objName    = "commObj"
		clusterBck = cluster.NewBck(
			bck.Name, bck.Provider, bck.Ns,
			&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}},
		)
		bmdMock = cluster.NewBaseBownerMock(clusterBck)
	)

	BeforeEach(func() {
		// Generate random data.
		_, err := rand.Read(transformData)
		Expect(err).NotTo(HaveOccurred())

		// Initialize the filesystem and directories.
		tmpDir, err = ioutil.TempDir("", "")
		Expect(err).NotTo(HaveOccurred())

		mpath := filepath.Join(tmpDir, "mpath")
		err = cmn.CreateDir(mpath)
		Expect(err).NotTo(HaveOccurred())
		fs.Init()
		fs.DisableFsIDCheck()
		Expect(fs.Add(mpath)).NotTo(HaveOccurred())

		tMock = cluster.NewTargetMock(bmdMock)

		// Create an object.
		lom := &cluster.LOM{T: tMock, ObjName: objName}
		err = lom.Init(clusterBck.Bck)
		Expect(err).NotTo(HaveOccurred())
		err = tutils.CreateRandomFile(lom.GetFQN(), dataSize)
		Expect(err).NotTo(HaveOccurred())
		lom.SetSize(dataSize)
		err = lom.Persist()
		Expect(err).NotTo(HaveOccurred())

		// Initialize the HTTP servers.
		transformerServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write(transformData)
			Expect(err).NotTo(HaveOccurred())
		}))
		targetServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := comm.DoTransform(w, r, clusterBck, objName)
			Expect(err).NotTo(HaveOccurred())
		}))
		proxyServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, targetServer.URL, http.StatusMovedPermanently)
		}))

	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)
		proxyServer.Close()
		transformerServer.Close()
		targetServer.Close()
	})

	tests := []string{
		pushCommType,
		redirectCommType,
		revProxyCommType,
	}

	for _, commType := range tests {
		It("should perform transformation "+commType, func() {
			pod := &corev1.Pod{}
			pod.SetName("somename")

			comm = makeCommunicator(tMock, pod, commType, transformerServer.URL, "")
			resp, err := http.Get(proxyServer.URL)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			b, err := ioutil.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(b)).To(Equal(len(transformData)))
			Expect(b).To(Equal(transformData))
		})
	}
})
