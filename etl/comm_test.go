// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
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
		_, err = fs.Add(mpath, "daeID")
		Expect(err).NotTo(HaveOccurred())

		tMock = cluster.NewTargetMock(bmdMock)
		// cluster.InitLomLocker(tMock)

		// Create an object.
		lom := &cluster.LOM{ObjName: objName}
		err = lom.Init(clusterBck.Bck)
		Expect(err).NotTo(HaveOccurred())
		err = createRandomFile(lom.GetFQN(), dataSize)
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
			err := comm.Do(w, r, clusterBck, objName)
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
		PushCommType,
		RedirectCommType,
		RevProxyCommType,
	}

	for _, commType := range tests {
		It("should perform transformation "+commType, func() {
			pod := &corev1.Pod{}
			pod.SetName("somename")

			comm = makeCommunicator(commArgs{
				t:              tMock,
				pod:            pod,
				commType:       commType,
				transformerURL: transformerServer.URL,
			})
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

// Creates a file with random content.
func createRandomFile(fileName string, size int64) error {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	f, err := cmn.CreateFile(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := f.Write(b)
	if err != nil {
		return err
	}
	if int64(n) != size {
		return fmt.Errorf("could not write %d bytes, wrote %d bytes", size, n)
	}
	return nil
}
