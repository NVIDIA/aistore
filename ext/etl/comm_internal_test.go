// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	cryptorand "crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("CommunicatorTest", func() {
	var (
		tmpDir            string
		comm              HTTPCommunicator
		transformerServer *httptest.Server
		targetServer      *httptest.Server
		proxyServer       *httptest.Server

		dataSize      = int64(50 * cos.MiB)
		transformData = make([]byte, dataSize)

		bck                      = cmn.Bck{Name: "commBck", Provider: apc.AIS, Ns: cmn.NsGlobal}
		objName                  = "commObj"
		etlTransformArgs         = "{\"from_time\":2.43,\"to_time\":3.43}"
		expectedEtlTransformArgs = ""
		paramLatestVer           = "false"
		clusterBck               = meta.NewBck(
			bck.Name, bck.Provider, bck.Ns,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumCesXxh}},
		)
		bmdMock = mock.NewBaseBownerMock(clusterBck)
	)

	BeforeEach(func() {
		// Generate random data.
		_, err := cryptorand.Read(transformData)
		Expect(err).NotTo(HaveOccurred())

		// Initialize the filesystem and directories.
		tmpDir, err = os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		mpath := filepath.Join(tmpDir, "mpath")
		err = cos.CreateDir(mpath)
		Expect(err).NotTo(HaveOccurred())
		fs.TestNew(nil)
		_, err = fs.Add(mpath, "daeID")
		Expect(err).NotTo(HaveOccurred())

		_ = mock.NewTarget(bmdMock)
		// cluster.InitLomLocker(tMock)

		// Create an object.
		lom := &core.LOM{ObjName: objName}
		err = lom.InitBck(clusterBck.Bucket())
		Expect(err).NotTo(HaveOccurred())
		err = createRandomFile(lom.FQN, dataSize)
		Expect(err).NotTo(HaveOccurred())
		lom.SetAtimeUnix(time.Now().UnixNano())
		lom.SetSize(dataSize)
		err = lom.Persist()
		Expect(err).NotTo(HaveOccurred())

		// Initialize the HTTP servers.
		transformerServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedEtlTransformArgs := r.URL.Query().Get(apc.QparamETLTransformArgs)
			Expect(receivedEtlTransformArgs).To(Equal(expectedEtlTransformArgs))

			switch r.Method {
			case http.MethodGet: // hpull
				latestVer, err := cos.ParseBool(r.URL.Query().Get(apc.QparamLatestVer))
				Expect(err).NotTo(HaveOccurred())
				Expect(latestVer).To(BeFalse(), "Expected 'latestVer' parameter in hpull")

			case http.MethodPut: // hpush
				hasLatestVer := r.URL.Query().Has(apc.QparamLatestVer)
				// 'latestVer' should not be passed to ETL in hpush since targets handle synchronization during read.
				Expect(hasLatestVer).To(BeFalse(), "Unexpected 'latestVer' parameter in hpush; sync is handled by the target.")
			}

			_, err := w.Write(transformData)
			Expect(err).NotTo(HaveOccurred())
		}))
		targetServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedEtlTransformArgs := r.URL.Query().Get(apc.QparamETLTransformArgs)
			ver := r.URL.Query().Get(apc.QparamLatestVer)
			val, err := cos.ParseBool(ver)
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(BeFalse())

			ecode, err := comm.InlineTransform(w, r, lom, receivedEtlTransformArgs)
			Expect(err).NotTo(HaveOccurred())
			Expect(ecode).To(Equal(0))
		}))
		proxyServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirectURL := targetServer.URL + r.URL.Path + "?"
			if r.URL.RawQuery != "" {
				redirectURL += r.URL.RawQuery + "&"
			}
			http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
		}))
	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)
		proxyServer.Close()
		transformerServer.Close()
		targetServer.Close()
	})

	tests := []string{Hpush, Hpull}

	for _, testType := range []string{"inline", "offline"} {
		for _, commType := range tests {
			It("should perform "+testType+" transformation "+commType, func() {
				pod := &corev1.Pod{}
				pod.SetName("somename")

				xctn := mock.NewXact(apc.ActETLInline)
				boot := &etlBootstrapper{
					msg: InitSpecMsg{
						InitMsgBase: InitMsgBase{
							CommTypeX: commType,
							Timeout:   cos.Duration(DefaultTimeout),
						},
					},
					pod:  pod,
					uri:  transformerServer.URL,
					xctn: xctn,
				}
				comm = newCommunicator(nil, boot, nil).(HTTPCommunicator)

				switch testType {
				case "inline":
					q := url.Values{}
					q.Add(apc.QparamETLTransformArgs, etlTransformArgs)
					expectedEtlTransformArgs = etlTransformArgs

					q.Add(apc.QparamLatestVer, paramLatestVer)
					resp, err := http.Get(cos.JoinQuery(proxyServer.URL, q))
					Expect(err).NotTo(HaveOccurred())
					defer resp.Body.Close()

					b, err := cos.ReadAll(resp.Body)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(b)).To(Equal(len(transformData)))
					Expect(b).To(Equal(transformData))
				case "offline":
					lom := &core.LOM{ObjName: objName}
					err := lom.InitBck(clusterBck.Bucket())
					Expect(err).NotTo(HaveOccurred())

					expectedEtlTransformArgs = ""
					resp := comm.OfflineTransform(lom, false, false, "")
					Expect(resp.Err).NotTo(HaveOccurred())
					defer resp.R.Close()

					b, err := cos.ReadAll(resp.R)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(b)).To(Equal(len(transformData)))
				}
			})
		}
	}
})

// Creates a file with random content.
func createRandomFile(fileName string, size int64) error {
	b := make([]byte, size)
	if _, err := cryptorand.Read(b); err != nil {
		return err
	}
	f, err := cos.CreateFile(fileName)
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
