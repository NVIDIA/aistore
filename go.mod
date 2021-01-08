module github.com/NVIDIA/aistore

go 1.15

// NOTE: Remember to update `deploy/test/ci` image if the dependencies are updated.

require (
	cloud.google.com/go/storage v1.12.0
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/NVIDIA/go-tfdata v0.3.1
	github.com/OneOfOne/xxhash v1.2.8
	github.com/aws/aws-sdk-go v1.34.33
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgryski/go-metro v0.0.0-20200812162917-85c65e2d0165 // indirect
	github.com/fatih/color v1.10.0
	github.com/frankban/quicktest v1.5.0 // indirect
	github.com/go-logr/logr v0.2.1 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/googleapis/gnostic v0.5.1 // indirect
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f
	github.com/jacobsa/fuse v0.0.0-20200706075950-f8927095af03
	github.com/json-iterator/go v1.1.10
	github.com/karrick/godirwalk v1.16.1
	github.com/klauspost/compress v1.11.0 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/klauspost/reedsolomon v1.9.9
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lufia/iostat v1.1.0
	github.com/mmcloughlin/avo v0.0.0-20200803215136-443f81d77104 // indirect
	github.com/onsi/ginkgo v1.14.1
	github.com/onsi/gomega v1.10.2
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pierrec/lz4/v3 v3.3.2
	github.com/pkg/errors v0.9.1
	github.com/seiflotfy/cuckoofilter v0.0.0-20200511222245-56093a4d3841
	github.com/teris-io/shortid v0.0.0-20171029131806-771a37caa5cf
	github.com/tidwall/buntdb v1.1.2
	github.com/tidwall/gjson v1.6.1 // indirect
	github.com/tinylib/msgp v1.1.3
	github.com/urfave/cli v1.22.4
	github.com/valyala/fasthttp v1.16.0
	github.com/vbauerster/mpb/v4 v4.12.2
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a
	golang.org/x/mod v0.3.1-0.20200828183125-ce943fd02449 // indirect
	golang.org/x/net v0.0.0-20200927032502-5d4f70055728 // indirect
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200928205150-006507a75852
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e // indirect
	golang.org/x/tools v0.0.0-20200928201943-a0ef9b62deab // indirect
	google.golang.org/api v0.32.0
	google.golang.org/genproto v0.0.0-20200925023002-c2d885f95484 // indirect
	gopkg.in/yaml.v2 v2.3.0
	k8s.io/api v0.19.2
	k8s.io/apimachinery v0.19.2
	k8s.io/client-go v0.19.2
)
