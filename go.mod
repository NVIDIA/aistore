module github.com/NVIDIA/aistore

go 1.19

// Direct dependencies
require (
	cloud.google.com/go/storage v1.27.0
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/NVIDIA/go-tfdata v0.3.1
	github.com/OneOfOne/xxhash v1.2.8
	github.com/aws/aws-sdk-go v1.44.108
	github.com/colinmarc/hdfs/v2 v2.3.0
	github.com/golang-jwt/jwt/v4 v4.4.2
	github.com/json-iterator/go v1.1.12
	github.com/karrick/godirwalk v1.17.0
	github.com/klauspost/reedsolomon v1.11.0
	github.com/lufia/iostat v1.2.1
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.20.2
	github.com/pierrec/lz4/v3 v3.3.5
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.13.0
	github.com/seiflotfy/cuckoofilter v0.0.0-20220411075957-e3b120b3f5fb
	github.com/teris-io/shortid v0.0.0-20220617161101-71ec9f2aa569
	github.com/tidwall/buntdb v1.2.10
	github.com/tinylib/msgp v1.1.6
	github.com/valyala/fasthttp v1.40.0
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	github.com/vmihailenco/msgpack/v5 v5.3.5
	golang.org/x/crypto v0.0.0-20220926161630-eccd6366d1be
	golang.org/x/sync v0.0.0-20220929204114-8fcdb60fdcc0
	golang.org/x/sys v0.0.0-20220928140112-f11e5e49a4ec
	google.golang.org/api v0.98.0
	k8s.io/api v0.24.3
	k8s.io/apimachinery v0.24.3
	k8s.io/client-go v0.24.3
	k8s.io/metrics v0.24.3
)

// Indirect dependencies
require (
	cloud.google.com/go v0.104.0 // indirect
	cloud.google.com/go/compute v1.10.0 // indirect
	cloud.google.com/go/iam v0.5.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20211217172704-adc40b04c140 // indirect
	github.com/emicklei/go-restful/v3 v3.9.0 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.5.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/klauspost/cpuid/v2 v2.1.1 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-ieproxy v0.0.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2 // indirect
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tidwall/btree v1.4.4 // indirect
	github.com/tidwall/gjson v1.14.3 // indirect
	github.com/tidwall/grect v0.1.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/rtred v0.1.2 // indirect
	github.com/tidwall/tinyqueue v0.1.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/net v0.0.0-20220927171203-f486391704dc // indirect
	golang.org/x/oauth2 v0.0.0-20220909003341-f21342109be1 // indirect
	golang.org/x/term v0.0.0-20220919170432-7a66f970e087 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20220922220347-f3bd1da661af // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220930163606-c98284e70a91 // indirect
	google.golang.org/grpc v1.49.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
	k8s.io/kube-openapi v0.0.0-20220928191237-829ce0c27909 // indirect
	k8s.io/utils v0.0.0-20220922133306-665eaaec4324 // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
