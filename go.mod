module github.com/taylorsilva/csi-baggageclaim

go 1.15

require (
	code.cloudfoundry.org/lager v2.0.0+incompatible
	github.com/concourse/baggageclaim v1.9.0
	github.com/concourse/flag v1.1.0
	github.com/container-storage-interface/spec v1.3.0
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.4.3
	github.com/kubernetes-csi/csi-lib-utils v0.9.0
	github.com/pborman/uuid v1.2.1
	github.com/stretchr/testify v1.6.1
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20201209123823-ac852fbbde11
	google.golang.org/grpc v1.34.0
	k8s.io/mount-utils v0.20.0
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920
)
