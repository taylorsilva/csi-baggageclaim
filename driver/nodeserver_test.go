package driver_test

import (
	"code.cloudfoundry.org/lager"
	"github.com/concourse/baggageclaim"
	"github.com/concourse/baggageclaim/baggageclaimfakes"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/taylorsilva/csi-baggageclaim/driver"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
)

var (
	bagClient  *baggageclaimfakes.FakeClient
	mounter    *mount.FakeMounter
	logger     lager.Logger
	nodeServer csi.NodeServer
	ctx        context.Context

	maxNodes int64
)

type NodeServerSuite struct {
	suite.Suite
	*require.Assertions
}

func (s *NodeServerSuite) SetupTest() {
	bagClient = new(baggageclaimfakes.FakeClient)
	mounter = mount.NewFakeMounter([]mount.MountPoint{})
	ctx = context.TODO()
	logger = lager.NewLogger("node-test")
	maxNodes = 100

	nodeServer = driver.NewNodeServer("nodeId", maxNodes, bagClient, mounter, logger)
}

func (s *NodeServerSuite) TestNodeGetInfo() {
	req := &csi.NodeGetInfoRequest{}
	response, err := nodeServer.NodeGetInfo(ctx, req)

	s.Equal(err, nil)
	s.Equal("nodeId", response.NodeId)
	s.Equal(maxNodes, response.MaxVolumesPerNode)
	s.Empty(response.AccessibleTopology,
		"there should be no topology constraints for volumes")
}

func (s *NodeServerSuite) TestNodeGetCapabilities() {
	req := &csi.NodeGetCapabilitiesRequest{}
	response, err := nodeServer.NodeGetCapabilities(ctx, req)

	s.Equal(err, nil)
	s.Len(response.Capabilities, 1)
	expectedCapabilities := []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
	}
	s.Subset(expectedCapabilities, response.Capabilities,
		"should only contain the STAGE_UNSTAGE_VOLUME capability")
}

func (s *NodeServerSuite) TestNodeGetVolumeStats() {
	req := &csi.NodeGetVolumeStatsRequest{}
	response, err := nodeServer.NodeGetVolumeStats(ctx, req)

	s.Equal(status.Error(codes.Unimplemented, ""), err)
	s.Empty(response)
}

func (s *NodeServerSuite) TestNodeExpandVolume() {
	req := &csi.NodeExpandVolumeRequest{}
	response, err := nodeServer.NodeExpandVolume(ctx, req)

	s.Equal(status.Error(codes.Unimplemented, ""), err)
	s.Empty(response)
}

func (s *NodeServerSuite) TestCreateEmptyVolume() {
	bagClient.CreateVolumeReturns(&baggageclaimfakes.FakeVolume{}, nil)

	req := &csi.NodeStageVolumeRequest{
		VolumeId:         "some-id",
		VolumeCapability: &csi.VolumeCapability{},
		VolumeContext:    map[string]string{},
	}
	response, err := nodeServer.NodeStageVolume(ctx, req)

	s.Empty(err)
	s.Equal(&csi.NodeStageVolumeResponse{}, response)
	s.Equal(1, bagClient.CreateVolumeCallCount())

	_, actualVolId, actualVolSpec := bagClient.CreateVolumeArgsForCall(0)
	s.Equal("some-id", actualVolId)
	s.Equal(baggageclaim.VolumeSpec{Strategy: baggageclaim.EmptyStrategy{}}, actualVolSpec)
}

func (s *NodeServerSuite) TestCreateClonedVolumeWithParentVolumeOnNode() {
	parentVolume := &baggageclaimfakes.FakeVolume{}
	bagClient.LookupVolumeReturns(parentVolume, true, nil)
	bagClient.CreateVolumeReturns(&baggageclaimfakes.FakeVolume{}, nil)

	req := &csi.NodeStageVolumeRequest{
		VolumeId:         "some-id",
		VolumeCapability: &csi.VolumeCapability{},
		VolumeContext:    map[string]string{"parentVolumeId": "parent-vol-id"},
	}
	response, err := nodeServer.NodeStageVolume(ctx, req)

	s.Empty(err)
	s.Equal(&csi.NodeStageVolumeResponse{}, response)

	s.Equal(1, bagClient.LookupVolumeCallCount())
	_, actualParentId := bagClient.LookupVolumeArgsForCall(0)
	s.Equal("parent-vol-id", actualParentId)

	s.Equal(1, bagClient.CreateVolumeCallCount())
	_, actualVolId, actualVolSpec := bagClient.CreateVolumeArgsForCall(0)
	s.Equal("some-id", actualVolId)
	s.Equal(baggageclaim.VolumeSpec{
		Strategy: baggageclaim.COWStrategy{
			Parent: parentVolume,
		},
	},
		actualVolSpec)
}

func (s *NodeServerSuite) TestCreateClonedVolumeWithParentVolumeOnPeerNode() {
}

func (s *NodeServerSuite) TestNodeStageVolumeErrors() {
	tests := []struct {
		request       *csi.NodeStageVolumeRequest
		expectedError error
	}{
		{
			request: &csi.NodeStageVolumeRequest{
				VolumeId:         "",
				VolumeCapability: &csi.VolumeCapability{},
			},
			expectedError: status.Error(codes.InvalidArgument, "Volume ID missing in request"),
		},
		{
			request: &csi.NodeStageVolumeRequest{
				VolumeId: "some-id",
			},
			expectedError: status.Error(codes.InvalidArgument, "Volume Capability missing in request"),
		},
	}

	for _, test := range tests {
		response, err := nodeServer.NodeStageVolume(ctx, test.request)

		s.Empty(response)
		s.Error(err)
		s.Equal(test.expectedError, err)
	}
}

func (s *NodeServerSuite) TestNodeUnstageVolume() {
	bagClient.DestroyVolumeReturns(nil)

	req := &csi.NodeUnstageVolumeRequest{
		VolumeId: "some-id",
	}
	response, err := nodeServer.NodeUnstageVolume(ctx, req)

	s.Empty(err)
	s.Equal(&csi.NodeUnstageVolumeResponse{}, response)
	s.Equal(1, bagClient.DestroyVolumeCallCount())

	_, actualVolId := bagClient.DestroyVolumeArgsForCall(0)
	s.Equal("some-id", actualVolId)
}
