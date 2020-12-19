package driver

import (
	"fmt"
	"os"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/baggageclaim"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"k8s.io/mount-utils"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TopologyKeyNode = "topology.baggageclaim.csi/node"

type nodeServer struct {
	nodeID            string
	maxVolumesPerNode int64
	logger            lager.Logger
	bagClient         baggageclaim.Client
	mounter           mount.Interface
}

func NewNodeServer(nodeId string, maxVolumesPerNode int64, client baggageclaim.Client, mounter mount.Interface, logger lager.Logger) *nodeServer {
	return &nodeServer{
		nodeID:            nodeId,
		maxVolumesPerNode: maxVolumesPerNode,
		logger:            logger,
		bagClient:         client,
		mounter:           mounter,
	}
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	//logger := ns.logger.Session("NodePublishVolume", lager.Data{"VolumeId": req.VolumeId})

	// Check arguments
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	volumeContext := req.GetVolumeContext()
	var strategy baggageclaim.Strategy

	glog.V(4).Infof("concourse: volumetContext: %s", volumeContext)
	if id, ok := volumeContext["sourceVolumeID"]; ok {
		glog.V(4).Info("concourse: using COWStrategy, source volume id found: " + id)
		srcVolume, _, err := ns.bagClient.LookupVolume(ns.logger, id)
		if err != nil {
			return nil, err
		}
		strategy = baggageclaim.COWStrategy{Parent: srcVolume}
	} else {
		glog.V(4).Info("concourse: using EmptyStrategy")
		strategy = baggageclaim.EmptyStrategy{}
	}

	volume, err := ns.bagClient.CreateVolume(ns.logger, req.VolumeId, baggageclaim.VolumeSpec{
		Strategy:   strategy,
		Properties: map[string]string{},
	})
	if err != nil {
		return nil, err
	}

	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	readOnly := req.GetReadonly()

	options := []string{"bind"}
	if readOnly {
		options = append(options, "ro")
	}

	mounter := mount.New("")
	path := volume.Path()
	glog.V(4).Infof("concourse: mounting baggageclaim volume at %s", path)
	if err := mounter.Mount(path, targetPath, "", options); err != nil {
		return nil, err
	}

	// targetPath := req.GetTargetPath()
	// ephemeralVolume := req.GetVolumeContext()["csi.storage.k8s.io/ephemeral"] == "true" ||
	// 	req.GetVolumeContext()["csi.storage.k8s.io/ephemeral"] == "" && ns.ephemeral // Kubernetes 1.15 doesn't have csi.storage.k8s.io/ephemeral.

	// if req.GetVolumeCapability().GetBlock() != nil &&
	// 	req.GetVolumeCapability().GetMount() != nil {
	// 	return nil, status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
	// }

	// // if ephemeral is specified, create volume here to avoid errors
	// if ephemeralVolume {
	// 	volID := req.GetVolumeId()
	// 	volName := fmt.Sprintf("ephemeral-%s", volID)
	// 	vol, err := createHostpathVolume(req.GetVolumeId(), volName, maxStorageCapacity, mountAccess, ephemeralVolume)
	// 	if err != nil && !os.IsExist(err) {
	// 		glog.Error("ephemeral mode failed to create volume: ", err)
	// 		return nil, status.Error(codes.Internal, err.Error())
	// 	}
	// 	glog.V(4).Infof("ephemeral mode: created volume: %s", vol.VolPath)
	// }

	// vol, err := getVolumeByID(req.GetVolumeId())
	// if err != nil {
	// 	return nil, status.Error(codes.NotFound, err.Error())
	// }

	// if req.GetVolumeCapability().GetBlock() != nil {
	// 	if vol.VolAccessType != blockAccess {
	// 		return nil, status.Error(codes.InvalidArgument, "cannot publish a non-block volume as block volume")
	// 	}

	// 	volPathHandler := volumepathhandler.VolumePathHandler{}

	// 	// Get loop device from the volume path.
	// 	loopDevice, err := volPathHandler.GetLoopDevice(vol.VolPath)
	// 	if err != nil {
	// 		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get the loop device: %v", err))
	// 	}

	// 	mounter := mount.New("")

	// 	// Check if the target path exists. Create if not present.
	// 	_, err = os.Lstat(targetPath)
	// 	if os.IsNotExist(err) {
	// 		if err = mounter.MakeFile(targetPath); err != nil {
	// 			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create target path: %s: %v", targetPath, err))
	// 		}
	// 	}
	// 	if err != nil {
	// 		return nil, status.Errorf(codes.Internal, "failed to check if the target block file exists: %v", err)
	// 	}

	// 	// Check if the target path is already mounted. Prevent remounting.
	// 	notMount, err := mounter.IsNotMountPoint(targetPath)
	// 	if err != nil {
	// 		if !os.IsNotExist(err) {
	// 			return nil, status.Errorf(codes.Internal, "error checking path %s for mount: %s", targetPath, err)
	// 		}
	// 		notMount = true
	// 	}
	// 	if !notMount {
	// 		// It's already mounted.
	// 		glog.V(5).Infof("Skipping bind-mounting subpath %s: already mounted", targetPath)
	// 		return &csi.NodePublishVolumeResponse{}, nil
	// 	}

	// 	options := []string{"bind"}
	// 	if err := mount.New("").Mount(loopDevice, targetPath, "", options); err != nil {
	// 		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount block device: %s at %s: %v", loopDevice, targetPath, err))
	// 	}
	// } else if req.GetVolumeCapability().GetMount() != nil {
	// 	if vol.VolAccessType != mountAccess {
	// 		return nil, status.Error(codes.InvalidArgument, "cannot publish a non-mount volume as mount volume")
	// 	}

	// 	notMnt, err := mount.New("").IsNotMountPoint(targetPath)
	// 	if err != nil {
	// 		if os.IsNotExist(err) {
	// 			if err = os.MkdirAll(targetPath, 0750); err != nil {
	// 				return nil, status.Error(codes.Internal, err.Error())
	// 			}
	// 			notMnt = true
	// 		} else {
	// 			return nil, status.Error(codes.Internal, err.Error())
	// 		}
	// 	}

	// 	if !notMnt {
	// 		return &csi.NodePublishVolumeResponse{}, nil
	// 	}

	// 	fsType := req.GetVolumeCapability().GetMount().GetFsType()

	// 	deviceId := ""
	// 	if req.GetPublishContext() != nil {
	// 		deviceId = req.GetPublishContext()[deviceID]
	// 	}

	// 	readOnly := req.GetReadonly()
	// 	volumeId := req.GetVolumeId()
	// 	attrib := req.GetVolumeContext()
	// 	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	// 	glog.V(4).Infof("target %v\nfstype %v\ndevice %v\nreadonly %v\nvolumeId %v\nattributes %v\nmountflags %v\n",
	// 		targetPath, fsType, deviceId, readOnly, volumeId, attrib, mountFlags)

	// 	options := []string{"bind"}
	// 	if readOnly {
	// 		options = append(options, "ro")
	// 	}
	// 	mounter := mount.New("")
	// 	path := getVolumePath(volumeId)

	// 	if err := mounter.Mount(path, targetPath, "", options); err != nil {
	// 		var errList strings.Builder
	// 		errList.WriteString(err.Error())
	// 		if vol.Ephemeral {
	// 			if rmErr := os.RemoveAll(path); rmErr != nil && !os.IsNotExist(rmErr) {
	// 				errList.WriteString(fmt.Sprintf(" :%s", rmErr.Error()))
	// 			}
	// 		}
	// 		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount device: %s at %s: %s", path, targetPath, errList.String()))
	// 	}
	// }

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	glog.V(4).Infof("concourse: Calling NodeUnpublishVolume: %v", req)
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	vol, _, err := ns.bagClient.LookupVolume(ns.logger, volumeID)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if vol != nil {
		err := ns.bagClient.DestroyVolume(ns.logger, volumeID)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("concourse: failed to delete volume: %s", err))
		}
	}

	// Unmount only if the target path is really a mount point.
	if notMnt, err := mount.IsNotMountPoint(mount.New(""), targetPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if !notMnt {
		// Unmounting the image or filesystem.
		err = mount.New("").Unmount(targetPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	// Delete the mount point.
	// Does not return error for non-existent path, repeated calls OK for idempotency.
	if err = os.RemoveAll(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.V(4).Infof("concourse: volume %s has been unpublished.", targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}

	var strategy baggageclaim.Strategy
	if parentId, ok := req.VolumeContext["parentVolumeId"]; ok {
		parentVol, found, err := ns.bagClient.LookupVolume(ns.logger, parentId)
		if !found || err != nil {
			return nil, status.Error(codes.Internal,
				fmt.Sprintf("Could not find parent volume to clone: %v", err))
		}
		strategy = baggageclaim.COWStrategy{
			Parent: parentVol,
		}
	} else {
		strategy = baggageclaim.EmptyStrategy{}
	}

	_, err := ns.bagClient.CreateVolume(ns.logger, req.VolumeId, baggageclaim.VolumeSpec{Strategy: strategy})
	if err != nil {
		return nil, status.Error(codes.Internal,
			fmt.Sprintf("Failed to create volume in baggageclaim: %v", err))
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	ns.bagClient.DestroyVolume(ns.logger, req.VolumeId)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	return &csi.NodeGetInfoResponse{
		NodeId:            ns.nodeID,
		MaxVolumesPerNode: ns.maxVolumesPerNode,
	}, nil
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (ns *nodeServer) NodeGetVolumeStats(ctx context.Context, in *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
