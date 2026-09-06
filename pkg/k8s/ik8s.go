package k8s

import (
	lctx "context"

	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

type IKubernetes interface {
	GetPersistentVolumeClaimByName(pctx lctx.Context, pnamespace, pname string) (*lsentity.PersistentVolumeClaim, lserr.IError)
	GetStorageClassByName(pctx lctx.Context, pname string) (*lsentity.StorageClass, lserr.IError)
	GetPersistentVolume(pctx lctx.Context, pname string) (*lsentity.PersistentVolume, lserr.IError)

	// Event recorder
	PersistentVolumeClaimEventWarning(pctx lctx.Context, pnamespace, pname, preason, pmessage string)
	PersistentVolumeClaimEventNormal(pctx lctx.Context, pnamespace, pname, preason, pmessage string)
	PersistentVolumeEventWarning(pctx lctx.Context, pname, preason, pmessage string)
	PersistentVolumeEventNormal(pctx lctx.Context, pname, preason, pmessage string)

	// Volume-centric events: emit on the PV (always present while the volume
	// exists) and additionally on the PVC when it is still around. A stuck
	// detach usually outlives its PVC, so the PV is the anchor.
	FindPersistentVolumeByHandle(pctx lctx.Context, phandle string) (*lsentity.PersistentVolume, lserr.IError)
	VolumeEventWarning(pctx lctx.Context, ppvName, preason, pmessage string)
	VolumeEventNormal(pctx lctx.Context, ppvName, preason, pmessage string)
}
