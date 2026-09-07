package k8s

import (
	lctx "context"

	lcoreV1 "k8s.io/api/core/v1"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lk8s "k8s.io/client-go/kubernetes"
	lk8srecord "k8s.io/client-go/tools/record"
	llog "k8s.io/klog/v2"

	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

type kubernetes struct {
	lk8s.Interface
	lk8srecord.EventRecorder
}

func NewKubernetes(pk8sclient lk8s.Interface, precorder lk8srecord.EventRecorder) IKubernetes {
	return &kubernetes{
		Interface:     pk8sclient,
		EventRecorder: precorder,
	}
}

func (s *kubernetes) GetPersistentVolumeClaimByName(pctx lctx.Context, pnamespace, pname string) (*lsentity.PersistentVolumeClaim, lserr.IError) {
	pvc, err := s.CoreV1().PersistentVolumeClaims(pnamespace).Get(pctx, pname, lmetav1.GetOptions{})
	if err != nil {
		return nil, lserr.ErrK8sPvcFailedToGet(pnamespace, pname, err)
	}

	if pvc == nil {
		return nil, lserr.ErrK8sPvcNotFound(pnamespace, pname)
	}

	return lsentity.NewPersistentVolumeClaim(pvc), nil
}

func (s *kubernetes) GetStorageClassByName(pctx lctx.Context, pname string) (*lsentity.StorageClass, lserr.IError) {
	sc, err := s.StorageV1().StorageClasses().Get(pctx, pname, lmetav1.GetOptions{})
	if err != nil {
		return nil, lserr.ErrK8sStorageClassFailedToGet(pname, err)
	}

	if sc == nil {
		return nil, lserr.ErrK8sStorageClassNotFound(pname)
	}

	return lsentity.NewStorageClass(sc), nil
}

func (s *kubernetes) GetPersistentVolume(pctx lctx.Context, pname string) (*lsentity.PersistentVolume, lserr.IError) {
	pv, err := s.CoreV1().PersistentVolumes().Get(pctx, pname, lmetav1.GetOptions{})
	if err != nil {
		return nil, lserr.ErrK8sPvFailedToGet(pname, err)
	}

	if pv == nil {
		return nil, lserr.ErrK8sPvNotFound(pname)
	}

	return lsentity.NewPersistentVolume(pv), nil

}

func (s *kubernetes) PersistentVolumeClaimEventWarning(pctx lctx.Context, pnamespace, pname, preason, pmessage string) {
	if pnamespace == "" || pname == "" {
		return
	}

	pvc, err := s.GetPersistentVolumeClaimByName(pctx, pnamespace, pname)
	if err != nil || pvc == nil {
		return
	}
	s.EventRecorder.Event(pvc.PersistentVolumeClaim, lcoreV1.EventTypeWarning, preason, pmessage)
}

func (s *kubernetes) PersistentVolumeClaimEventNormal(pctx lctx.Context, pnamespace, pname, preason, pmessage string) {
	if pnamespace == "" || pname == "" {
		return
	}

	pvc, err := s.GetPersistentVolumeClaimByName(pctx, pnamespace, pname)
	if err != nil || pvc == nil {
		return
	}
	s.EventRecorder.Event(pvc.PersistentVolumeClaim, lcoreV1.EventTypeNormal, preason, pmessage)
}

func (s *kubernetes) PersistentVolumeEventWarning(pctx lctx.Context, pname, preason, pmessage string) {
	if pname == "" {
		return
	}

	pvc, err := s.GetPersistentVolume(pctx, pname)
	if err != nil || pvc == nil {
		return
	}
	s.EventRecorder.Event(pvc.PersistentVolume, lcoreV1.EventTypeWarning, preason, pmessage)
}

func (s *kubernetes) PersistentVolumeEventNormal(pctx lctx.Context, pname, preason, pmessage string) {
	if pname == "" {
		return
	}

	pvc, err := s.GetPersistentVolume(pctx, pname)
	if err != nil || pvc == nil {
		return
	}
	s.EventRecorder.Event(pvc.PersistentVolume, lcoreV1.EventTypeNormal, preason, pmessage)
}

// FindPersistentVolumeByHandle locates the PV backing an IaaS volume ID.
//
// This is a full LIST of PersistentVolumes. That is affordable only because
// callers use it at a handful of moments per stuck volume (breaker trip, each
// backoff step, recovery) - never once per retry. Keep it that way.
func (s *kubernetes) FindPersistentVolumeByHandle(pctx lctx.Context, phandle string) (*lsentity.PersistentVolume, lserr.IError) {
	if phandle == "" {
		return nil, lserr.ErrK8sPvNotFound(phandle)
	}

	pvs, err := s.CoreV1().PersistentVolumes().List(pctx, lmetav1.ListOptions{})
	if err != nil {
		return nil, lserr.ErrK8sPvFailedToGet(phandle, err)
	}

	for i := range pvs.Items {
		csi := pvs.Items[i].Spec.CSI
		if csi != nil && csi.VolumeHandle == phandle {
			return lsentity.NewPersistentVolume(&pvs.Items[i]), nil
		}
	}

	return nil, lserr.ErrK8sPvNotFound(phandle)
}

// VolumeEventWarning emits a Warning on the PV, and on its PVC when that still
// exists. Failures are logged and swallowed: telling someone about a problem
// must never become a second problem.
func (s *kubernetes) VolumeEventWarning(pctx lctx.Context, ppvName, preason, pmessage string) {
	s.volumeEvent(pctx, ppvName, lcoreV1.EventTypeWarning, preason, pmessage)
}

// VolumeEventNormal emits a Normal event the same way - used to report that a
// pair which had been stuck finally detached.
func (s *kubernetes) VolumeEventNormal(pctx lctx.Context, ppvName, preason, pmessage string) {
	s.volumeEvent(pctx, ppvName, lcoreV1.EventTypeNormal, preason, pmessage)
}

func (s *kubernetes) volumeEvent(pctx lctx.Context, ppvName, peventType, preason, pmessage string) {
	if ppvName == "" {
		return
	}

	pv, ierr := s.GetPersistentVolume(pctx, ppvName)
	if ierr != nil || pv == nil || pv.PersistentVolume == nil {
		llog.V(2).InfoS("[DEBUG] - volumeEvent: PV not available, skipping event",
			"pv", ppvName, "reason", preason)

		return
	}
	s.EventRecorder.Event(pv.PersistentVolume, peventType, preason, pmessage)

	claim := pv.PersistentVolume.Spec.ClaimRef
	if claim == nil || claim.Name == "" {
		return
	}

	pvc, ierr := s.GetPersistentVolumeClaimByName(pctx, claim.Namespace, claim.Name)
	if ierr != nil || pvc == nil || pvc.PersistentVolumeClaim == nil {
		llog.V(2).InfoS("[DEBUG] - volumeEvent: PVC gone, event emitted on PV only",
			"pv", ppvName, "namespace", claim.Namespace, "name", claim.Name)

		return
	}
	s.EventRecorder.Event(pvc.PersistentVolumeClaim, peventType, preason, pmessage)
}
