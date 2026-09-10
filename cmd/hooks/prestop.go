package hooks

import (
	lctx "context"
	lerrors "errors"
	lfmt "fmt"
	los "os"
	lsync "sync"

	lk8score "k8s.io/api/core/v1"
	lstoragev1 "k8s.io/api/storage/v1"
	lk8serrors "k8s.io/apimachinery/pkg/api/errors"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	linformers "k8s.io/client-go/informers"
	lk8s "k8s.io/client-go/kubernetes"
	lcache "k8s.io/client-go/tools/cache"
	llog "k8s.io/klog/v2"
)

/*
When a node is terminated, workloads using block storage volumes can take 6+ minutes to start
up again. This happens when a volume is not cleanly unmounted, which causes the Attach/Detach
controller (in kube-controller-manager) to wait for 6 minutes before issuing a force detach and
allowing the volume to be attached to another node.

This PreStop lifecycle hook aims to ensure that before the node (and the CSI driver node pod
running on it) is shut down, all VolumeAttachment objects associated with that node are removed,
thereby indicating that all volumes have been successfully unmounted and detached.

No unnecessary delay is added to the termination workflow, as the PreStop hook logic is only
executed when the node is being drained (thus preventing delays in termination where the node pod
is killed due to a rolling restart, or during driver upgrades, but the workload pods are expected
to keep running). If the PreStop hook hangs during its execution, the driver node pod will be
forcefully terminated after terminationGracePeriodSeconds, defined in the pod spec.
*/

const clusterAutoscalerTaint = "ToBeDeletedByClusterAutoscaler"
const v1KarpenterTaint = "karpenter.sh/disrupted"
const v1beta1KarpenterTaint = "karpenter.sh/disruption"

// drainTaints includes taints used by Kubernetes or autoscalers that signify node
// draining or pod eviction.
var drainTaints = map[string]struct{}{
	lk8score.TaintNodeUnschedulable: {}, // Kubernetes common eviction taint (kubectl drain)
	clusterAutoscalerTaint:          {},
	v1KarpenterTaint:                {},
	v1beta1KarpenterTaint:           {},
}

func PreStop(pclientset lk8s.Interface) error {
	llog.InfoS("PreStop: executing PreStop lifecycle hook")

	nodeName := los.Getenv("CSI_NODE_NAME")
	if nodeName == "" {
		return lerrors.New("PreStop: CSI_NODE_NAME missing")
	}

	node, err := fetchNode(pclientset, nodeName)
	switch {
	case lk8serrors.IsNotFound(err):
		llog.InfoS("PreStop: node does not exist - assuming this is a termination event, checking for remaining VolumeAttachments", "node", nodeName)
	case err != nil:
		return err
	case !isNodeBeingDrained(node):
		llog.InfoS("PreStop: node is not being drained, skipping VolumeAttachments check", "node", nodeName)
		return nil
	default:
		llog.InfoS("PreStop: node is being drained, checking for remaining VolumeAttachments", "node", nodeName)
	}

	return waitForVolumeAttachments(pclientset, nodeName)
}

func fetchNode(pclientset lk8s.Interface, pnodeName string) (*lk8score.Node, error) {
	node, err := pclientset.CoreV1().Nodes().Get(lctx.Background(), pnodeName, lmetav1.GetOptions{})
	if err != nil {
		return nil, lfmt.Errorf("fetchNode: failed to retrieve node information: %w", err)
	}
	return node, nil
}

// isNodeBeingDrained returns true if node resource has a known drain/eviction taint.
func isNodeBeingDrained(pnode *lk8score.Node) bool {
	for _, taint := range pnode.Spec.Taints {
		if _, isDrainTaint := drainTaints[taint.Key]; isDrainTaint {
			return true
		}
	}
	return false
}

// completionSignal releases the hook once no VolumeAttachment is left for this node.
// checkVolumeAttachments runs from the initial List and again from every informer
// Delete/Update callback, so more than one of them can observe "nothing left".
// Closing a channel twice panics, and a panic in a preStop hook kills the container
// early - losing exactly the protection the hook exists to provide - so releasing is
// made idempotent instead of closing the channel directly.
type completionSignal struct {
	ch   chan struct{}
	once lsync.Once
}

func newCompletionSignal() *completionSignal {
	return &completionSignal{ch: make(chan struct{})}
}

// release unblocks the hook. Safe to call any number of times, from any goroutine.
func (s *completionSignal) release() {
	s.once.Do(func() {
		close(s.ch)
	})
}

// done doubles as the informer's stop channel: releasing the hook stops the informer.
func (s *completionSignal) done() <-chan struct{} {
	return s.ch
}

func waitForVolumeAttachments(pclientset lk8s.Interface, pnodeName string) error {
	allAttachmentsDeleted := newCompletionSignal()

	factory := linformers.NewSharedInformerFactory(pclientset, 0)
	informer := factory.Storage().V1().VolumeAttachments().Informer()

	_, err := informer.AddEventHandler(lcache.ResourceEventHandlerFuncs{
		DeleteFunc: func(pobj any) {
			rescanOnAttachmentEvent(pclientset, pnodeName, allAttachmentsDeleted, pobj, "DeleteFunc")
		},
		UpdateFunc: func(poldObj, pnewObj any) {
			rescanOnAttachmentEvent(pclientset, pnodeName, allAttachmentsDeleted, pnewObj, "UpdateFunc")
		},
	})
	if err != nil {
		return lfmt.Errorf("failed to add event handler to VolumeAttachment informer: %w", err)
	}

	go informer.Run(allAttachmentsDeleted.done())

	if err := checkVolumeAttachments(pclientset, pnodeName, allAttachmentsDeleted); err != nil {
		llog.ErrorS(err, "waitForVolumeAttachments: error checking VolumeAttachments")
	}

	// There is deliberately no timeout here: the bound on this wait is the pod's
	// terminationGracePeriodSeconds, after which kubelet force-kills the container.
	<-allAttachmentsDeleted.done()
	llog.InfoS("waitForVolumeAttachments: finished waiting for VolumeAttachments to be deleted. preStopHook completed")
	return nil
}

// rescanOnAttachmentEvent re-lists the VolumeAttachments whenever an informer event
// may concern this node. An object that does not type-assert - a
// cache.DeletedFinalStateUnknown tombstone, which the informer delivers whenever the
// watch has to be re-established - is rescanned too: upstream dereferences the failed
// assertion and panics, while skipping the rescan could park the hook until
// terminationGracePeriodSeconds expires. The List is what decides, and it is cheap.
func rescanOnAttachmentEvent(pclientset lk8s.Interface, pnodeName string, psignal *completionSignal, pobj any, psource string) {
	llog.V(5).InfoS(psource+": VolumeAttachment event received", "node", pnodeName)

	va, ok := pobj.(*lstoragev1.VolumeAttachment)
	switch {
	case !ok:
		llog.InfoS(psource+": object is not a VolumeAttachment, checking VolumeAttachments anyway", "obj", pobj, "node", pnodeName)
	case va.Spec.NodeName != pnodeName:
		return
	}

	if err := checkVolumeAttachments(pclientset, pnodeName, psignal); err != nil {
		llog.ErrorS(err, psource+": error checking VolumeAttachments")
	}
}

func checkVolumeAttachments(pclientset lk8s.Interface, pnodeName string, pallAttachmentsDeleted *completionSignal) error {
	allAttachments, err := pclientset.StorageV1().VolumeAttachments().List(lctx.Background(), lmetav1.ListOptions{})
	if err != nil {
		return lfmt.Errorf("checkVolumeAttachments: failed to list VolumeAttachments: %w", err)
	}

	for _, attachment := range allAttachments.Items {
		if attachment.Spec.NodeName == pnodeName {
			llog.InfoS("checkVolumeAttachments: not ready to exit, found VolumeAttachment", "attachment", attachment.Name, "node", pnodeName)
			return nil
		}
	}

	pallAttachmentsDeleted.release()
	return nil
}
