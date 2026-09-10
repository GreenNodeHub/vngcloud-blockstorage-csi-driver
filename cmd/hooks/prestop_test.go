package hooks

import (
	lctx "context"
	lfmt "fmt"
	ltesting "testing"
	ltime "time"

	lk8score "k8s.io/api/core/v1"
	lstoragev1 "k8s.io/api/storage/v1"
	lk8serrors "k8s.io/apimachinery/pkg/api/errors"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lruntime "k8s.io/apimachinery/pkg/runtime"
	lfake "k8s.io/client-go/kubernetes/fake"
	ltesting2 "k8s.io/client-go/testing"
	lcache "k8s.io/client-go/tools/cache"
)

func nodeWithTaints(pname string, ptaintKeys ...string) *lk8score.Node {
	var taints []lk8score.Taint
	for _, key := range ptaintKeys {
		taints = append(taints, lk8score.Taint{
			Key:    key,
			Effect: lk8score.TaintEffectNoSchedule,
		})
	}
	return &lk8score.Node{
		ObjectMeta: lmetav1.ObjectMeta{Name: pname},
		Spec:       lk8score.NodeSpec{Taints: taints},
	}
}

// listedVolumeAttachments reports whether the hook got as far as looking at the
// VolumeAttachments of the node. The taint gate is the whole reason the hook is
// safe to run on every termination, so "did it even ask" is the assertion that
// matters, not how many times it asked.
func listedVolumeAttachments(pclient *lfake.Clientset) bool {
	for _, action := range pclient.Actions() {
		if action.GetVerb() == "list" && action.GetResource().Resource == "volumeattachments" {
			return true
		}
	}
	return false
}

func TestPreStopMissingNodeNameIsAnError(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "")
	client := lfake.NewSimpleClientset()

	err := PreStop(client)

	if err == nil {
		t.Fatal("want an error when CSI_NODE_NAME is missing: a silent success looks like a hook that ran")
	}
	if listedVolumeAttachments(client) {
		t.Fatal("hook listed VolumeAttachments without knowing which node it runs on")
	}
}

func TestPreStopNodeNotBeingDrainedReturnsWithoutLooking(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	// A rolling restart or a driver upgrade kills the node pod while the workload
	// pods keep running; the hook must not delay that.
	client := lfake.NewSimpleClientset(nodeWithTaints("test-node"))

	if err := PreStop(client); err != nil {
		t.Fatalf("PreStop() error = %v", err)
	}

	if listedVolumeAttachments(client) {
		t.Fatal("hook checked VolumeAttachments for a node that is not being drained")
	}
}

func TestPreStopNodeWithUnrelatedTaintReturnsWithoutLooking(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	client := lfake.NewSimpleClientset(nodeWithTaints("test-node", "bs.csi.vngcloud.vn/fake-taint"))

	if err := PreStop(client); err != nil {
		t.Fatalf("PreStop() error = %v", err)
	}

	if listedVolumeAttachments(client) {
		t.Fatal("hook checked VolumeAttachments for a node carrying only an unrelated taint")
	}
}

func TestPreStopDrainTaintsProceedToAttachmentCheck(t *ltesting.T) {
	for _, taintKey := range []string{
		lk8score.TaintNodeUnschedulable,
		clusterAutoscalerTaint,
		v1KarpenterTaint,
		v1beta1KarpenterTaint,
	} {
		t.Run(taintKey, func(t *ltesting.T) {
			t.Setenv("CSI_NODE_NAME", "test-node")
			client := lfake.NewSimpleClientset(nodeWithTaints("test-node", taintKey))

			if err := PreStop(client); err != nil {
				t.Fatalf("PreStop() error = %v", err)
			}

			if !listedVolumeAttachments(client) {
				t.Fatalf("hook skipped the VolumeAttachments check for a node tainted %q", taintKey)
			}
		})
	}
}

func TestPreStopMissingNodeProceedsToAttachmentCheck(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	// The Node object can already be gone by the time the hook runs; that is a
	// termination event, not a reason to skip the check.
	client := lfake.NewSimpleClientset()

	if err := PreStop(client); err != nil {
		t.Fatalf("PreStop() error = %v", err)
	}

	if !listedVolumeAttachments(client) {
		t.Fatal("hook skipped the VolumeAttachments check for a node that no longer exists")
	}
}

func TestPreStopNodeGetFailureIsReported(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	client := lfake.NewSimpleClientset()
	client.PrependReactor("get", "nodes", func(ltesting2.Action) (bool, lruntime.Object, error) {
		// Anything that is not NotFound leaves us unable to tell a drain from a
		// rolling restart, so it must not be swallowed into a skip.
		return true, nil, lk8serrors.NewInternalError(lfmt.Errorf("apiserver is unhappy"))
	})

	err := PreStop(client)

	if err == nil {
		t.Fatal("want an error when the Node cannot be read")
	}
	if listedVolumeAttachments(client) {
		t.Fatal("hook checked VolumeAttachments although it never learned whether the node is draining")
	}
}

func TestIsNodeBeingDrained(t *ltesting.T) {
	testCases := []struct {
		name         string
		nodeTaintKey string
		want         bool
	}{
		{"common eviction taint (kubectl drain)", lk8score.TaintNodeUnschedulable, true},
		{"cluster autoscaler taint", clusterAutoscalerTaint, true},
		{"Karpenter v1 taint", v1KarpenterTaint, true},
		{"Karpenter v1beta1 taint", v1beta1KarpenterTaint, true},
		{"unrelated taint", "bs.csi.vngcloud.vn/fake-taint", false},
		{"no taint at all", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *ltesting.T) {
			node := nodeWithTaints("test-node")
			if tc.nodeTaintKey != "" {
				node = nodeWithTaints("test-node", tc.nodeTaintKey)
			}

			if got := isNodeBeingDrained(node); got != tc.want {
				t.Fatalf("isNodeBeingDrained() with taint %q = %t, want %t", tc.nodeTaintKey, got, tc.want)
			}
		})
	}
}

func volumeAttachment(pname, pnodeName string) *lstoragev1.VolumeAttachment {
	return &lstoragev1.VolumeAttachment{
		ObjectMeta: lmetav1.ObjectMeta{Name: pname},
		Spec: lstoragev1.VolumeAttachmentSpec{
			NodeName: pnodeName,
			Attacher: "bs.csi.vngcloud.vn",
		},
	}
}

func TestPreStopReturnsWhenNodeHasNoAttachments(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	client := lfake.NewSimpleClientset(
		nodeWithTaints("test-node", lk8score.TaintNodeUnschedulable),
	)

	done := make(chan error, 1)
	go func() { done <- PreStop(client) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PreStop() error = %v", err)
		}
	case <-ltime.After(10 * ltime.Second):
		t.Fatal("PreStop() hung although the node has no VolumeAttachments at all")
	}
}

func TestPreStopIsNotHeldByAnotherNodesAttachment(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	// Only Spec.NodeName decides whose volume this is; waiting on someone else's
	// attachment would burn the whole terminationGracePeriodSeconds for nothing.
	client := lfake.NewSimpleClientset(
		nodeWithTaints("test-node", lk8score.TaintNodeUnschedulable),
		volumeAttachment("va-other-node", "test-node-2"),
	)

	done := make(chan error, 1)
	go func() { done <- PreStop(client) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PreStop() error = %v", err)
		}
	case <-ltime.After(10 * ltime.Second):
		t.Fatal("PreStop() waited on a VolumeAttachment belonging to a different node")
	}
}

func TestPreStopBlocksUntilTheNodesAttachmentIsDeleted(t *ltesting.T) {
	t.Setenv("CSI_NODE_NAME", "test-node")
	client := lfake.NewSimpleClientset(
		nodeWithTaints("test-node", lk8score.TaintNodeUnschedulable),
		volumeAttachment("va-test-node", "test-node"),
	)

	done := make(chan error, 1)
	go func() { done <- PreStop(client) }()

	// Returning here would tell kubelet the volumes are detached while they are not,
	// which is the 6-minute force-detach wait the hook exists to avoid.
	select {
	case err := <-done:
		t.Fatalf("PreStop() returned (err = %v) while a VolumeAttachment for the node was still present", err)
	case <-ltime.After(300 * ltime.Millisecond):
	}

	if err := client.StorageV1().VolumeAttachments().Delete(lctx.Background(), "va-test-node", lmetav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the VolumeAttachment: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PreStop() error = %v", err)
		}
	case <-ltime.After(10 * ltime.Second):
		t.Fatal("PreStop() did not return after the last VolumeAttachment for the node was deleted")
	}
}

func TestCheckVolumeAttachmentsReleaseIsIdempotent(t *ltesting.T) {
	// checkVolumeAttachments runs from the initial List and again from every informer
	// Delete/Update callback, so more than one of them can find zero attachments left
	// for this node. Closing a channel twice panics, and a panic in a preStop hook
	// kills the container early - losing exactly the protection the hook provides.
	client := lfake.NewSimpleClientset(volumeAttachment("va-other-node", "test-node-2"))
	allAttachmentsDeleted := newCompletionSignal()

	for i := 0; i < 2; i++ {
		if err := checkVolumeAttachments(client, "test-node", allAttachmentsDeleted); err != nil {
			t.Fatalf("checkVolumeAttachments() call %d error = %v", i+1, err)
		}
	}

	select {
	case <-allAttachmentsDeleted.done():
	default:
		t.Fatal("hook was never released although no VolumeAttachment belongs to the node")
	}
}

func TestRescanOnAttachmentEventIgnoresAnotherNode(t *ltesting.T) {
	client := lfake.NewSimpleClientset()
	allAttachmentsDeleted := newCompletionSignal()

	rescanOnAttachmentEvent(client, "test-node", allAttachmentsDeleted, volumeAttachment("va-other-node", "test-node-2"), "TestFunc")

	if listedVolumeAttachments(client) {
		t.Fatal("an event about another node's VolumeAttachment cost a List")
	}
	select {
	case <-allAttachmentsDeleted.done():
		t.Fatal("hook was released by an event about another node's VolumeAttachment")
	default:
	}
}

func TestRescanOnAttachmentEventRechecksOnUnexpectedObject(t *ltesting.T) {
	// A DeletedFinalStateUnknown tombstone arrives instead of the object whenever the
	// watch is re-established. Upstream dereferences the failed type assertion and
	// panics; skipping the recheck instead would park the hook until the grace period
	// expires, so ask the API - the List is what decides.
	client := lfake.NewSimpleClientset()
	allAttachmentsDeleted := newCompletionSignal()

	tombstone := lcache.DeletedFinalStateUnknown{Key: "va-test-node"}

	rescanOnAttachmentEvent(client, "test-node", allAttachmentsDeleted, tombstone, "TestFunc")

	select {
	case <-allAttachmentsDeleted.done():
	case <-ltime.After(ltime.Second):
		t.Fatal("hook was not released although no VolumeAttachment is left for the node")
	}
}
