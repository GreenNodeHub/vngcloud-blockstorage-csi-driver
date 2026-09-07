package driver

import (
	lstr "strings"
	ltesting "testing"

	lcodes "google.golang.org/grpc/codes"
	lstt "google.golang.org/grpc/status"
)

// Item 5: the paused return must not claim the driver tried.
//
// This message lands in VolumeAttachment.status.detachError.message, the first
// place an operator looks. During a breaker pause the driver deliberately did
// NOT issue a detach, so reusing ErrDetachVolume's "CANNOT Detach volume ..."
// tells them the opposite of what happened.
//
// The gRPC code stays codes.Internal on purpose: that is the code this path is
// known to be retried on, and changing it was considered and rejected.
func TestErrDetachVolumePausedDiffersInMessageButNotInCode(t *ltesting.T) {
	const volumeID, nodeID = "vol-a", "ins-1"

	tried := lstt.Convert(ErrDetachVolume(volumeID, nodeID))
	paused := lstt.Convert(ErrDetachVolumePaused(volumeID, nodeID))

	if paused.Code() != tried.Code() {
		t.Fatalf("paused code = %v, want the same as ErrDetachVolume (%v)", paused.Code(), tried.Code())
	}
	if paused.Code() != lcodes.Internal {
		t.Fatalf("paused code = %v, want %v", paused.Code(), lcodes.Internal)
	}
	if paused.Message() == tried.Message() {
		t.Fatalf("both constructors produced the same message %q; the pause must read differently", paused.Message())
	}

	// It must not claim an attempt was made, and it must say the driver is
	// still watching, so an operator does not read the pause as "given up".
	if lstr.Contains(paused.Message(), "CANNOT Detach") {
		t.Fatalf("paused message = %q, still claims a failed attempt", paused.Message())
	}
	for _, want := range []string{volumeID, nodeID, "paus", "prob"} {
		if !lstr.Contains(lstr.ToLower(paused.Message()), lstr.ToLower(want)) {
			t.Fatalf("paused message = %q, does not mention %q", paused.Message(), want)
		}
	}
}
