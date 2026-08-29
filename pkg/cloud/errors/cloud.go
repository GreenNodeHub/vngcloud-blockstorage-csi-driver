package errors

import (
	lfmt "fmt"

	lsdkErr "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
)

var (
	ErrVolumeIsInErrorState = func(pvolId string) IError {
		return NewError(new(lsdkErr.SdkError).
			WithErrorCode(EcVServerVolumeIsInErrorState).
			WithErrors(lfmt.Errorf("volume %s is in error state", pvolId)).
			WithMessage(lfmt.Sprintf("volume %s is in error state", pvolId)).
			WithKVparameters("volumeId", pvolId))
	}

	// psdkErr may be nil: the wait helpers construct these errors from a plain
	// timeout with no SDK error to wrap. Dereferencing a nil interface here used
	// to panic the whole controller on every wait timeout.
	ErrVolumeFailedToDetach = func(pinstanceId, pvolId string, psdkErr lsdkErr.IError) IError {
		e := new(lsdkErr.SdkError).
			WithErrorCode(EcVServerVolumeFailedToDetach).
			WithMessage(lfmt.Sprintf("Failed to detach volume %s from instance %s", pvolId, pinstanceId)).
			WithKVparameters("instanceId", pinstanceId, "volumeId", pvolId)
		if psdkErr != nil {
			e = e.WithErrors(psdkErr.GetError()).WithParameters(psdkErr.GetParameters())
		}

		return NewError(e)
	}

	// psdkErr may be nil: the wait helpers construct these errors from a plain
	// timeout with no SDK error to wrap. Dereferencing a nil interface here used
	// to panic the whole controller on every wait timeout.
	ErrVolumeFailedToGet = func(pvolId string, psdkErr lsdkErr.IError) IError {
		e := new(lsdkErr.SdkError).
			WithErrorCode(EcVServerVolumeFailedToGet).
			WithMessage(lfmt.Sprintf("Failed to get volume %s", pvolId)).
			WithKVparameters("volumeId", pvolId)
		if psdkErr != nil {
			e = e.WithErrors(psdkErr.GetError()).WithParameters(psdkErr.GetParameters())
		}

		return NewError(e)
	}

	// psdkErr may be nil: the wait helpers construct these errors from a plain
	// timeout with no SDK error to wrap. Dereferencing a nil interface here used
	// to panic the whole controller on every wait timeout.
	ErrVolumeFailedToDelete = func(pvolId string, psdkErr lsdkErr.IError) IError {
		e := new(lsdkErr.SdkError).
			WithErrorCode(EcVServerVolumeFailedToDelete).
			WithMessage(lfmt.Sprintf("Failed to delete volume %s", pvolId)).
			WithKVparameters("volumeId", pvolId)
		if psdkErr != nil {
			e = e.WithErrors(psdkErr.GetError()).WithParameters(psdkErr.GetParameters())
		}

		return NewError(e)
	}

	ErrVolumeNotFound = func(pvolId string) IError {
		return NewError(new(lsdkErr.SdkError).
			WithErrorCode(EcVServerVolumeNotFound).
			WithMessage(lfmt.Sprintf("Volume %s not found", pvolId)).
			WithKVparameters("volumeId", pvolId))
	}

	// psdkErr may be nil: the wait helpers construct these errors from a plain
	// timeout with no SDK error to wrap. Dereferencing a nil interface here used
	// to panic the whole controller on every wait timeout.
	ErrVolumeFailedToAttach = func(pinstanceId, pvolId string, psdkErr lsdkErr.IError) IError {
		e := new(lsdkErr.SdkError).
			WithErrorCode(EcVServerVolumeFailedToAttach).
			WithMessage(lfmt.Sprintf("Failed to attach volume %s to instance %s", pvolId, pinstanceId)).
			WithKVparameters("instanceId", pinstanceId, "volumeId", pvolId)
		if psdkErr != nil {
			e = e.WithErrors(psdkErr.GetError()).WithParameters(psdkErr.GetParameters())
		}

		return NewError(e)
	}
)
