package main

import (
	lerrors "errors"
	los "os"
	ltesting "testing"

	lflag "github.com/spf13/pflag"
	lk8score "k8s.io/api/core/v1"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lk8s "k8s.io/client-go/kubernetes"
	lfake "k8s.io/client-go/kubernetes/fake"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
)

// pre-stop-hook is a short-lived command, not a driver mode: it must not be rejected
// as an unknown mode, and it must not require the node flags or the IaaS credentials
// that every driver mode reads from the environment - the node container only needs
// to reach the Kubernetes API to run it.
func TestGetOptionsAcceptsPreStopHook(t *ltesting.T) {
	for _, key := range []string{
		"VNGCLOUD_ACCESS_KEY_ID",
		"VNGCLOUD_SECRET_ACCESS_KEY",
		"VNGCLOUD_IDENTITY_ENDPOINT",
		"VNGCLOUD_VSERVER_ENDPOINT",
	} {
		t.Setenv(key, "")
	}
	defer func(pargs []string) { los.Args = pargs }(los.Args)
	los.Args = []string{"vngcloud-blockstorage-csi-driver", preStopHookCmd}

	options := GetOptions(lflag.NewFlagSet("test", lflag.ContinueOnError))

	if string(options.DriverMode) != preStopHookCmd {
		t.Fatalf("GetOptions() DriverMode = %q, want %q", options.DriverMode, preStopHookCmd)
	}
	if options.Global == nil {
		t.Fatal("GetOptions() returned a nil Global config, which main dereferences")
	}
}

// The exit code is the whole report the hook gives kubelet: 1 shows up as a
// FailedPreStopHook event, while 0 says the hook ran. Getting it backwards either
// hides a broken hook or cries wolf on every termination.
func TestRunPreStopHookExitCodes(t *ltesting.T) {
	testCases := []struct {
		name         string
		nodeName     string
		client       func() (lk8s.Interface, error)
		wantExitCode int
	}{
		{
			name:     "Kubernetes API unreachable: do not hold up termination",
			nodeName: "test-node",
			client: func() (lk8s.Interface, error) {
				return nil, lerrors.New("no in-cluster config")
			},
			wantExitCode: 0,
		},
		{
			name:     "hook fails: make it visible to kubelet",
			nodeName: "",
			client: func() (lk8s.Interface, error) {
				return lfake.NewSimpleClientset(), nil
			},
			wantExitCode: 1,
		},
		{
			name:     "hook ran: the node is not being drained",
			nodeName: "test-node",
			client: func() (lk8s.Interface, error) {
				return lfake.NewSimpleClientset(&lk8score.Node{
					ObjectMeta: lmetav1.ObjectMeta{Name: "test-node"},
				}), nil
			},
			wantExitCode: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *ltesting.T) {
			t.Setenv("CSI_NODE_NAME", tc.nodeName)

			defer func(pclient lscloud.KubernetesAPIClient, pexit func(int)) {
				lscloud.DefaultKubernetesAPIClient = pclient
				osExit = pexit
			}(lscloud.DefaultKubernetesAPIClient, osExit)

			lscloud.DefaultKubernetesAPIClient = tc.client

			gotExitCode := -1
			osExit = func(pcode int) { gotExitCode = pcode }

			runPreStopHook()

			if gotExitCode != tc.wantExitCode {
				t.Fatalf("runPreStopHook() exited %d, want %d", gotExitCode, tc.wantExitCode)
			}
		})
	}
}
