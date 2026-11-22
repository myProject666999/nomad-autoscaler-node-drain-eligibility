// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package scaleutils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/hashicorp/nomad/api"
	"github.com/shoenig/test/must"
)

// fakeNomad is an in-memory Nomad API substitute for scale-in tests. It
// serves the node list, node info and node allocations endpoints without
// connecting to a real Nomad cluster.
type fakeNomad struct {
	mu sync.Mutex

	nodes    []*api.NodeListStub
	nodeInfo map[string]*api.Node

	// allocSeq holds, per node ID, the sequence of allocation lists returned
	// on successive calls. The last entry is repeated once the sequence is
	// exhausted. This allows tests to change the allocation snapshot between
	// the protection check, the drain wait and the pre-terminate re-check.
	allocSeq map[string][][]*api.Allocation

	// allocStatus, when non-nil, is the HTTP status returned for the node's
	// allocations endpoint, simulating a read failure.
	allocStatus map[string]int

	allocCalls map[string]int
}

func newFakeNomad() *fakeNomad {
	return &fakeNomad{
		nodeInfo:    make(map[string]*api.Node),
		allocSeq:    make(map[string][][]*api.Allocation),
		allocStatus: make(map[string]int),
		allocCalls:  make(map[string]int),
	}
}

// fakeDrainer records UpdateDrainOpts calls and allows per-node behaviour.
type fakeDrainer struct {
	mu sync.Mutex

	// updateErr, when set for a node ID, makes UpdateDrainOpts fail.
	updateErr map[string]error

	// monitorBlock, when true for a node ID, makes MonitorDrain block until
	// the context is done instead of closing immediately.
	monitorBlock map[string]bool

	// monitorErr, when set for a node ID, sends an error message on the
	// monitor channel.
	monitorErr map[string]string

	// delay optionally slows the drain of a node, used to prove result
	// ordering does not depend on goroutine completion order.
	delay map[string]time.Duration

	drainCalls []string
}

func newFakeDrainer() *fakeDrainer {
	return &fakeDrainer{
		updateErr:    make(map[string]error),
		monitorBlock: make(map[string]bool),
		monitorErr:   make(map[string]string),
		delay:        make(map[string]time.Duration),
	}
}

func (f *fakeDrainer) UpdateDrainOpts(nodeID string, opts *api.DrainOptions, _ *api.WriteOptions) (*api.NodeDrainUpdateResponse, error) {
	f.mu.Lock()
	f.drainCalls = append(f.drainCalls, nodeID)
	err := f.updateErr[nodeID]
	delay := f.delay[nodeID]
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return &api.NodeDrainUpdateResponse{}, nil
}

func (f *fakeDrainer) MonitorDrain(ctx context.Context, nodeID string, _ uint64, _ bool) <-chan *api.MonitorMessage {
	f.mu.Lock()
	block := f.monitorBlock[nodeID]
	errMsg := f.monitorErr[nodeID]
	f.mu.Unlock()

	outCh := make(chan *api.MonitorMessage, 1)
	if errMsg != "" {
		outCh <- &api.MonitorMessage{Level: api.MonitorMsgLevelError, Message: errMsg}
		close(outCh)
		return outCh
	}
	if block {
		go func() {
			<-ctx.Done()
			close(outCh)
		}()
		return outCh
	}
	close(outCh)
	return outCh
}

func (f *fakeDrainer) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.drainCalls))
	copy(out, f.drainCalls)
	return out
}

// detailedTestEnv bundles the in-memory substitutes for a detailed scale-in
// test: the Nomad API fake, the drainer fake and the utils under test.
type detailedTestEnv struct {
	cu      *ClusterScaleUtils
	nomad   *fakeNomad
	drainer *fakeDrainer
}

func newDetailedTestEnv(t *testing.T) *detailedTestEnv {
	t.Helper()

	nomad := newFakeNomad()
	nomad.nodes = []*api.NodeListStub{
		{
			ID:                    "node-a",
			NodeClass:             "pool-a",
			Status:                api.NodeStatusReady,
			SchedulingEligibility: api.NodeSchedulingEligible,
		},
		{
			ID:                    "node-b",
			NodeClass:             "pool-a",
			Status:                api.NodeStatusReady,
			SchedulingEligibility: api.NodeSchedulingEligible,
		},
	}
	nomad.nodeInfo["node-a"] = &api.Node{ID: "node-a"}
	nomad.nodeInfo["node-b"] = &api.Node{ID: "node-b"}
	nomad.allocSeq["node-a"] = [][]*api.Allocation{{}}
	nomad.allocSeq["node-b"] = [][]*api.Allocation{{}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		nomad.mu.Lock()
		defer nomad.mu.Unlock()

		switch r.URL.Path {
		case "/v1/nodes":
			must.NoError(t, json.NewEncoder(w).Encode(nomad.nodes))
		case "/v1/node/node-a", "/v1/node/node-b", "/v1/node/node-c":
			n, ok := nomad.nodeInfo[r.URL.Path[len("/v1/node/"):]]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			must.NoError(t, json.NewEncoder(w).Encode(n))
		case "/v1/node/node-a/allocations", "/v1/node/node-b/allocations", "/v1/node/node-c/allocations":
			nodeID := r.URL.Path[len("/v1/node/") : len(r.URL.Path)-len("/allocations")]

			if status, ok := nomad.allocStatus[nodeID]; ok {
				w.WriteHeader(status)
				return
			}

			seq := nomad.allocSeq[nodeID]
			call := nomad.allocCalls[nodeID]
			nomad.allocCalls[nodeID] = call + 1

			var allocs []*api.Allocation
			if len(seq) > 0 {
				idx := min(call, len(seq)-1)
				allocs = seq[idx]
			}

			w.Header().Set("X-Nomad-Index", fmt.Sprintf("%d", call+1))
			must.NoError(t, json.NewEncoder(w).Encode(allocs))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	drainer := newFakeDrainer()

	cu := &ClusterScaleUtils{
		log:    hclog.NewNullLogger(),
		client: testNomadClient(t, server.URL),
		ClusterNodeIDLookupFunc: func(node *api.Node) (string, error) {
			return node.ID, nil
		},
		drainer: drainer,
	}

	return &detailedTestEnv{cu: cu, nomad: nomad, drainer: drainer}
}

func detailedCfg(extra map[string]string) map[string]string {
	cfg := testScaleInCfg()
	for k, v := range extra {
		cfg[k] = v
	}
	return cfg
}

func resultForNode(results []NodeDrainResult, nodeID string) *NodeDrainResult {
	for i := range results {
		if results[i].NomadNodeID == nodeID {
			return &results[i]
		}
	}
	return nil
}

func TestDetailed_noAllocationsProtectionEnabled(t *testing.T) {
	env := newDetailedTestEnv(t)

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{sdk.TargetConfigKeyNodeProtectAllocations: "true"}),
		[]string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 2, res.Terminate)
	must.Eq(t, "node-a", res.Terminate[0].NomadNodeID)
	must.Eq(t, "node-b", res.Terminate[1].NomadNodeID)

	// The provider-facing set contains only confirmed nodes, and every node
	// was actually drained. Drain calls are concurrent, so compare as a set.
	drainCalls := env.drainer.calls()
	slices.Sort(drainCalls)
	must.Eq(t, []string{"node-a", "node-b"}, drainCalls)
	for _, r := range res.NodeResults {
		must.Eq(t, NodeDrainStatusDrained, r.Status)
	}
}

func TestDetailed_protectedAllocation(t *testing.T) {
	env := newDetailedTestEnv(t)

	// node-b runs a non-terminal allocation of the protected "db" job.
	env.nomad.allocSeq["node-b"] = [][]*api.Allocation{
		{testAlloc("alloc-1", "db", api.AllocClientStatusRunning)},
	}

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{
			sdk.TargetConfigKeyNodeProtectAllocations: "true",
			sdk.TargetConfigKeyNodeProtectJobs:        "db",
		}),
		[]string{"node-a", "node-b"}, 2)

	must.NoError(t, err)

	// Only node-a is authorised for termination; node-b was refused before
	// any drain was triggered on it.
	must.Len(t, 1, res.Terminate)
	must.Eq(t, "node-a", res.Terminate[0].NomadNodeID)
	must.Eq(t, []string{"node-a"}, env.drainer.calls())

	rb := resultForNode(res.NodeResults, "node-b")
	must.NotNil(t, rb)
	must.Eq(t, NodeDrainStatusProtected, rb.Status)
	must.Eq(t, NodeDrainStageProtection, rb.Stage)
	must.Eq(t, NodeProtectReasonProtectedJob, rb.Reason)
}

func TestDetailed_terminalAllocationsDoNotProtect(t *testing.T) {
	env := newDetailedTestEnv(t)

	env.nomad.allocSeq["node-b"] = [][]*api.Allocation{
		{testAlloc("alloc-1", "db", api.AllocClientStatusComplete)},
	}

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{
			sdk.TargetConfigKeyNodeProtectAllocations: "true",
			sdk.TargetConfigKeyNodeProtectJobs:        "db",
			sdk.TargetConfigKeyNodeProtectAll:         "true",
		}),
		[]string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 2, res.Terminate)
}

func TestDetailed_allocationReadFailureIsNotEmpty(t *testing.T) {
	env := newDetailedTestEnv(t)

	// The allocations endpoint for node-b fails; it must not be treated as
	// an empty node.
	env.nomad.allocStatus["node-b"] = http.StatusInternalServerError

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{sdk.TargetConfigKeyNodeProtectAllocations: "true"}),
		[]string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 1, res.Terminate)
	must.Eq(t, "node-a", res.Terminate[0].NomadNodeID)
	must.Eq(t, []string{"node-a"}, env.drainer.calls())

	rb := resultForNode(res.NodeResults, "node-b")
	must.NotNil(t, rb)
	must.Eq(t, NodeDrainStatusFailed, rb.Status)
	must.Eq(t, NodeDrainStageProtection, rb.Stage)
	must.Eq(t, NodeProtectReasonAllocationsUnreadable, rb.Reason)
}

func TestDetailed_autoscalerNodeExcluded(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.cu.curNodeID = "node-a"

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 1, res.Terminate)
	must.Eq(t, "node-b", res.Terminate[0].NomadNodeID)
	must.Eq(t, []string{"node-b"}, env.drainer.calls())
}

func TestDetailed_ineligibleNodeRemainsCandidate(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.nomad.nodes[1].SchedulingEligibility = api.NodeSchedulingIneligible

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 2, res.Terminate)
}

func TestDetailed_outOfGroupRemoteIDsFiltered(t *testing.T) {
	env := newDetailedTestEnv(t)

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"node-a"}, 2)

	must.NoError(t, err)
	must.Len(t, 1, res.Terminate)
	must.Eq(t, "node-a", res.Terminate[0].NomadNodeID)
	must.Eq(t, []string{"node-a"}, env.drainer.calls())
}

func TestDetailed_partialDrainFailureTerminatesSuccesses(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.drainer.updateErr["node-b"] = errors.New("nomad api unavailable")

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"node-a", "node-b"}, 2)

	// The default partial failure policy terminates the confirmed subset.
	must.NoError(t, err)
	must.Len(t, 1, res.Terminate)
	must.Eq(t, "node-a", res.Terminate[0].NomadNodeID)

	rb := resultForNode(res.NodeResults, "node-b")
	must.NotNil(t, rb)
	must.Eq(t, NodeDrainStatusFailed, rb.Status)
	must.Eq(t, NodeDrainStageDrain, rb.Stage)
	must.Eq(t, NodeDrainReasonDrainAPIError, rb.Reason)
}

func TestDetailed_partialDrainFailureAbortPolicy(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.drainer.updateErr["node-b"] = errors.New("nomad api unavailable")

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{sdk.TargetConfigKeyScaleInPartialFailure: ScaleInPartialFailureAbort}),
		[]string{"node-a", "node-b"}, 2)

	// The abort policy authorises nothing, even though node-a drained.
	must.Error(t, err)
	must.NotNil(t, res)
	must.Len(t, 0, res.Terminate)

	ra := resultForNode(res.NodeResults, "node-a")
	must.NotNil(t, ra)
	must.Eq(t, NodeDrainStatusDrained, ra.Status)
}

func TestDetailed_drainTimeout(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.drainer.monitorBlock["node-a"] = true

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(ctx,
		detailedCfg(nil), []string{"node-a"}, 1)

	must.NoError(t, err)
	must.Len(t, 0, res.Terminate)

	ra := resultForNode(res.NodeResults, "node-a")
	must.NotNil(t, ra)
	must.Eq(t, NodeDrainStatusTimeout, ra.Status)
	must.Eq(t, NodeDrainStageDrain, ra.Stage)
	must.Eq(t, NodeDrainReasonDrainTimeout, ra.Reason)
}

func TestDetailed_contextCancelledBeforeStart(t *testing.T) {
	env := newDetailedTestEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(ctx,
		detailedCfg(nil), []string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 0, res.Terminate)

	// No drain was started for any node.
	must.Len(t, 0, env.drainer.calls())

	for _, id := range []string{"node-a", "node-b"} {
		r := resultForNode(res.NodeResults, id)
		must.NotNil(t, r)
		must.Eq(t, NodeDrainStatusCancelled, r.Status)
		must.Eq(t, NodeDrainReasonNotStarted, r.Reason)
	}
}

func TestDetailed_tasksNotDeadAfterDrain(t *testing.T) {
	env := newDetailedTestEnv(t)

	// The drain completes but a task stays running; the wait for dead tasks
	// must not be confused with the drain completing.
	env.nomad.allocSeq["node-a"] = [][]*api.Allocation{
		{{ID: "alloc-1", TaskStates: map[string]*api.TaskState{"main": {State: "running"}}}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(ctx,
		detailedCfg(nil), []string{"node-a"}, 1)

	must.NoError(t, err)
	must.Len(t, 0, res.Terminate)

	ra := resultForNode(res.NodeResults, "node-a")
	must.NotNil(t, ra)
	must.Eq(t, NodeDrainStatusTimeout, ra.Status)
	must.Eq(t, NodeDrainStageWaitTasks, ra.Stage)
}

func TestDetailed_remoteIDConflict(t *testing.T) {
	env := newDetailedTestEnv(t)

	// Both nodes claim the same remote instance ID.
	env.cu.ClusterNodeIDLookupFunc = func(node *api.Node) (string, error) {
		return "shared-remote-id", nil
	}

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"shared-remote-id"}, 2)

	// Neither node may be drained or terminated when the mapping conflicts.
	must.Error(t, err)
	must.NotNil(t, res)
	must.Len(t, 0, res.Terminate)
	must.Len(t, 0, env.drainer.calls())

	for _, id := range []string{"node-a", "node-b"} {
		r := resultForNode(res.NodeResults, id)
		must.NotNil(t, r)
		must.Eq(t, NodeDrainStatusMappingFailed, r.Status)
		must.Eq(t, NodeDrainStageRemoteMapping, r.Stage)
		must.Eq(t, NodeDrainReasonRemoteIDConflict, r.Reason)
	}
}

func TestDetailed_preTerminateRecheck(t *testing.T) {
	env := newDetailedTestEnv(t)

	// node-a is empty at the protection check and during the drain, but a
	// protected allocation appears before termination is authorised.
	env.nomad.allocSeq["node-a"] = [][]*api.Allocation{
		{},
		{},
		{testAlloc("alloc-9", "db", api.AllocClientStatusRunning)},
	}

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{
			sdk.TargetConfigKeyNodeProtectAllocations: "true",
			sdk.TargetConfigKeyNodeProtectJobs:        "db",
		}),
		[]string{"node-a"}, 1)

	must.NoError(t, err)
	must.Len(t, 0, res.Terminate)

	ra := resultForNode(res.NodeResults, "node-a")
	must.NotNil(t, ra)
	must.Eq(t, NodeDrainStatusProtected, ra.Status)
	must.Eq(t, NodeDrainStagePreTerminate, ra.Stage)
	must.Eq(t, NodeProtectReasonProtectedJob, ra.Reason)
}

func TestDetailed_shortfallFailPolicy(t *testing.T) {
	env := newDetailedTestEnv(t)

	env.nomad.allocSeq["node-b"] = [][]*api.Allocation{
		{testAlloc("alloc-1", "db", api.AllocClientStatusRunning)},
	}

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{
			sdk.TargetConfigKeyNodeProtectAllocations: "true",
			sdk.TargetConfigKeyNodeProtectJobs:        "db",
			sdk.TargetConfigKeyScaleInShortfall:       ScaleInShortfallFail,
		}),
		[]string{"node-a", "node-b"}, 2)

	// The fail policy aborts rather than scaling in fewer nodes.
	must.Error(t, err)
	must.NotNil(t, res)
	must.Len(t, 0, res.Terminate)
	must.Len(t, 0, env.drainer.calls())
}

func TestDetailed_resultOrderIsDeterministic(t *testing.T) {
	env := newDetailedTestEnv(t)

	// Slow down the first selected node; the second will finish first.
	env.drainer.delay["node-a"] = 150 * time.Millisecond

	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"node-a", "node-b"}, 2)

	must.NoError(t, err)
	must.Len(t, 2, res.Terminate)

	// The result order follows the candidate order, not the goroutine
	// completion order.
	must.Eq(t, "node-a", res.NodeResults[0].NomadNodeID)
	must.Eq(t, "node-b", res.NodeResults[1].NomadNodeID)
}

func TestDetailed_retryRecognizesExistingDrainState(t *testing.T) {
	env := newDetailedTestEnv(t)

	// node-b is already draining, e.g. from a previous scale-in round whose
	// cloud termination failed. The default filter treats this as an
	// unstable pool and refuses to proceed, rather than silently
	// re-selecting the node.
	env.nomad.nodes[1].Drain = true

	_, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(nil), []string{"node-a", "node-b"}, 1)
	must.Error(t, err)
	must.ErrorContains(t, err, "draining")
	must.Len(t, 0, env.drainer.calls())

	// With node_filter_ignore_drain the action proceeds, but the already
	// draining node is not re-selected as a candidate: a retry never
	// re-drains or re-terminates a node a previous round already drained,
	// and the helper never restores scheduling eligibility itself.
	res, err := env.cu.RunPreScaleInTasksWithRemoteCheckDetailed(t.Context(),
		detailedCfg(map[string]string{XNodeFilterOptionIgnoreDrain: "true"}),
		[]string{"node-a", "node-b"}, 2)
	must.NoError(t, err)
	must.Len(t, 1, res.Terminate)
	must.Eq(t, "node-a", res.Terminate[0].NomadNodeID)
	must.Eq(t, []string{"node-a"}, env.drainer.calls())
}

func TestDetailed_compatWrapperFailsOnAnyFailure(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.drainer.updateErr["node-b"] = errors.New("nomad api unavailable")

	// The compatibility wrapper preserves the historical contract: any
	// failure fails the whole action and nothing is returned.
	ids, err := env.cu.RunPreScaleInTasksWithRemoteCheck(t.Context(),
		detailedCfg(nil), []string{"node-a", "node-b"}, 2)

	must.Error(t, err)
	must.Nil(t, ids)
}

func TestDrainNodesDetailed_classification(t *testing.T) {
	env := newDetailedTestEnv(t)
	env.drainer.updateErr["node-b"] = errors.New("boom")

	nodes := []NodeResourceID{
		{NomadNodeID: "node-a", RemoteResourceID: "node-a"},
		{NomadNodeID: "node-b", RemoteResourceID: "node-b"},
	}

	results, err := env.cu.DrainNodesDetailed(t.Context(), detailedCfg(nil), nodes)
	must.NoError(t, err)
	must.Len(t, 2, results)

	must.Eq(t, NodeDrainStatusDrained, results[0].Status)
	must.Eq(t, NodeDrainReasonDrained, results[0].Reason)

	must.Eq(t, NodeDrainStatusFailed, results[1].Status)
	must.Eq(t, NodeDrainStageDrain, results[1].Stage)
	must.ErrorContains(t, results[1].Err, "boom")
}

func TestDrainNodesDetailed_contextDoneBeforeStart(t *testing.T) {
	env := newDetailedTestEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	nodes := []NodeResourceID{
		{NomadNodeID: "node-a", RemoteResourceID: "node-a"},
		{NomadNodeID: "node-b", RemoteResourceID: "node-b"},
	}

	results, err := env.cu.DrainNodesDetailed(ctx, detailedCfg(nil), nodes)
	must.NoError(t, err)
	must.Len(t, 2, results)

	must.Len(t, 0, env.drainer.calls())
	for _, r := range results {
		must.Eq(t, NodeDrainStatusCancelled, r.Status)
		must.Eq(t, NodeDrainReasonNotStarted, r.Reason)
		must.ErrorIs(t, r.Err, context.Canceled)
	}
}
