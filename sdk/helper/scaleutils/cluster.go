// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package scaleutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	hclog "github.com/hashicorp/go-hclog"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	errHelper "github.com/hashicorp/nomad-autoscaler/sdk/helper/error"
	"github.com/hashicorp/nomad-autoscaler/sdk/helper/scaleutils/nodepool"
	"github.com/hashicorp/nomad-autoscaler/sdk/helper/scaleutils/nodeselector"
	"github.com/hashicorp/nomad/api"
)

// We use the node drainer interface in order to separate the nomad-autoscaler
// node drain logic from the actual drain execution, which is executed using the
// nomad API, allowing us to better define the boundary and write better tests.
type nodeDrainer interface {
	UpdateDrainOpts(nodeID string, opts *api.DrainOptions, q *api.WriteOptions) (*api.NodeDrainUpdateResponse, error)
	MonitorDrain(ctx context.Context, nodeID string, index uint64, ignoreSys bool) <-chan *api.MonitorMessage
}

var errInvalidScaleInNum = errors.New("number of nodes requested for removal must be greater than zero")

// ClusterScaleUtils provides common functionality when performing horizontal
// cluster scaling evaluations and actions.
type ClusterScaleUtils struct {
	log    hclog.Logger
	client *api.Client

	// curNodeID is the ID of the node that the Nomad Autoscaler is currently
	// running on.
	//
	// TODO(jrasell) this should be removed once the cluster targets and core
	//  autoscaler components are updated to handle reconciliation.
	curNodeID string

	// ClusterNodeIDLookupFunc is the callback function used to translate a
	// Nomad nodes ID to the remote resource ID used by the target platform.
	ClusterNodeIDLookupFunc ClusterNodeIDLookupFunc

	drainer nodeDrainer
}

// NewClusterScaleUtils instantiates a new ClusterScaleUtils object for use.
func NewClusterScaleUtils(cfg *api.Config, log hclog.Logger) (*ClusterScaleUtils, error) {

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate Nomad client: %v", err)
	}

	// Identifying the node is best-effort and should not result in a terminal
	// error when setting up the utils.
	id, err := autoscalerNodeID(client)
	if err != nil {
		log.Error("failed to identify Nomad Autoscaler nodeID", "error", err)
	}

	return &ClusterScaleUtils{
		log:       log,
		client:    client,
		curNodeID: id,
		drainer:   client.Nodes(),
	}, nil
}

// RunPreScaleInTasks triggers all the tasks, including node identification and
// draining, required before terminating the nodes in the remote provider.
// COMPAT(0.4)
func (c *ClusterScaleUtils) RunPreScaleInTasks(ctx context.Context, cfg map[string]string, num int) ([]NodeResourceID, error) {

	// Check that the ClusterNodeIDLookupFunc has been set, otherwise we cannot
	// attempt to identify nodes and their remote resource IDs.
	if c.ClusterNodeIDLookupFunc == nil {
		return nil, errors.New("required ClusterNodeIDLookupFunc not set")
	}

	nodes, err := c.IdentifyScaleInNodes(cfg, num)
	if err != nil {
		return nil, err
	}

	// Technically we do not need this information until after the nodes have
	// been drained. However, this doesn't change cluster state and so do this
	// first to make sure there are no issues in translating.
	nodeResourceIDs, err := c.IdentifyScaleInRemoteIDs(nodes)
	if err != nil {
		return nil, err
	}

	// Drain the nodes.
	// TODO(jrasell) we should try some reconciliation here, where we identify
	//  failed nodes and continue with nodes that drained successfully.
	if err := c.DrainNodes(ctx, cfg, nodeResourceIDs); err != nil {
		return nil, err
	}
	c.log.Debug("pre scale-in tasks now complete")

	return nodeResourceIDs, nil
}

// ScaleInResult is the outcome of the pre scale-in tasks. It separates the
// nodes that are confirmed safe to terminate from the per-node detail of the
// whole attempt, so callers never have to treat a candidate list as
// termination authorisation.
type ScaleInResult struct {

	// Terminate is the confirmed set of nodes that drained successfully and
	// passed every protection check, deduplicated by remote resource ID.
	// These, and only these, may be terminated in the remote provider. It
	// is nil when the partial failure policy aborted the action.
	Terminate []NodeResourceID

	// NodeResults holds one result per node that entered the pipeline, in a
	// deterministic order that does not depend on goroutine completion.
	NodeResults []NodeDrainResult
}

// RunPreScaleInTasksWithRemoteCheck triggers all the tasks, including node
// identification, filtering by remote ID, and draining, required before
// terminating the nodes in the remote provider.
//
// The returned list contains only nodes that drained successfully. If any
// node fails to drain, an error is returned and no termination set is
// produced, matching the historical behaviour of this function. Callers that
// can act on partial success should use
// RunPreScaleInTasksWithRemoteCheckDetailed.
func (c *ClusterScaleUtils) RunPreScaleInTasksWithRemoteCheck(ctx context.Context, cfg map[string]string, remoteIDs []string, num int) ([]NodeResourceID, error) {

	result, err := c.RunPreScaleInTasksWithRemoteCheckDetailed(ctx, cfg, remoteIDs, num)
	if err != nil {
		return nil, err
	}

	// Preserve the historical contract: any node that failed, timed out or
	// was cancelled fails the whole action and nothing is returned for
	// termination.
	var mErr *multierror.Error
	for _, r := range result.NodeResults {
		if r.failed() {
			mErr = multierror.Append(mErr, fmt.Errorf("node %s: %s", r.NomadNodeID, resultErrorString(r)))
		}
	}
	if mErr != nil {
		return nil, errHelper.FormattedMultiError(mErr)
	}

	return result.Terminate, nil
}

// resultErrorString renders a node result for error aggregation, preferring
// the underlying error and falling back to the stable reason.
func resultErrorString(r NodeDrainResult) string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return r.Reason
}

// RunPreScaleInTasksWithRemoteCheckDetailed triggers all the tasks required
// before terminating nodes in the remote provider and reports the outcome
// per node.
//
// The pipeline is: identify eligible nodes, map them to remote provider IDs,
// restrict them to the supplied remote ID set, refuse nodes protected by
// allocation rules, select candidates with the configured node selector,
// drain them, and finally re-check protection before authorising
// termination. Only nodes reported in ScaleInResult.Terminate may be
// terminated; failed, unknown, cancelled and protected nodes never enter
// that set.
func (c *ClusterScaleUtils) RunPreScaleInTasksWithRemoteCheckDetailed(ctx context.Context, cfg map[string]string, remoteIDs []string, num int) (*ScaleInResult, error) {

	// Check that the ClusterNodeIDLookupFunc has been set, otherwise we cannot
	// attempt to identify nodes and their remote resource IDs.
	if c.ClusterNodeIDLookupFunc == nil {
		return nil, errors.New("required ClusterNodeIDLookupFunc not set")
	}

	if num < 1 {
		return nil, errInvalidScaleInNum
	}

	protection, err := newAllocProtection(cfg)
	if err != nil {
		return nil, err
	}

	shortfall, err := scaleInShortfallPolicy(cfg)
	if err != nil {
		return nil, err
	}

	partialFailure, err := scaleInPartialFailurePolicy(cfg)
	if err != nil {
		return nil, err
	}

	result := &ScaleInResult{}

	// Find nodes in Nomad that match the node filtering criteria.
	nodes, err := c.identifyEligibleScaleInNodes(cfg)
	if err != nil {
		return nil, err
	}

	// Map each node to its remote provider ID. Nodes that cannot be mapped,
	// or whose mapping conflicts, are recorded and excluded; they are never
	// treated as empty or silently dropped.
	mapped, mapResults := c.mapNodesToRemoteIDs(nodes)
	result.NodeResults = append(result.NodeResults, mapResults...)

	// Index nodes by Nomad node ID for easier access.
	nodesMap := make(map[string]*api.NodeListStub)
	for _, n := range nodes {
		nodesMap[n.ID] = n
	}

	// Filter out nodes that are not part of the remote ID list, keeping the
	// existing guarantee that only instances within the target provider
	// group are considered.
	inGroup := []NodeResourceID{}
	for _, id := range mapped {
		for _, remoteID := range remoteIDs {
			if id.RemoteResourceID == remoteID {
				c.log.Debug("node is part of the policy target", "node_id", id.NomadNodeID, "remote_id", id.RemoteResourceID)
				inGroup = append(inGroup, id)
				break
			}
		}
	}

	// Refuse nodes protected by their current allocation snapshot before
	// any drain is triggered. Nodes whose allocations cannot be read are
	// refused too; an unreadable node is not an empty node.
	candidates := inGroup
	if protection.enabled {
		candidates = nil
		for _, id := range inGroup {
			reason, protected := c.nodeProtectionStatus(id.NomadNodeID, protection)
			switch {
			case protected:
				result.NodeResults = append(result.NodeResults, NodeDrainResult{
					NodeResourceID: id,
					Status:         NodeDrainStatusProtected,
					Stage:          NodeDrainStageProtection,
					Reason:         reason,
				})
				c.log.Info("node protected from scale in by allocation rules",
					"node_id", id.NomadNodeID, "remote_id", id.RemoteResourceID, "reason", reason)
			case reason == NodeProtectReasonAllocationsUnreadable:
				result.NodeResults = append(result.NodeResults, NodeDrainResult{
					NodeResourceID: id,
					Status:         NodeDrainStatusFailed,
					Stage:          NodeDrainStageProtection,
					Reason:         reason,
					Err:            fmt.Errorf("failed to read allocations for node %s", id.NomadNodeID),
				})
			default:
				candidates = append(candidates, id)
			}
		}
	}

	// Ensure we have not filtered out all the available nodes.
	if len(candidates) == 0 {
		return result, fmt.Errorf("no nodes identified for scaling in action")
	}

	// If the caller has requested more nodes than we have available once
	// filtered, the configured shortfall policy decides whether we scale in
	// fewer nodes or abort. Protection is never skipped to make up the
	// count, and the autoscaler's own node is never a candidate.
	if num > len(candidates) {
		if shortfall == ScaleInShortfallFail {
			return result, fmt.Errorf("insufficient unprotected nodes for scale in: requested %d, available %d",
				num, len(candidates))
		}
		c.log.Warn("can only identify portion of requested nodes for removal",
			"requested", num, "available", len(candidates))
		num = len(candidates)
	}

	// Select which nodes to drain and terminate based on the policy's
	// node selector strategy.
	candidateStubs := make([]*api.NodeListStub, 0, len(candidates))
	for _, id := range candidates {
		candidateStubs = append(candidateStubs, nodesMap[id.NomadNodeID])
	}
	selectedNodes, err := c.SelectScaleInNodes(candidateStubs, cfg, num)
	if err != nil {
		return result, err
	}

	selectedMap := make(map[string]NodeResourceID, len(candidates))
	for _, id := range candidates {
		selectedMap[id.NomadNodeID] = id
	}
	selectedResourceIDs := []NodeResourceID{}
	for _, n := range selectedNodes {
		c.log.Debug("node selected for removal", "node_id", n.ID, "remote_id", selectedMap[n.ID].RemoteResourceID)
		selectedResourceIDs = append(selectedResourceIDs, selectedMap[n.ID])
	}

	// Drain the nodes, collecting a per-node result.
	drainResults, err := c.DrainNodesDetailed(ctx, cfg, selectedResourceIDs)
	if err != nil {
		return result, err
	}

	// Re-confirm the protection rules against a fresh allocation snapshot
	// before authorising termination. Allocations can change between the
	// first snapshot and the end of the drain; this narrows, but cannot
	// eliminate, the race window between the snapshot and the remote
	// provider termination call.
	if protection.enabled {
		for i := range drainResults {
			if drainResults[i].Status != NodeDrainStatusDrained {
				continue
			}
			reason, protected := c.nodeProtectionStatus(drainResults[i].NomadNodeID, protection)
			switch {
			case protected:
				drainResults[i].Status = NodeDrainStatusProtected
				drainResults[i].Stage = NodeDrainStagePreTerminate
				drainResults[i].Reason = reason
				c.log.Warn("node became protected after drain, refusing termination",
					"node_id", drainResults[i].NomadNodeID, "reason", reason)
			case reason == NodeProtectReasonAllocationsUnreadable:
				drainResults[i].Status = NodeDrainStatusFailed
				drainResults[i].Stage = NodeDrainStagePreTerminate
				drainResults[i].Reason = reason
				drainResults[i].Err = fmt.Errorf("failed to read allocations for node %s", drainResults[i].NomadNodeID)
			}
		}
	}
	result.NodeResults = append(result.NodeResults, drainResults...)

	// Build the termination set from the confirmed nodes only, terminating
	// each remote ID at most once.
	seenRemoteIDs := make(map[string]struct{})
	for _, r := range drainResults {
		if !r.terminatable() {
			continue
		}
		if _, ok := seenRemoteIDs[r.RemoteResourceID]; ok {
			c.log.Warn("skipping duplicate remote ID in termination set",
				"node_id", r.NomadNodeID, "remote_id", r.RemoteResourceID)
			continue
		}
		seenRemoteIDs[r.RemoteResourceID] = struct{}{}
		result.Terminate = append(result.Terminate, r.NodeResourceID)
	}

	// Whether partial success still terminates the confirmed subset is an
	// explicit policy. On abort, nothing is authorised for termination.
	failures := 0
	for _, r := range result.NodeResults {
		if r.failed() {
			failures++
		}
	}
	if failures > 0 && partialFailure == ScaleInPartialFailureAbort {
		result.Terminate = nil
		return result, fmt.Errorf("scale in aborted: %d node(s) did not complete successfully", failures)
	}

	c.log.Debug("pre scale-in tasks now complete")
	return result, nil
}

// nodeProtectionStatus reads the node's current allocation snapshot and
// evaluates the protection rules against it. A read failure is reported as
// the allocations-unreadable reason rather than as an empty node.
func (c *ClusterScaleUtils) nodeProtectionStatus(nodeID string, protection *allocProtection) (string, bool) {
	allocs, _, err := c.client.Nodes().Allocations(nodeID, nil)
	if err != nil {
		c.log.Error("failed to read node allocations for protection check", "node_id", nodeID, "error", err)
		return NodeProtectReasonAllocationsUnreadable, false
	}
	return protection.nodeProtectionReason(allocs)
}

// mapNodesToRemoteIDs translates each node to its remote provider ID on a
// per-node basis. Nodes that fail translation, or whose remote ID is claimed
// by more than one node, are returned as mapping-failed results and excluded
// from the mapped list.
func (c *ClusterScaleUtils) mapNodesToRemoteIDs(nodes []*api.NodeListStub) ([]NodeResourceID, []NodeDrainResult) {

	var results []NodeDrainResult
	mapped := make([]NodeResourceID, 0, len(nodes))

	for _, node := range nodes {

		// Read the full node object from the API which will contain the full
		// information required to identify the remote provider ID.
		nodeInfo, _, err := c.client.Nodes().Info(node.ID, nil)
		if err != nil {
			results = append(results, NodeDrainResult{
				NodeResourceID: NodeResourceID{NomadNodeID: node.ID},
				Status:         NodeDrainStatusMappingFailed,
				Stage:          NodeDrainStageRemoteMapping,
				Reason:         NodeDrainReasonNodeInfoUnreadable,
				Err:            err,
			})
			continue
		}

		// Use the identification function to attempt to pull the remote
		// provider ID information from the Node info.
		id, err := c.ClusterNodeIDLookupFunc(nodeInfo)
		if err != nil {
			results = append(results, NodeDrainResult{
				NodeResourceID: NodeResourceID{NomadNodeID: node.ID},
				Status:         NodeDrainStatusMappingFailed,
				Stage:          NodeDrainStageRemoteMapping,
				Reason:         NodeDrainReasonRemoteIDLookupFailed,
				Err:            err,
			})
			continue
		}

		c.log.Debug("identified remote provider ID for node", "node_id", nodeInfo.ID, "remote_id", id)
		mapped = append(mapped, NodeResourceID{NomadNodeID: node.ID, RemoteResourceID: id})
	}

	// A remote ID claimed by more than one node is a mapping conflict: we
	// cannot tell which node owns the instance, so every node involved is
	// refused rather than risking terminating the wrong instance.
	remoteIDCount := make(map[string]int, len(mapped))
	for _, id := range mapped {
		remoteIDCount[id.RemoteResourceID]++
	}

	out := make([]NodeResourceID, 0, len(mapped))
	for _, id := range mapped {
		if remoteIDCount[id.RemoteResourceID] > 1 {
			results = append(results, NodeDrainResult{
				NodeResourceID: id,
				Status:         NodeDrainStatusMappingFailed,
				Stage:          NodeDrainStageRemoteMapping,
				Reason:         NodeDrainReasonRemoteIDConflict,
				Err:            fmt.Errorf("remote ID %s claimed by multiple nodes", id.RemoteResourceID),
			})
			continue
		}
		out = append(out, id)
	}

	return out, results
}

func (c *ClusterScaleUtils) IdentifyScaleInNodes(cfg map[string]string, num int) ([]*api.NodeListStub, error) {

	if num < 1 {
		return nil, errInvalidScaleInNum
	}

	filteredNodes, err := c.identifyEligibleScaleInNodes(cfg)
	if err != nil {
		return nil, err
	}

	// If the caller has requested more nodes than we have available once
	// filtered, adjust the value. This shouldn't cause the whole scaling
	// action to fail, but we should warn.
	if num > len(filteredNodes) {
		c.log.Warn("can only identify portion of requested nodes for removal",
			"requested", num, "available", len(filteredNodes))
		num = len(filteredNodes)
	}

	selectedNodes, err := c.SelectScaleInNodes(filteredNodes, cfg, num)
	if err != nil {
		return nil, err
	}

	return selectedNodes, nil
}

func (c *ClusterScaleUtils) identifyEligibleScaleInNodes(cfg map[string]string) ([]*api.NodeListStub, error) {

	ids, err := nodepool.NewClusterNodePoolIdentifierList(cfg)
	if err != nil {
		return nil, err
	}
	c.log.Debug("performing node pool filtering", "identifiers", ids.String())

	// Pull a current list of Nomad nodes from the API including populated
	// resource fields.
	nodes, _, err := c.client.Nodes().List(&api.QueryOptions{Params: map[string]string{"resources": "true"}})
	if err != nil {
		return nil, fmt.Errorf("failed to list Nomad nodes from API: %v", err)
	}

	if c.log.IsDebug() {
		for _, n := range nodes {
			c.log.Debug("found node",
				"node_id", n.ID, "datacenter", n.Datacenter, "node_class", n.NodeClass, "node_pool", n.NodePool,
				"status", n.Status, "eligibility", n.SchedulingEligibility, "draining", n.Drain,
			)
		}
	}

	// For scale-in, ineligible nodes are valid termination candidates and
	// should not be excluded from the pool. Default to including them unless
	// the caller has explicitly configured otherwise.
	if _, ok := cfg[XNodeFilterOptionIgnoreIneligible]; !ok {
		cfg[XNodeFilterOptionIgnoreIneligible] = "true"
	}

	// Filter our nodes to select only those within our identified pool.
	filterOpts, err := NewNodeFilterOptions(cfg)
	if err != nil {
		return nil, err
	}

	filteredNodes, err := FilterNodesWithOptions(nodes, ids.IsPoolMember, filterOpts)
	if err != nil {
		return nil, err
	}

	// Filter out the Nomad node ID where this autoscaler instance is running.
	filteredNodes = filterOutNodeID(filteredNodes, c.curNodeID)

	if c.log.IsDebug() {
		for _, n := range filteredNodes {
			c.log.Debug("node passed filter criteria", "node_id", n.ID)
		}
	}

	// Ensure we have not filtered out all the available nodes.
	if len(filteredNodes) == 0 {
		return nil, fmt.Errorf("no nodes unfiltered for pool identifiers: %v", ids)
	}

	return filteredNodes, nil
}

func (c *ClusterScaleUtils) IdentifyScaleInRemoteIDs(nodes []*api.NodeListStub) ([]NodeResourceID, error) {

	var (
		out  []NodeResourceID
		mErr *multierror.Error
	)

	for _, node := range nodes {

		// Read the full node object from the API which will contain the full
		// information required to identify the remote provider ID. If we get a
		// single error here, it's likely we won't be able to perform any of the
		// API calls, therefore just exit rather than collect all the errors.
		nodeInfo, _, err := c.client.Nodes().Info(node.ID, nil)
		if err != nil {
			return nil, err
		}

		// Use the identification function to attempt to pull the remote
		// provider ID information from the Node info.
		id, err := c.ClusterNodeIDLookupFunc(nodeInfo)
		if err != nil {
			mErr = multierror.Append(mErr, err)
			continue
		}

		// Add a nice log message for the operators so they can see the node
		// that has been identified if they wish.
		c.log.Debug("identified remote provider ID for node", "node_id", nodeInfo.ID, "remote_id", id)
		out = append(out, NodeResourceID{NomadNodeID: node.ID, RemoteResourceID: id})
	}

	if mErr != nil {
		return nil, errHelper.FormattedMultiError(mErr)
	}
	return out, nil
}

func (c *ClusterScaleUtils) SelectScaleInNodes(nodes []*api.NodeListStub, cfg map[string]string, num int) ([]*api.NodeListStub, error) {
	// Setup the node selector used to identify suitable nodes for termination.
	selector, err := nodeselector.NewSelector(cfg, c.client, c.log)
	if err != nil {
		return nil, err
	}
	c.log.Debug("performing node selection", "selector_strategy", selector.Name())

	// It is possible the selector is unable to identify suitable nodes and so
	// we should return an error to stop additional execution.
	out := selector.Select(nodes, num)
	if len(out) < 1 {
		return nil, fmt.Errorf("no nodes selected using strategy %s", selector.Name())
	}

	// Selection can filter most of the nodes in the filtered list, we should
	// log about this, but using certain strategies this is to be expected.
	if len(out) < num {
		c.log.Info("identified portion of requested nodes for removal",
			"requested", num, "identified", len(out))
	}
	return out, nil
}

// RunPostScaleInTasks triggers any tasks which should occur after the nodes
// have been terminated within the remote provider.
//
// The context is currently ignored on purpose, pending investigation into
// plugging this into the Nomad API query meta.
func (c *ClusterScaleUtils) RunPostScaleInTasks(_ context.Context, cfg map[string]string, ids []NodeResourceID) error {

	// Attempt to read of the node purge config parameter. If it has been set
	// then check its value, otherwise the default stance is that node purging
	// is disabled.
	if val, ok := cfg[sdk.TargetConfigKeyNodePurge]; ok {

		// Parse the string as a bool. If we get an error return this as the
		// operator has attempted to configure this value, but it's not worth
		// breaking the whole pipeline for. Therefore log the error and return
		// as Nomad will eventually perform this work.
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			c.log.Error("failed to parse node_purge config param", "error", err)
			return nil
		}

		// If the operator has disabled node purging, exit.
		if !boolVal {
			return nil
		}
	} else {
		return nil
	}

	// Use a multierror to collect errors from any and all node purge calls
	// that fail.
	var mErr *multierror.Error

	// Iterate the node list and perform a purge on each node. In the event of
	// an error, add this to the list. Otherwise log useful information.
	for _, node := range ids {

		resp, _, err := c.client.Nodes().Purge(node.NomadNodeID, nil)
		if err != nil {
			mErr = multierror.Append(mErr, err)
		} else {
			c.log.Info("successfully purged Nomad node", "node_id", node.NomadNodeID, "nomad_evals", resp.EvalIDs)
		}
	}

	return errHelper.FormattedMultiError(mErr)
}

// IsPoolReady provides a method for understanding whether the node pool is in
// a state that allows it to be safely scaled. This should be used by target
// plugins when providing their status response. A non-nil error indicates
// there was a problem performing the check.
func (c *ClusterScaleUtils) IsPoolReady(cfg map[string]string) (bool, error) {

	ids, err := nodepool.NewClusterNodePoolIdentifierList(cfg)
	if err != nil {
		return false, err
	}

	nodes, _, err := c.client.Nodes().List(nil)
	if err != nil {
		return false, fmt.Errorf("failed to list Nomad nodes: %v", err)
	}

	filterOpts, err := NewNodeFilterOptions(cfg)
	if err != nil {
		return false, err
	}
	if _, err := FilterNodesWithOptions(nodes, ids.IsPoolMember, filterOpts); err != nil {
		c.log.Warn("node pool status readiness check failed", "error", err)
		return false, nil
	}
	return true, nil
}

// autoscalerNodeID identifies the NodeID which the Nomad Autoscaler is running
// on so that it can be protected from scaling in actions.
func autoscalerNodeID(client *api.Client) (string, error) {

	envVar := os.Getenv("NOMAD_ALLOC_ID")
	if envVar == "" {
		return "", nil
	}

	allocInfo, _, err := client.Allocations().Info(envVar, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call Nomad allocation info: %v", err)
	}
	return allocInfo.NodeID, nil
}
