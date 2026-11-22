// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package scaleutils

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/hashicorp/nomad-autoscaler/sdk/helper/blocking"
	errHelper "github.com/hashicorp/nomad-autoscaler/sdk/helper/error"
	"github.com/hashicorp/nomad/api"
)

const (
	defaultNodeDrainDeadline    = 15 * time.Minute
	defaultNodeIgnoreSystemJobs = false

	nodeDrainedMetaKey   = "drained_by"
	nodeDrainedMetaValue = "nomad-autoscaler"
)

// NodeDrainStatus identifies the terminal outcome of a single node within a
// scale in attempt. It is part of the result contract exposed to target
// plugins and must remain stable.
type NodeDrainStatus string

const (
	// NodeDrainStatusDrained indicates the node drain completed and all
	// tasks on the node are dead. Only nodes with this status may enter the
	// termination set.
	NodeDrainStatusDrained NodeDrainStatus = "drained"

	// NodeDrainStatusProtected indicates the node was refused before any
	// drain was triggered because allocation protection rules matched.
	NodeDrainStatusProtected NodeDrainStatus = "protected"

	// NodeDrainStatusTimeout indicates the node did not reach a terminal
	// drain state before the context deadline.
	NodeDrainStatusTimeout NodeDrainStatus = "timeout"

	// NodeDrainStatusCancelled indicates the drain was cancelled, either
	// because the caller context was cancelled or because the context was
	// already done before the drain for this node started.
	NodeDrainStatusCancelled NodeDrainStatus = "cancelled"

	// NodeDrainStatusFailed indicates a Nomad API or drain monitoring
	// failure that is neither a timeout nor a cancellation.
	NodeDrainStatusFailed NodeDrainStatus = "failed"

	// NodeDrainStatusMappingFailed indicates the node could not be mapped
	// to a remote provider ID, or the mapping conflicted with another node.
	NodeDrainStatusMappingFailed NodeDrainStatus = "mapping_failed"
)

// NodeDrainStage identifies the phase a node reached within a scale in
// attempt. It is part of the result contract exposed to target plugins and
// must remain stable.
type NodeDrainStage string

const (
	// NodeDrainStageRemoteMapping is the stage where Nomad nodes are
	// translated to remote provider IDs.
	NodeDrainStageRemoteMapping NodeDrainStage = "remote_mapping"

	// NodeDrainStageProtection is the stage where allocation protection
	// rules are evaluated before any drain is triggered.
	NodeDrainStageProtection NodeDrainStage = "protection"

	// NodeDrainStageDrain is the stage where the node drain is triggered
	// and monitored.
	NodeDrainStageDrain NodeDrainStage = "drain"

	// NodeDrainStageWaitTasks is the stage where the autoscaler waits for
	// all tasks on the node to be dead after the drain completed.
	NodeDrainStageWaitTasks NodeDrainStage = "wait_tasks"

	// NodeDrainStagePreTerminate is the stage where protection rules are
	// re-evaluated after a successful drain, before the node is authorised
	// for termination.
	NodeDrainStagePreTerminate NodeDrainStage = "pre_terminate"
)

// Stable reason strings used in NodeDrainResult.Reason for drain stage
// outcomes. Allocation protection reasons live in node_protection.go.
const (
	NodeDrainReasonDrained              = "drain_complete_tasks_dead"
	NodeDrainReasonDrainAPIError        = "drain_api_error"
	NodeDrainReasonDrainTimeout         = "drain_timeout"
	NodeDrainReasonDrainCancelled       = "drain_cancelled"
	NodeDrainReasonNotStarted           = "context_done_before_drain_start"
	NodeDrainReasonNodeInfoUnreadable   = "node_info_unreadable"
	NodeDrainReasonRemoteIDLookupFailed = "remote_id_lookup_failed"
	NodeDrainReasonRemoteIDConflict     = "remote_id_conflict"
)

// NodeDrainResult describes the outcome of a single node within a scale in
// attempt. Nodes that did not reach NodeDrainStatusDrained must never be
// terminated in the remote provider.
type NodeDrainResult struct {

	// NodeResourceID identifies the Nomad node and, when mapping succeeded,
	// its remote provider ID.
	NodeResourceID

	// Status is the terminal outcome for the node.
	Status NodeDrainStatus

	// Stage is the phase the node reached.
	Stage NodeDrainStage

	// Reason is a stable machine-readable reason string.
	Reason string

	// Err is the underlying error, if any. It is not serialized and is
	// intended for logging.
	Err error
}

// terminatable returns whether the node is confirmed safe to terminate in
// the remote provider.
func (r NodeDrainResult) terminatable() bool {
	return r.Status == NodeDrainStatusDrained
}

// failed returns whether the node represents a failure, as opposed to an
// intentional refusal (protected) or a success.
func (r NodeDrainResult) failed() bool {
	switch r.Status {
	case NodeDrainStatusTimeout, NodeDrainStatusCancelled,
		NodeDrainStatusFailed, NodeDrainStatusMappingFailed:
		return true
	default:
		return false
	}
}

// DrainNodes iterates the provided nodeID list and performs a drain on each
// one. Each node drain is monitored and events logged until the context is
// closed or all drains reach a terminal state.
//
// The returned error is an aggregation of the per-node errors; callers that
// need to distinguish which nodes drained successfully should use
// DrainNodesDetailed.
func (c *ClusterScaleUtils) DrainNodes(ctx context.Context, cfg map[string]string, nodes []NodeResourceID) error {

	results, err := c.DrainNodesDetailed(ctx, cfg, nodes)
	if err != nil {
		return err
	}

	var mErr *multierror.Error
	for _, r := range results {
		if r.Err != nil {
			mErr = multierror.Append(mErr, r.Err)
		}
	}
	return errHelper.FormattedMultiError(mErr)
}

// DrainNodesDetailed performs a drain on each of the provided nodes and
// returns one result per node, in the same order as the input list. The
// ordering therefore does not depend on goroutine completion order.
//
// Once the context is done, no new drains are started; nodes that were not
// yet started are recorded as cancelled or timed out, and nodes already in
// flight are recorded with their actual outcome.
func (c *ClusterScaleUtils) DrainNodesDetailed(ctx context.Context, cfg map[string]string, nodes []NodeResourceID) ([]NodeDrainResult, error) {

	drainSpec, err := drainSpec(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to generate node drainspec: %v", err)
	}

	// Pre-populate the results so the output order matches the input order
	// regardless of the order in which the drain routines finish.
	results := make([]NodeDrainResult, len(nodes))

	var wg sync.WaitGroup

	for i, node := range nodes {

		results[i].NodeResourceID = node

		// Do not start new drains once the context is done. Record the
		// refusal honestly rather than launching a routine that would exit
		// immediately with an ambiguous outcome.
		if ctxErr := ctx.Err(); ctxErr != nil {
			results[i].Stage = NodeDrainStageDrain
			results[i].Reason = NodeDrainReasonNotStarted
			results[i].Err = ctxErr
			results[i].Status = drainStatusForContextErr(ctxErr)
			continue
		}

		wg.Add(1)

		// Launch a routine to drain the node, recording the outcome in the
		// node's own result slot.
		go func(idx int, n NodeResourceID) {
			defer wg.Done()

			stage, err := c.drainNodeWithStage(ctx, n.NomadNodeID, drainSpec)
			if err == nil {
				results[idx].Status = NodeDrainStatusDrained
				results[idx].Stage = NodeDrainStageWaitTasks
				results[idx].Reason = NodeDrainReasonDrained
				c.log.Debug("node drain complete", "node_id", n.NomadNodeID)
				return
			}

			results[idx].Stage = stage
			results[idx].Err = err
			results[idx].Status, results[idx].Reason = classifyDrainError(err)
			c.log.Debug("node drain finished without success",
				"node_id", n.NomadNodeID, "stage", stage, "status", results[idx].Status, "error", err)
		}(i, node)
	}

	wg.Wait()

	return results, nil
}

// drainStatusForContextErr maps a context error to a drain status.
func drainStatusForContextErr(err error) NodeDrainStatus {
	if errors.Is(err, context.DeadlineExceeded) {
		return NodeDrainStatusTimeout
	}
	return NodeDrainStatusCancelled
}

// classifyDrainError maps a drain error to a stable status and reason.
// Context deadline errors are reported as timeouts, context cancellations as
// cancellations, and everything else as an API failure.
func classifyDrainError(err error) (NodeDrainStatus, string) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return NodeDrainStatusTimeout, NodeDrainReasonDrainTimeout
	case errors.Is(err, context.Canceled):
		return NodeDrainStatusCancelled, NodeDrainReasonDrainCancelled
	default:
		return NodeDrainStatusFailed, NodeDrainReasonDrainAPIError
	}
}

// drainSpec generates the Nomad API node drain specification based on the user
// configuration. Any options which have attempted to be configured, but are
// malformed are considered a terminal error. This allows operators to correct
// any mistakes and ensures we do not assume values.
func drainSpec(cfg map[string]string) (*api.DrainSpec, error) {

	// The configuration options that define the drain spec are optional and
	// have sensible defaults.
	deadline := defaultNodeDrainDeadline
	ignoreSystemJobs := defaultNodeIgnoreSystemJobs

	// Use a multierror so we can report all errors in a single call. This
	// allows for faster resolution and a nicer UX.
	var mErr *multierror.Error

	// Attempt to read the operator defined deadline from the config.
	if drainString, ok := cfg[sdk.TargetConfigKeyDrainDeadline]; ok {
		d, err := time.ParseDuration(drainString)
		if err != nil {
			mErr = multierror.Append(mErr, err)
		} else {
			deadline = d
		}
	}

	// Attempt to read the operator defined ignore system jobs from the config.
	if ignoreSystemJobsString, ok := cfg[sdk.TargetConfigKeyIgnoreSystemJobs]; ok {
		isj, err := strconv.ParseBool(ignoreSystemJobsString)
		if err != nil {
			mErr = multierror.Append(mErr, err)
		} else {
			ignoreSystemJobs = isj
		}
	}

	// Check whether we have found errors, and return these in a nicely
	// formatted way.
	if mErr != nil {
		return nil, errHelper.FormattedMultiError(mErr)
	}

	return &api.DrainSpec{
		Deadline:         deadline,
		IgnoreSystemJobs: ignoreSystemJobs,
	}, nil
}

// drainNode triggers a drain on the supplied ID using the DrainSpec. The
// function handles monitoring the drain and reporting its terminal status to
// the caller.
func (c *ClusterScaleUtils) drainNode(ctx context.Context, nodeID string, spec *api.DrainSpec) error {
	_, err := c.drainNodeWithStage(ctx, nodeID, spec)
	return err
}

// drainNodeWithStage is drainNode, additionally reporting the stage the
// drain reached when it failed.
func (c *ClusterScaleUtils) drainNodeWithStage(ctx context.Context, nodeID string, spec *api.DrainSpec) (NodeDrainStage, error) {

	c.log.Info("triggering drain on node", "node_id", nodeID, "deadline", spec.Deadline)

	opts := &api.DrainOptions{
		DrainSpec:    spec,
		MarkEligible: false,
		Meta: map[string]string{
			nodeDrainedMetaKey: nodeDrainedMetaValue,
		},
	}

	// Update the drain on the node.
	resp, err := c.drainer.UpdateDrainOpts(nodeID, opts, nil)
	if err != nil {
		return NodeDrainStageDrain, fmt.Errorf("failed to drain node: %w", err)
	}

	// Monitor the drain so we output the log messages. An error here indicates
	// the drain failed to complete successfully.
	if err := c.monitorNodeDrain(ctx, nodeID, resp.LastIndex, spec.IgnoreSystemJobs); err != nil {
		return NodeDrainStageDrain, fmt.Errorf("context done while monitoring node drain: %w", err)
	}

	// Drain completion is based on allocation status and does not guarantee
	// all tasks have finished. Wait until all tasks are dead before proceeding.
	if err := c.waitForAllTasksDead(ctx, nodeID); err != nil {
		return NodeDrainStageWaitTasks, fmt.Errorf("failed waiting for all tasks to be dead on node %s: %w", nodeID, err)
	}

	return NodeDrainStageWaitTasks, nil
}

// waitForAllTasksDead blocks until all tasks across all allocations on the
// node are dead, or the context is cancelled. This ensures that any remaining work
// on the node has completed before the caller proceeds with node termination
func (c *ClusterScaleUtils) waitForAllTasksDead(ctx context.Context, nodeID string) error {
	q := &api.QueryOptions{
		WaitIndex: 0,
	}

	for {
		allocs, meta, err := c.client.Nodes().Allocations(nodeID, q.WithContext(ctx))
		if err != nil {
			return fmt.Errorf("failed to list allocations for node: %w", err)
		}

		if !blocking.IndexHasChanged(meta.LastIndex, q.WaitIndex) {
			continue
		}

		allDead := true
	allocCheck:
		for _, alloc := range allocs {
			for taskName, taskState := range alloc.TaskStates {
				if taskState.State != "dead" {
					c.log.Info("waiting for task to finish before terminating node",
						"node_id", nodeID,
						"alloc_id", alloc.ID,
						"task", taskName,
						"state", taskState.State,
					)
					allDead = false
					break allocCheck
				}
			}
		}

		if allDead {
			return nil
		}

		q.WaitIndex = meta.LastIndex
	}
}

// monitorNodeDrain follows the drain of a node, logging the messages we
// receive to their appropriate level.
func (c *ClusterScaleUtils) monitorNodeDrain(ctx context.Context, nodeID string, index uint64, ignoreSys bool) error {
	for msg := range c.drainer.MonitorDrain(ctx, nodeID, index, ignoreSys) {

		switch msg.Level {
		case api.MonitorMsgLevelInfo:
			c.log.Info("received node drain message", "node_id", nodeID, "msg", msg.Message)
		case api.MonitorMsgLevelWarn:
			c.log.Warn("received node drain message", "node_id", nodeID, "msg", msg.Message)
		case api.MonitorMsgLevelError:
			return fmt.Errorf("received error while draining node: %s", msg.Message)
		default:
			c.log.Debug("received node drain message", "node_id", nodeID, "msg", msg.Message)
		}
	}
	return ctx.Err()
}

// RunPostScaleInTasksOnFailure is a utility that runs tasks on nodes which
// were unable to be successfully terminated. The current tasks are:
//
//   - modify node eligibility to true
func (c *ClusterScaleUtils) RunPostScaleInTasksOnFailure(nodes []NodeResourceID) error {

	var mErr *multierror.Error

	for _, node := range nodes {
		resp, err := c.client.Nodes().ToggleEligibility(node.NomadNodeID, true, nil)
		if err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf("failed to toggle eligibility on node %s: %v",
				node.NomadNodeID, err))
			continue
		}
		c.log.Info("successfully toggled node eligibility",
			"node_id", node.NomadNodeID, "evals", resp.EvalIDs)
	}

	return errHelper.FormattedMultiError(mErr)
}
