// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package scaleutils

import (
	"fmt"
	"strconv"
	"strings"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	errHelper "github.com/hashicorp/nomad-autoscaler/sdk/helper/error"
	"github.com/hashicorp/nomad/api"
)

// Stable reason strings used in NodeDrainResult.Reason. These are part of the
// result contract exposed to target plugins and operators, so they must
// remain stable across releases.
const (
	// NodeProtectReasonProtectedJob indicates the node runs a non-terminal
	// allocation of a job listed in node_protect_jobs.
	NodeProtectReasonProtectedJob = "protected_job"

	// NodeProtectReasonSystemJob indicates the node runs a non-terminal
	// system job allocation and node_protect_system_jobs is enabled.
	NodeProtectReasonSystemJob = "system_job"

	// NodeProtectReasonMigrating indicates the node runs an allocation that
	// the Nomad scheduler has marked for migration and
	// node_protect_migrating is enabled.
	NodeProtectReasonMigrating = "migrating_alloc"

	// NodeProtectReasonNonTerminal indicates the node runs a non-terminal
	// allocation and node_protect_all_allocations is enabled.
	NodeProtectReasonNonTerminal = "non_terminal_alloc"

	// NodeProtectReasonAllocationsUnreadable indicates the allocation
	// snapshot for the node could not be read. The node is refused rather
	// than treated as empty.
	NodeProtectReasonAllocationsUnreadable = "allocations_unreadable"
)

// Scale-in shortfall policy values for sdk.TargetConfigKeyScaleInShortfall.
const (
	// ScaleInShortfallPartial scales in fewer nodes than requested when
	// protection filtering reduces the candidate set. This is the default
	// and matches the historical "adjust and warn" behaviour.
	ScaleInShortfallPartial = "partial"

	// ScaleInShortfallFail aborts the whole scale in action with an error
	// when protection filtering reduces the candidate set below the
	// requested count.
	ScaleInShortfallFail = "fail"
)

// Scale-in partial failure policy values for
// sdk.TargetConfigKeyScaleInPartialFailure.
const (
	// ScaleInPartialFailureTerminateSuccesses terminates the subset of nodes
	// that drained successfully when other nodes fail. This is the default.
	ScaleInPartialFailureTerminateSuccesses = "terminate_successes"

	// ScaleInPartialFailureAbort terminates nothing and returns an error
	// when any selected node fails to drain.
	ScaleInPartialFailureAbort = "abort"
)

// allocProtection holds the parsed allocation protection configuration. All
// rules are evaluated against a snapshot of the node's allocations taken
// from the Nomad API; the snapshot is re-read before termination is
// authorised because allocations can change between candidate selection and
// the end of the drain.
type allocProtection struct {

	// enabled is the master switch. When false, no protection evaluation is
	// performed and candidate behaviour is identical to previous releases.
	enabled bool

	// systemJobs marks non-terminal system job allocations as protecting.
	systemJobs bool

	// migrating marks allocations with DesiredTransition.Migrate set as
	// protecting.
	migrating bool

	// allNonTerminal marks any non-terminal allocation as protecting.
	allNonTerminal bool

	// jobs is the set of job IDs whose non-terminal allocations always
	// protect the node they run on.
	jobs map[string]struct{}
}

// newAllocProtection parses the allocation protection configuration. Values
// that are present but malformed are a terminal error so operators do not
// run with a silently different protection posture than configured.
func newAllocProtection(cfg map[string]string) (*allocProtection, error) {

	p := &allocProtection{
		systemJobs: false,
		migrating:  true,
		jobs:       make(map[string]struct{}),
	}

	var mErr *multierror.Error

	if str, ok := cfg[sdk.TargetConfigKeyNodeProtectAllocations]; ok {
		enabled, err := strconv.ParseBool(str)
		if err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf(
				"failed to parse value %s for %s configuration: %v",
				str, sdk.TargetConfigKeyNodeProtectAllocations, err))
		} else {
			p.enabled = enabled
		}
	}

	if str, ok := cfg[sdk.TargetConfigKeyNodeProtectSystemJobs]; ok {
		v, err := strconv.ParseBool(str)
		if err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf(
				"failed to parse value %s for %s configuration: %v",
				str, sdk.TargetConfigKeyNodeProtectSystemJobs, err))
		} else {
			p.systemJobs = v
		}
	}

	if str, ok := cfg[sdk.TargetConfigKeyNodeProtectMigrating]; ok {
		v, err := strconv.ParseBool(str)
		if err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf(
				"failed to parse value %s for %s configuration: %v",
				str, sdk.TargetConfigKeyNodeProtectMigrating, err))
		} else {
			p.migrating = v
		}
	}

	if str, ok := cfg[sdk.TargetConfigKeyNodeProtectAll]; ok {
		v, err := strconv.ParseBool(str)
		if err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf(
				"failed to parse value %s for %s configuration: %v",
				str, sdk.TargetConfigKeyNodeProtectAll, err))
		} else {
			p.allNonTerminal = v
		}
	}

	if str, ok := cfg[sdk.TargetConfigKeyNodeProtectJobs]; ok {
		for _, jobID := range strings.Split(str, ",") {
			jobID = strings.TrimSpace(jobID)
			if jobID != "" {
				p.jobs[jobID] = struct{}{}
			}
		}
	}

	if mErr != nil {
		return nil, errHelper.FormattedMultiError(mErr)
	}
	return p, nil
}

// allocProtectionReason evaluates a single allocation against the protection
// rules. It returns a stable reason string and true when the allocation
// protects its node from scale in.
//
// Terminal allocations (client status complete/failed/lost, or desired
// status stop/evict) never protect a node: they are already finishing or
// finished and the drain lifecycle handles them.
func (p *allocProtection) allocProtectionReason(alloc *api.Allocation) (string, bool) {

	if alloc == nil {
		return "", false
	}

	if alloc.ClientTerminalStatus() || alloc.ServerTerminalStatus() {
		return "", false
	}

	if _, ok := p.jobs[alloc.JobID]; ok {
		return NodeProtectReasonProtectedJob, true
	}

	if p.migrating && alloc.DesiredTransition.Migrate != nil && *alloc.DesiredTransition.Migrate {
		return NodeProtectReasonMigrating, true
	}

	if p.systemJobs && alloc.Job != nil && alloc.Job.Type != nil && *alloc.Job.Type == api.JobTypeSystem {
		return NodeProtectReasonSystemJob, true
	}

	if p.allNonTerminal {
		return NodeProtectReasonNonTerminal, true
	}

	return "", false
}

// nodeProtectionReason evaluates a node's allocation snapshot, returning the
// reason of the first protecting allocation. The allocation list order is
// not guaranteed by the API, so when multiple rules could match the returned
// reason is the first in slice order; all matching reasons are equally valid
// refusals.
func (p *allocProtection) nodeProtectionReason(allocs []*api.Allocation) (string, bool) {
	for _, alloc := range allocs {
		if reason, ok := p.allocProtectionReason(alloc); ok {
			return reason, true
		}
	}
	return "", false
}

// scaleInShortfallPolicy parses the configured behaviour for when protection
// filtering leaves fewer candidates than requested. The default is partial,
// which mirrors the historical adjust-and-warn behaviour.
func scaleInShortfallPolicy(cfg map[string]string) (string, error) {
	str, ok := cfg[sdk.TargetConfigKeyScaleInShortfall]
	if !ok {
		return ScaleInShortfallPartial, nil
	}
	switch str {
	case ScaleInShortfallPartial, ScaleInShortfallFail:
		return str, nil
	default:
		return "", fmt.Errorf("invalid value %q for %s configuration, must be %q or %q",
			str, sdk.TargetConfigKeyScaleInShortfall, ScaleInShortfallPartial, ScaleInShortfallFail)
	}
}

// scaleInPartialFailurePolicy parses the configured behaviour for when some
// selected nodes fail to drain. The default is terminate_successes.
func scaleInPartialFailurePolicy(cfg map[string]string) (string, error) {
	str, ok := cfg[sdk.TargetConfigKeyScaleInPartialFailure]
	if !ok {
		return ScaleInPartialFailureTerminateSuccesses, nil
	}
	switch str {
	case ScaleInPartialFailureTerminateSuccesses, ScaleInPartialFailureAbort:
		return str, nil
	default:
		return "", fmt.Errorf("invalid value %q for %s configuration, must be %q or %q",
			str, sdk.TargetConfigKeyScaleInPartialFailure,
			ScaleInPartialFailureTerminateSuccesses, ScaleInPartialFailureAbort)
	}
}
