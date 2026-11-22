# Scale-in allocation protection and per-node drain results

This document describes the new pre-scale-in guarantees added to
`sdk/helper/scaleutils`: allocation-based node protection, per-node drain
results, and the termination authorisation boundary between the helper and
the target plugins.

## What changed and why

Previously, `RunPreScaleInTasks` and `RunPreScaleInTasksWithRemoteCheck`
returned the full candidate list once `DrainNodes` succeeded, and returned
only an aggregated error when any node failed. The candidate list was, in
effect, used as cloud deletion authorisation, and a single failed node
abandoned the whole action with no way to tell which nodes were safe.

The helper now:

1. Optionally refuses nodes **before any drain** based on a snapshot of
   their current allocations (`node_protect_allocations` and friends).
2. Produces a **per-node result** (`NodeDrainResult`) that distinguishes
   protected-not-drained, drained-and-tasks-dead, timeout, cancellation, API
   failure and remote-mapping failure.
3. Returns an explicit **termination set** (`ScaleInResult.Terminate`) that
   contains only nodes confirmed safe in this round. Target plugins
   terminate exactly this set — never the candidate list.

## Protection configuration and defaults

All keys live in `sdk/target.go` and are parsed from the target
configuration map. Unrecognised or malformed values are terminal errors, so
operators never run with a silently different protection posture.

| Key | Default | Meaning |
| --- | --- | --- |
| `node_protect_allocations` | `false` | Master switch. When unset/false, candidate behaviour is identical to previous releases. |
| `node_protect_system_jobs` | `false` | Non-terminal system job allocations protect the node. Default off: system jobs run on every node, so enabling this would block all scale-in. |
| `node_protect_migrating` | `true` | Allocations the scheduler marked for migration (`Allocation.DesiredTransition.Migrate == true`) protect the node. |
| `node_protect_all_allocations` | `false` | Any non-terminal allocation protects the node (most conservative). |
| `node_protect_jobs` | `""` | Comma-separated job IDs whose non-terminal allocations always protect the node. |
| `node_scale_in_shortfall` | `partial` | `partial`: scale in fewer nodes when protection shrinks the candidate set (historical adjust-and-warn). `fail`: abort the whole action with an error. |
| `node_scale_in_partial_failure` | `terminate_successes` | `terminate_successes`: terminate the confirmed subset when other nodes fail. `abort`: terminate nothing and return an error. |

### How allocations are judged

The rules are evaluated against `api.Allocation` values from
`Nodes().Allocations`:

- **Terminal allocations never protect.** An allocation is terminal when
  `ClientTerminalStatus()` (client status `complete`, `failed` or `lost`) or
  `ServerTerminalStatus()` (desired status `stop` or `evict`) is true. These
  helpers already exist on the Nomad API type.
- **System tasks** are identified by `alloc.Job.Type == api.JobTypeSystem`
  (nil-safe). Only protecting when `node_protect_system_jobs = true`.
- **Migrating tasks** are identified by
  `alloc.DesiredTransition.Migrate != nil && *Migrate`. Protecting by
  default once protection is enabled.
- **User-specified tasks** are matched on `alloc.JobID` against
  `node_protect_jobs`.

These are new configuration surface area; no pre-existing node or
allocation metadata marker is assumed.

### Failure-secure reading

A node whose allocations cannot be read is **refused**, not treated as
empty. The same applies to nodes whose remote ID mapping fails or conflicts
(two nodes claiming the same remote ID): they are excluded with an explicit
`mapping_failed` result and are never drained or terminated.

## Drain behaviour (unchanged) and `IgnoreSystemJobs`

`defaultNodeDrainDeadline = 15 * time.Minute` and
`defaultNodeIgnoreSystemJobs = false` are unchanged, as is their config
parsing (`node_drain_deadline`, `node_drain_ignore_system_jobs`). Each drain
still sets `MarkEligible=false` and `drained_by=nomad-autoscaler` via
`UpdateDrainOpts`, then runs `monitorNodeDrain`, then
`waitForAllTasksDead`.

`node_drain_ignore_system_jobs` only affects the **drain monitor**: with
`true`, Nomad reports drain completion without stopping system
allocations. It does **not** affect `waitForAllTasksDead`, which waits for
every task on the node to be `dead`. Consequence: with
`ignore_system_jobs = true`, system tasks keep running after drain
completion and `waitForAllTasksDead` will block on them until the context
ends. Operators combining the two should be aware the scale-in will only
proceed once those tasks actually exit (or the evaluation context times
out). Drain completion is never used as a substitute for all tasks being
dead.

## Per-node result interface

```go
type NodeDrainResult struct {
    NodeResourceID            // NomadNodeID and, when mapped, RemoteResourceID
    Status NodeDrainStatus    // drained | protected | timeout | cancelled | failed | mapping_failed
    Stage  NodeDrainStage     // remote_mapping | protection | drain | wait_tasks | pre_terminate
    Reason string             // stable machine-readable reason
    Err    error              // underlying error, for logging
}
```

- Results are returned in candidate order, independent of goroutine
  completion order.
- Only `Status == drained` nodes enter `ScaleInResult.Terminate`. Failed,
  unknown, cancelled and protected nodes never do.
- The termination set is deduplicated by remote ID; a remote ID is
  terminated at most once, and never outside the target provider group
  (the existing remote-check filter is preserved).
- When the request context is cancelled, no new drains are started; nodes
  not yet started are recorded as `cancelled`/`timeout` with reason
  `context_done_before_drain_start`, and in-flight drains are recorded with
  their actual outcome. Unreaped goroutines are never reported as success.

## Candidate shortfall and partial success

- **Shortfall** (protection leaves fewer candidates than requested):
  default `partial` scales in fewer nodes with a warning — matching the
  historical adjust-and-warn behaviour. `fail` aborts with an error.
  Protection is never skipped to make up the count, and the autoscaler's
  own node is never a candidate (`filterOutNodeID` is preserved).
- **Partial drain failure**: default `terminate_successes` authorises the
  confirmed subset; `abort` authorises nothing and returns an error. This
  is an explicit, configured policy — the number of instances a target
  plugin terminates always equals `len(ScaleInResult.Terminate)` and the
  IDs are exactly that set.

## Race window and retry semantics

Protection is checked twice: before the first drain and again after each
successful drain, before termination is authorised. This narrows but
**cannot eliminate** the race window between the allocation snapshot and
the cloud termination call — there is no atomic commit across the Nomad
scheduler and a cloud API. A node that becomes protected after its drain is
refused at the `pre_terminate` stage.

For nodes that drained successfully but whose cloud termination failed:

- A retry recognises the existing drain state (`drained_by=nomad-autoscaler`
  meta, `MarkEligible=false` already applied) rather than assuming the
  instance is gone or re-draining blindly.
- The helper does **not** automatically restore scheduling eligibility.
  Re-enabling eligibility remains an explicit caller decision (the existing
  `RunPostScaleInTasksOnFailure`, used by the AWS plugin, is that explicit
  path and is unchanged).
- Retries deduplicate by remote ID and never target instances outside the
  provider group.

## Compatibility

- `RunPreScaleInTasks` (COMPAT 0.4) and `IdentifyScaleInNodes`,
  `IdentifyScaleInRemoteIDs`, `SelectScaleInNodes`, `DrainNodes` keep their
  signatures. `DrainNodes` is now implemented on top of
  `DrainNodesDetailed` and still returns only an aggregated error.
- `RunPreScaleInTasksWithRemoteCheck` keeps its signature and historical
  contract: any failed/timed-out/cancelled node fails the whole action and
  nothing is returned. It is now a wrapper over the detailed method.
- New: `RunPreScaleInTasksWithRemoteCheckDetailed` returns `*ScaleInResult`
  with the confirmed termination set and per-node results.
- With protection unconfigured (the default), candidate selection,
  filtering (`node_filter_ignore_*`, including `node_filter_ignore_ineligible`
  defaulting to `true` so ineligible nodes remain candidates), self-node
  exclusion, and selector ordering are all unchanged.

## Logging and authorisation boundary

- Every node excluded from termination is logged with node ID, remote ID,
  stage, status and stable reason; the three target plugins additionally
  log each excluded node before terminating.
- The authorisation boundary is `ScaleInResult.Terminate`: plugins must
  terminate exactly this set. The candidate list, the remote ID list and
  the drain list are never deletion authorisation.

## Target plugin integration

Confirmed against each in-repo consumer of the helper:

- **aws-asg**: uses `RunPreScaleInTasksWithRemoteCheckDetailed`, terminates
  only `Terminate` via `TerminateInstanceInAutoScalingGroup`, and returns a
  no-op when the confirmed set is empty. Existing partial-termination
  handling (`RunPostScaleInTasksOnFailure`, activity tracking) is retained.
- **gce-mig**: same contract; `deleteInstances` receives only the confirmed
  remote IDs; empty confirmed set is a no-op.
- **azure-vmss**: same contract; `BeginDeleteInstances` receives only the
  confirmed instance IDs; empty confirmed set is a no-op.
- **ibmcloud-ig** and the app-scaling **nomad** target do not use this
  helper and are unaffected.

Out of scope, as agreed: policy hot-reload, check arbitration, and policy
`Count` bounds are unchanged.

## Tests

`sdk/helper/scaleutils` tests use in-memory substitutes only — a fake Nomad
API (nodes, node info, node allocations with scripted sequences and failure
statuses), a fake drainer with per-node behaviour, and controllable
contexts. No real Nomad cluster, no real drains, no cloud instances.
Covered: no allocations, protected allocations, terminal allocations,
allocation read failure, autoscaler self-exclusion, ineligible nodes
remaining candidates, out-of-group remote filtering, partial drain failure
(both policies), drain timeout, context cancelled before start, tasks not
dead after drain, remote ID mapping conflict, pre-terminate re-check,
shortfall policies, deterministic result ordering, and the compatibility
wrapper contract. Assertions verify the provider-facing termination set
contains only confirmed nodes.
