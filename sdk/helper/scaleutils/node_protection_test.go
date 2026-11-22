// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package scaleutils

import (
	"testing"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/hashicorp/nomad/api"
	"github.com/shoenig/test/must"
)

func testAlloc(id, jobID, clientStatus string) *api.Allocation {
	return &api.Allocation{
		ID:            id,
		JobID:         jobID,
		DesiredStatus: api.AllocDesiredStatusRun,
		ClientStatus:  clientStatus,
	}
}

func testSystemAlloc(id, jobID string) *api.Allocation {
	alloc := testAlloc(id, jobID, api.AllocClientStatusRunning)
	alloc.Job = &api.Job{Type: &[]string{api.JobTypeSystem}[0]}
	return alloc
}

func TestNewAllocProtection_defaults(t *testing.T) {
	p, err := newAllocProtection(map[string]string{})
	must.NoError(t, err)
	must.False(t, p.enabled)
	must.False(t, p.systemJobs)
	must.True(t, p.migrating)
	must.False(t, p.allNonTerminal)
	must.MapEmpty(t, p.jobs)
}

func TestNewAllocProtection_parsing(t *testing.T) {
	p, err := newAllocProtection(map[string]string{
		sdk.TargetConfigKeyNodeProtectAllocations: "true",
		sdk.TargetConfigKeyNodeProtectSystemJobs:  "true",
		sdk.TargetConfigKeyNodeProtectMigrating:   "false",
		sdk.TargetConfigKeyNodeProtectAll:         "true",
		sdk.TargetConfigKeyNodeProtectJobs:        "db, cache ,,worker",
	})
	must.NoError(t, err)
	must.True(t, p.enabled)
	must.True(t, p.systemJobs)
	must.False(t, p.migrating)
	must.True(t, p.allNonTerminal)
	must.MapLen(t, 3, p.jobs)
	must.MapContainsKey(t, p.jobs, "db")
	must.MapContainsKey(t, p.jobs, "cache")
	must.MapContainsKey(t, p.jobs, "worker")
}

func TestNewAllocProtection_parseErrors(t *testing.T) {
	for _, key := range []string{
		sdk.TargetConfigKeyNodeProtectAllocations,
		sdk.TargetConfigKeyNodeProtectSystemJobs,
		sdk.TargetConfigKeyNodeProtectMigrating,
		sdk.TargetConfigKeyNodeProtectAll,
	} {
		_, err := newAllocProtection(map[string]string{key: "maybe"})
		must.Error(t, err, must.Sprintf("expected error for %s", key))
	}
}

func TestAllocProtection_allocProtectionReason(t *testing.T) {
	migrating := testAlloc("alloc-m", "web", api.AllocClientStatusRunning)
	migrate := true
	migrating.DesiredTransition.Migrate = &migrate

	testCases := []struct {
		name           string
		protection     *allocProtection
		alloc          *api.Allocation
		expectedReason string
		expected       bool
	}{
		{
			name:       "terminal client status never protects",
			protection: &allocProtection{enabled: true, allNonTerminal: true},
			alloc:      testAlloc("alloc-1", "web", api.AllocClientStatusComplete),
			expected:   false,
		},
		{
			name:       "failed client status never protects",
			protection: &allocProtection{enabled: true, allNonTerminal: true},
			alloc:      testAlloc("alloc-1", "web", api.AllocClientStatusFailed),
			expected:   false,
		},
		{
			name:       "server terminal status never protects",
			protection: &allocProtection{enabled: true, allNonTerminal: true},
			alloc: func() *api.Allocation {
				a := testAlloc("alloc-1", "web", api.AllocClientStatusRunning)
				a.DesiredStatus = api.AllocDesiredStatusStop
				return a
			}(),
			expected: false,
		},
		{
			name:           "protected job",
			protection:     &allocProtection{enabled: true, jobs: map[string]struct{}{"db": {}}},
			alloc:          testAlloc("alloc-1", "db", api.AllocClientStatusRunning),
			expectedReason: NodeProtectReasonProtectedJob,
			expected:       true,
		},
		{
			name:           "pending protected job alloc protects",
			protection:     &allocProtection{enabled: true, jobs: map[string]struct{}{"db": {}}},
			alloc:          testAlloc("alloc-1", "db", api.AllocClientStatusPending),
			expectedReason: NodeProtectReasonProtectedJob,
			expected:       true,
		},
		{
			name:           "migrating alloc",
			protection:     &allocProtection{enabled: true, migrating: true},
			alloc:          migrating,
			expectedReason: NodeProtectReasonMigrating,
			expected:       true,
		},
		{
			name:       "migrating alloc ignored when disabled",
			protection: &allocProtection{enabled: true, migrating: false},
			alloc:      migrating,
			expected:   false,
		},
		{
			name:           "system job",
			protection:     &allocProtection{enabled: true, systemJobs: true},
			alloc:          testSystemAlloc("alloc-1", "agent"),
			expectedReason: NodeProtectReasonSystemJob,
			expected:       true,
		},
		{
			name:       "system job ignored when disabled",
			protection: &allocProtection{enabled: true, systemJobs: false},
			alloc:      testSystemAlloc("alloc-1", "agent"),
			expected:   false,
		},
		{
			name:           "any non-terminal alloc",
			protection:     &allocProtection{enabled: true, allNonTerminal: true},
			alloc:          testAlloc("alloc-1", "web", api.AllocClientStatusRunning),
			expectedReason: NodeProtectReasonNonTerminal,
			expected:       true,
		},
		{
			name:       "running service alloc not protected by default rules",
			protection: &allocProtection{enabled: true, migrating: true},
			alloc:      testAlloc("alloc-1", "web", api.AllocClientStatusRunning),
			expected:   false,
		},
		{
			name:       "nil alloc",
			protection: &allocProtection{enabled: true, allNonTerminal: true},
			alloc:      nil,
			expected:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reason, protected := tc.protection.allocProtectionReason(tc.alloc)
			must.Eq(t, tc.expected, protected)
			must.Eq(t, tc.expectedReason, reason)
		})
	}
}

func TestScaleInPolicies_parsing(t *testing.T) {

	// Defaults.
	p, err := scaleInShortfallPolicy(map[string]string{})
	must.NoError(t, err)
	must.Eq(t, ScaleInShortfallPartial, p)

	p, err = scaleInPartialFailurePolicy(map[string]string{})
	must.NoError(t, err)
	must.Eq(t, ScaleInPartialFailureTerminateSuccesses, p)

	// Explicit values.
	p, err = scaleInShortfallPolicy(map[string]string{sdk.TargetConfigKeyScaleInShortfall: "fail"})
	must.NoError(t, err)
	must.Eq(t, ScaleInShortfallFail, p)

	p, err = scaleInPartialFailurePolicy(map[string]string{sdk.TargetConfigKeyScaleInPartialFailure: "abort"})
	must.NoError(t, err)
	must.Eq(t, ScaleInPartialFailureAbort, p)

	// Invalid values are terminal errors.
	_, err = scaleInShortfallPolicy(map[string]string{sdk.TargetConfigKeyScaleInShortfall: "whatever"})
	must.Error(t, err)

	_, err = scaleInPartialFailurePolicy(map[string]string{sdk.TargetConfigKeyScaleInPartialFailure: "whatever"})
	must.Error(t, err)
}
