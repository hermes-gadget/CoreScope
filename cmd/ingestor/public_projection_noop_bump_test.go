package main

import "testing"

func TestCorePolicyFingerprintIsOrderIndependentAndRuleSensitive(t *testing.T) {
	a := []corePrivatePrefix{
		{NodeID: "aa", Network: "ukmesh", Size: 1, Prefix: "1"},
		{NodeID: "bb", Network: "teesside", Size: 2, Prefix: "2f"},
	}
	// Same rules, different order: must hash identically.
	b := []corePrivatePrefix{a[1], a[0]}
	if corePolicyFingerprint(a) != corePolicyFingerprint(b) {
		t.Fatal("fingerprint depends on prefix ordering")
	}
	// Case and UK-family aliases normalise like index() does.
	c := []corePrivatePrefix{
		{NodeID: "AA", Network: "ukmesh", Size: 1, Prefix: "1"},
		{NodeID: "bb", Network: "northeast", Size: 2, Prefix: "2F"},
	}
	if corePolicyFingerprint(a) != corePolicyFingerprint(c) {
		t.Fatal("fingerprint is case or network-family sensitive")
	}
	// A real rule change must alter the digest.
	d := []corePrivatePrefix{a[0]}
	if corePolicyFingerprint(a) == corePolicyFingerprint(d) {
		t.Fatal("removing a private prefix did not change the fingerprint")
	}
}

func TestDecideProjectionAction(t *testing.T) {
	policy := &corePublicPolicy{generation: 838, fingerprint: "fp1"}
	cases := []struct {
		name        string
		generation  int64
		ready       int
		fingerprint string
		want        projectionDecision
	}{
		{"same generation and ready", 838, 1, "fp1", projectionReady},
		{"same generation not ready", 838, 0, "fp1", projectionRebuild},
		{"counter-only bump", 837, 1, "fp1", projectionAdoptGeneration},
		{"real policy change", 837, 1, "fp0", projectionRebuild},
		{"unknown stored fingerprint", 837, 1, "", projectionRebuild},
		{"new generation while not ready", 837, 0, "fp1", projectionRebuild},
	}
	for _, tc := range cases {
		if got := decideProjectionAction(tc.generation, tc.ready, tc.fingerprint, policy); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}
