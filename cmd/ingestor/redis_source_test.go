package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDecodeRedisStreamValuePendingEntry(t *testing.T) {
	payload := `{"_summary":"advert","direction":"rx"}`
	value := `{"v":1,"k":"p","o":"f47ac10b-58cc-4372-a567-0e02b2c3d479","a":1700000000000,"ip":0,"vo":1,` +
		`"d":{"time":"2026-10-11T12:00:00.000Z","packetHash":"AB12","topic":"meshcore/EGNX/OBS/packets",` +
		`"topicPrefix":"meshcore","iata":"EGNX","packetType":4,"routeType":1,"hopCount":2,"rssi":-80,"snr":6.5,` +
		`"payloadJson":` + mustJSONString(t, payload) + `,"companionSender":null,"rawHex":"DEADBEEF","advertCount":3,` +
		`"pathHashes":["ABCD"],"pathHashSizeBytes":2,"network":"ukmesh","transportCodes":null,"regionScope":null}}`

	entry, ok := decodeRedisStreamValue(value)
	if !ok {
		t.Fatal("valid pending entry must decode")
	}
	if entry.time != "2026-10-11T12:00:00.000Z" {
		t.Fatalf("time = %q", entry.time)
	}
	if entry.topic != "meshcore/EGNX/OBS/packets" {
		t.Fatalf("topic = %q", entry.topic)
	}
	if entry.rawHex != "DEADBEEF" {
		t.Fatalf("rawHex = %q", entry.rawHex)
	}
	if entry.network != "ukmesh" {
		t.Fatalf("network = %q", entry.network)
	}
	if entry.snr == nil || *entry.snr != 6.5 {
		t.Fatalf("snr = %v", entry.snr)
	}
	if entry.rssi == nil || *entry.rssi != -80 {
		t.Fatalf("rssi = %v", entry.rssi)
	}
	if entry.direction != "rx" {
		t.Fatalf("direction = %q", entry.direction)
	}
	if !entry.visibilityOK {
		t.Fatal("visibilityOK must be true for vo=1")
	}
}

func TestDecodeRedisStreamValueRejections(t *testing.T) {
	cases := map[string]string{
		"empty":         "",
		"not json":      "nope",
		"commit marker": `{"v":1,"k":"c","t":"1700000000000-0","a":1700000001000,"n":5}`,
		"wrong version": `{"v":2,"k":"p","d":{}}`,
		"missing body":  `{"v":1,"k":"p"}`,
		"missing raw":   `{"v":1,"k":"p","vo":1,"d":{"time":"2026-10-11T12:00:00Z","topic":"t","rawHex":""}}`,
		"missing time":  `{"v":1,"k":"p","vo":1,"d":{"time":"","topic":"t","rawHex":"AA"}}`,
		"missing topic": `{"v":1,"k":"p","vo":1,"d":{"time":"2026-10-11T12:00:00Z","topic":"","rawHex":"AA"}}`,
	}
	for name, value := range cases {
		if _, ok := decodeRedisStreamValue(value); ok {
			t.Fatalf("%s: expected rejection", name)
		}
	}
}

func TestRedisEntryAllowedFilters(t *testing.T) {
	privateEntry := redisPendingEntry{network: "ukmesh", visibilityOK: false}
	publicEntry := redisPendingEntry{network: "ukmesh", visibilityOK: true}
	otherNet := redisPendingEntry{network: "teesside", visibilityOK: true}

	// Scope matching is independent of the frozen admission visibility bit.
	srcDefault := RedisSource{}
	if !redisEntryAllowed(privateEntry, srcDefault) {
		t.Fatal("admission visibility is not current authorization")
	}
	if !redisEntryAllowed(publicEntry, srcDefault) {
		t.Fatal("public entry must pass default guard")
	}

	noGuard := false
	srcOpen := RedisSource{RequirePublicVisibility: &noGuard}
	if !redisEntryAllowed(privateEntry, srcOpen) {
		t.Fatal("private entry must pass when the guard is disabled")
	}

	srcScoped := RedisSource{Networks: []string{"ukmesh"}}
	if !redisEntryAllowed(publicEntry, srcScoped) {
		t.Fatal("scoped entry must pass")
	}
	if redisEntryAllowed(otherNet, srcScoped) {
		t.Fatal("out-of-scope network must be rejected")
	}

	srcScopedOpen := RedisSource{Networks: []string{"ukmesh", "teesside"}, RequirePublicVisibility: &noGuard}
	if !redisEntryAllowed(otherNet, srcScopedOpen) {
		t.Fatal("multi-network scope must include listed networks")
	}
}

func TestF01CorePublicPolicyOverridesFrozenRedisVisibility(t *testing.T) {
	public := &corePublicPolicy{}
	e := redisPendingEntry{network: "northeast", rxNodeID: "AABB", visibilityOK: false,
		pathHashes: []string{"BEEF"}, pathHashSizeBytes: 2}
	if !public.allows(e) {
		t.Fatal("a now-public entry must pass even when its admission bit was false")
	}
	privateObserver := &corePublicPolicy{prefixes: []corePrivatePrefix{{NodeID: "AABB", Network: "ukmesh"}}}
	if privateObserver.allows(e) {
		t.Fatal("current observer privacy must override vo=1")
	}
	privatePath := &corePublicPolicy{prefixes: []corePrivatePrefix{{Prefix: "BEEF", Size: 2, Network: "teesside"}}}
	if privatePath.allows(e) {
		t.Fatal("current path privacy must match family scope")
	}
	if (&corePublicPolicy{}).allows(redisPendingEntry{network: "ukmesh", pathHashes: []string{"GG"}, pathHashSizeBytes: 1}) {
		t.Fatal("invalid path cannot become public")
	}
	if (*corePublicPolicy)(nil).allows(e) {
		t.Fatal("missing authority must fail closed")
	}
}

func TestF07RedisTailRequiresPairedCompletenessSource(t *testing.T) {
	cfg := &Config{RedisSources: []RedisSource{{Name: "live"}}}
	if err := cfg.ValidateCompanionSources(); err == nil {
		t.Fatal("tail-only Redis source must be rejected")
	}
	cfg.PostgresSources = []PostgresSource{{Name: "complete", DSN: "postgres://fixture"}}
	if err := cfg.ValidateCompanionSources(); err != nil {
		t.Fatalf("paired source rejected: %v", err)
	}
}

func TestRedisEnvelopeTimestamp(t *testing.T) {
	if got := redisEnvelopeTimestamp("2026-10-11T12:00:00.000Z"); got != "2026-10-11T12:00:00Z" {
		// RFC3339Nano drops trailing zero fractions; both forms are valid.
		if got != "2026-10-11T12:00:00.000Z" {
			t.Fatalf("timestamp normalisation = %q", got)
		}
	}
	// Sub-second precision must survive.
	got := redisEnvelopeTimestamp("2026-10-11T12:00:00.123Z")
	if got != "2026-10-11T12:00:00.123Z" {
		t.Fatalf("sub-second timestamp = %q", got)
	}
	// Unparseable values pass through unchanged rather than being dropped.
	if got := redisEnvelopeTimestamp("garbage"); got != "garbage" {
		t.Fatalf("garbage timestamp = %q", got)
	}
}

func TestRedisSourceStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "redis-source-test.state.json")

	if _, ok := loadRedisSourceState(path); ok {
		t.Fatal("missing file must read as no cursor")
	}

	want := redisSourceState{LastID: "1700000000000-5"}
	if err := saveRedisSourceState(path, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok := loadRedisSourceState(path)
	if !ok || got.LastID != want.LastID {
		t.Fatalf("round trip = %+v ok=%v", got, ok)
	}

	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadRedisSourceState(path); ok {
		t.Fatal("corrupt file must read as no cursor")
	}
}

func TestDefaultRedisStatePath(t *testing.T) {
	got := defaultRedisStatePath("/cfg", "ukmesh-live")
	if got != filepath.Join("/cfg", "redis-source-ukmesh-live.state.json") {
		t.Fatalf("state path = %q", got)
	}
	if got := defaultRedisStatePath("/cfg", ""); got != filepath.Join("/cfg", "redis-source-default.state.json") {
		t.Fatalf("empty-name state path = %q", got)
	}
	if got := defaultRedisStatePath("/cfg", "a/b c"); got != filepath.Join("/cfg", "redis-source-a-b-c.state.json") {
		t.Fatalf("unsafe-name state path = %q", got)
	}
}

func TestRedisRedactURL(t *testing.T) {
	if got := redisRedactURL("redis://:secret@redis:6379"); got != "redis://:***@redis:6379" {
		t.Fatalf("redacted = %q", got)
	}
	if got := redisRedactURL("redis://redis:6379"); got != "redis://redis:6379" {
		t.Fatalf("no-credential URL = %q", got)
	}
}

func TestRedisSourceConfigDefaults(t *testing.T) {
	src := RedisSource{}
	if src.PollIntervalMsOrDefault() != 500 {
		t.Fatalf("poll default = %d", src.PollIntervalMsOrDefault())
	}
	if src.BatchSizeOrDefault() != 500 {
		t.Fatalf("batch default = %d", src.BatchSizeOrDefault())
	}
	if src.StreamKeyOrDefault() != "meshcore:ingest:pending" {
		t.Fatalf("stream key default = %q", src.StreamKeyOrDefault())
	}
	if !src.PublicVisibilityRequired() {
		t.Fatal("visibility guard must default on")
	}
	off := false
	src2 := RedisSource{PollIntervalMs: 100, BatchSize: 25, StreamKey: "custom", RequirePublicVisibility: &off}
	if src2.PollIntervalMsOrDefault() != 100 || src2.BatchSizeOrDefault() != 25 ||
		src2.StreamKeyOrDefault() != "custom" || src2.PublicVisibilityRequired() {
		t.Fatal("explicit values must be honoured")
	}
}

func TestRedisEnvelopeMatchesPipelineShape(t *testing.T) {
	entry, ok := decodeRedisStreamValue(`{"v":1,"k":"p","vo":1,` +
		`"d":{"time":"2026-10-11T12:00:00.500Z","packetHash":"AB","topic":"meshcore/EGNX/OBS/packets",` +
		`"network":"ukmesh","rawHex":"00FF","snr":5.5,"rssi":-70,"payloadJson":"{\"direction\":\"tx\"}"}}`)
	if !ok {
		t.Fatal("entry must decode")
	}
	env := pgEnvelope{
		Raw:       entry.rawHex,
		Timestamp: redisEnvelopeTimestamp(entry.time),
		SNR:       entry.snr,
		RSSI:      entry.rssi,
		Direction: entry.direction,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]interface{}
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back["raw"] != "00FF" || back["direction"] != "tx" || back["timestamp"] != "2026-10-11T12:00:00.5Z" {
		t.Fatalf("envelope = %s", raw)
	}
	if back["SNR"] != 5.5 || back["RSSI"] != -70.0 {
		t.Fatalf("signal fields = %s", raw)
	}
}

func mustJSONString(t *testing.T, s string) string {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
