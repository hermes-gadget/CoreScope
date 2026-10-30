package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type publicLiveEntry struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}
type publicLiveBatch struct {
	Generation int64             `json:"generation"`
	Entries    []publicLiveEntry `json:"entries"`
	NextCursor string            `json:"nextCursor"`
}

func validRedisID(id string) bool {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 64); err != nil {
			return false
		}
	}
	return true
}
func compareRedisIDs(left, right string) int {
	a, b := strings.Split(left, "-"), strings.Split(right, "-")
	for i := 0; i < 2; i++ {
		x, _ := strconv.ParseUint(a[i], 10, 64)
		y, _ := strconv.ParseUint(b[i], 10, 64)
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
	}
	return 0
}

func fetchPublicLiveBatch(ctx context.Context, client *http.Client, source RedisSource, cursor string) (publicLiveBatch, error) {
	target, err := url.Parse(source.PublicURL)
	if err != nil {
		return publicLiveBatch{}, err
	}
	query := target.Query()
	query.Set("after", cursor)
	query.Set("limit", strconv.Itoa(min(source.BatchSizeOrDefault(), 500)))
	target.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return publicLiveBatch{}, err
	}
	req.Header.Set("Authorization", "Bearer "+source.PublicToken)
	response, err := client.Do(req)
	if err != nil {
		return publicLiveBatch{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return publicLiveBatch{}, errors.New("authorized public source unavailable")
	}
	var batch publicLiveBatch
	if err := json.NewDecoder(io.LimitReader(response.Body, 8*1024*1024)).Decode(&batch); err != nil {
		return batch, err
	}
	if batch.Generation < 1 || len(batch.Entries) > 500 || !validRedisID(batch.NextCursor) {
		return batch, errors.New("invalid public source batch")
	}
	if compareRedisIDs(batch.NextCursor, cursor) < 0 {
		return batch, errors.New("public cursor moved backwards")
	}
	previous := cursor
	for _, entry := range batch.Entries {
		if !validRedisID(entry.ID) || compareRedisIDs(entry.ID, previous) <= 0 || compareRedisIDs(entry.ID, batch.NextCursor) > 0 {
			return batch, errors.New("invalid public observation order")
		}
		previous = entry.ID
	}
	return batch, nil
}

func runPublicLiveSource(store *Store, source RedisSource, buffer *IngestBuffer, configDir string) {
	tag := source.Name
	if tag == "" {
		tag = "public-live"
	}
	if store.publicProjection == nil || source.PublicURL == "" || source.PublicToken == "" || source.URL != "" {
		log.Printf("[public-live:%s] ERROR: revocable projection and credentialed public source required", tag)
		return
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	statePath := source.StatePath
	if statePath == "" {
		statePath = defaultRedisStatePath(configDir, tag)
	}
	state, ok := loadRedisSourceState(statePath)
	if !ok {
		state = redisSourceState{LastID: redisZeroID}
	}
	status := RegisterSourceStatus(tag, "authorized-public-api")
	connected := false
	refused := func(err error) {
		status.MarkDisconnect(time.Now(), err)
		connected = false
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		batch, err := fetchPublicLiveBatch(ctx, client, source, state.LastID)
		cancel()
		if err != nil {
			refused(err)
			time.Sleep(time.Second)
			continue
		}
		records := make([]publicProjectionRecord, 0, len(batch.Entries))
		for _, item := range batch.Entries {
			entry, ok := decodeRedisStreamValue(item.Value)
			if !ok {
				err = errors.New("invalid public observation")
				break
			}
			if !redisEntryAllowed(entry, source) {
				continue
			}
			payload, _ := json.Marshal(pgEnvelope{Raw: entry.rawHex, Timestamp: redisEnvelopeTimestamp(entry.time), SNR: entry.snr, RSSI: entry.rssi, Direction: entry.direction})
			records = append(records, publicProjectionRecord{ObservationID: entry.observationID, Topic: entry.topic, Payload: payload,
				Network: entry.network, RXNodeID: entry.rxNodeID, SrcNodeID: entry.srcNodeID, PathHashes: entry.pathHashes, PathHashSizeBytes: entry.pathHashSizeBytes,
				Tag: tag, Source: MQTTSource{Name: tag, PreserveEnvelopeTime: true, MaxEnvelopePastDays: source.MaxEnvelopePastDays}})
		}
		if err != nil {
			refused(err)
			time.Sleep(time.Second)
			continue
		}
		result, accepted := buffer.SubmitTracked(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return store.publicProjection.Batch(ctx, records)
		})
		next := redisSourceState{LastID: batch.NextCursor}
		var results []<-chan error
		if accepted {
			results = append(results, result)
		}
		if err := finishSourceBatch(results, !accepted, func() error { return saveRedisSourceState(statePath, next) }); err != nil {
			refused(err)
			time.Sleep(time.Second)
			continue
		}
		state = next
		if !connected {
			status.MarkConnect(time.Now())
			connected = true
		}
		for range records {
			status.MarkPacket(time.Now())
		}
		markReceiptForTag(tag, time.Now())
		if source.MaxRowsPerSec > 0 && len(records) > 0 {
			time.Sleep(time.Duration(float64(len(records)) / float64(source.MaxRowsPerSec) * float64(time.Second)))
		}
		time.Sleep(time.Duration(source.PollIntervalMsOrDefault()) * time.Millisecond)
	}
}
