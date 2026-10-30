package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// One authority check per captured broadcast batch, shared by all clients.
// Public measurements have no per-client application queue after that check.
func (h *Hub) BroadcastPublicBatch(messages []WSMessage, generation int64) {
	if h.publicProjection == nil || len(messages) == 0 || len(messages) > 1000 {
		return
	}
	payloads := make([][]byte, 0, len(messages))
	bytes := 0
	for _, message := range messages {
		encoded, err := json.Marshal(message)
		if err != nil {
			return
		}
		bytes += len(encoded)
		if bytes > 8*1024*1024 {
			return
		}
		payloads = append(payloads, encoded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	current, err := h.publicProjection.epoch(ctx)
	cancel()
	if err != nil || current != generation {
		return
	}
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	deadline := time.Now().Add(time.Second)
	jobs := make(chan *Client)
	var workers sync.WaitGroup
	for i := 0; i < min(32, len(clients)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for client := range jobs {
				if !client.writeMu.TryLock() {
					client.conn.Close()
					continue
				}
				client.conn.SetWriteDeadline(deadline)
				for _, payload := range payloads {
					if err := client.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						client.conn.Close()
						break
					}
				}
				client.writeMu.Unlock()
			}
		}()
	}
	for _, client := range clients {
		jobs <- client
	}
	close(jobs)
	workers.Wait()
}
