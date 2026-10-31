package main

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Broadcast struct {
	MessageType string `json:"type"`
	Message     int    `json:"message"`
}

// Propagate is the internal node-to-node message. Clients only ever use
// broadcast; nodes only ever forward with propagate.
type Propagate struct {
	MessageType string `json:"type"`
	Messages    []int  `json:"messages"`
}

type Topology struct {
	MessageType string              `json:"type"`
	Topologies  map[string][]string `json:"topology"`
}

type State struct {
	sync.RWMutex
	Messages  map[int]struct{}
	Neighbors []string
	IsHub     bool
}

type Propagation struct {
	Messages []int
	Src      string
}

func NewState() *State {
	return &State{
		Messages:  make(map[int]struct{}),
		Neighbors: make([]string, 0),
	}
}

// Batcher tracks, for each neighbor, the messages the hub still has to get
// acknowledged, and whether a send to that neighbor is already in flight.
type Batcher struct {
	sync.Mutex
	pending  map[string]map[int]struct{}
	inFlight map[string]bool
}

func NewBatcher() *Batcher {
	return &Batcher{
		pending:  make(map[string]map[int]struct{}),
		inFlight: make(map[string]bool),
	}
}

func (b *Batcher) add(dest string, messages []int) {
	b.Lock()
	defer b.Unlock()
	if b.pending[dest] == nil {
		b.pending[dest] = make(map[int]struct{})
	}
	for _, message := range messages {
		b.pending[dest][message] = struct{}{}
	}
}

// propagate reads new messages off the queue and hands them to every neighbor
// except the one they came from. The hub queues them for the next batch;
// leaves send them to the hub right away.
func propagate(n *maelstrom.Node, state *State, batcher *Batcher, queue <-chan Propagation) {
	for propagation := range queue {
		// Copy the neighbors so we don't hold the lock while sending.
		state.RLock()
		neighbors := slices.Clone(state.Neighbors)
		isHub := state.IsHub
		state.RUnlock()

		for _, neighbor := range neighbors {
			if neighbor == propagation.Src {
				continue
			}
			if isHub {
				batcher.add(neighbor, propagation.Messages)
			} else {
				body := Propagate{MessageType: "propagate", Messages: propagation.Messages}
				go sendUntilAcked(n, neighbor, body)
			}
		}
	}
}

// flushBatches starts a send to every neighbor that has pending messages and
// no send already in flight, once per interval. Empty batches are skipped.
func flushBatches(n *maelstrom.Node, batcher *Batcher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		batcher.Lock()
		for dest, messages := range batcher.pending {
			if len(messages) > 0 && !batcher.inFlight[dest] {
				batcher.inFlight[dest] = true
				go sendBatchUntilAcked(n, batcher, dest)
			}
		}
		batcher.Unlock()
	}
}

// sendBatchUntilAcked sends dest everything pending for it. On failure it
// retries right away instead of waiting for the next interval, folding in any
// messages that arrived since the last attempt.
func sendBatchUntilAcked(n *maelstrom.Node, batcher *Batcher, dest string) {
	for {
		batcher.Lock()
		messages := slices.Collect(maps.Keys(batcher.pending[dest]))
		batcher.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := n.SyncRPC(ctx, dest, Propagate{MessageType: "propagate", Messages: messages})
		cancel()
		if err == nil && resp.Type() == "propagate_ok" {
			// Only clear what this send carried; anything added meanwhile
			// stays pending for the next batch.
			batcher.Lock()
			for _, message := range messages {
				delete(batcher.pending[dest], message)
			}
			batcher.inFlight[dest] = false
			batcher.Unlock()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sendUntilAcked sends body to dest and retries until it gets propagate_ok.
func sendUntilAcked(n *maelstrom.Node, dest string, body Propagate) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := n.SyncRPC(ctx, dest, body)
		cancel()
		if err == nil && resp.Type() == "propagate_ok" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	n := maelstrom.NewNode()

	state := NewState()
	batcher := NewBatcher()

	queue := make(chan Propagation, 1000)
	go propagate(n, state, batcher, queue)
	go flushBatches(n, batcher, 300*time.Millisecond)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcast Broadcast
		if err := json.Unmarshal(msg.Body, &broadcast); err != nil {
			return err
		}
		state.Lock()
		_, alreadyReceived := state.Messages[broadcast.Message]
		state.Messages[broadcast.Message] = struct{}{}
		state.Unlock()
		if !alreadyReceived {
			queue <- Propagation{Messages: []int{broadcast.Message}, Src: msg.Src}
		}

		resp := map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("propagate", func(msg maelstrom.Message) error {
		var body Propagate
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var newMessages []int
		state.Lock()
		for _, message := range body.Messages {
			if _, alreadyReceived := state.Messages[message]; !alreadyReceived {
				state.Messages[message] = struct{}{}
				newMessages = append(newMessages, message)
			}
		}
		state.Unlock()
		if len(newMessages) > 0 {
			queue <- Propagation{Messages: newMessages, Src: msg.Src}
		}

		resp := map[string]any{
			"type": "propagate_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.RLock()
		defer state.RUnlock()
		resp := map[string]any{
			"type":     "read_ok",
			"messages": slices.Collect(maps.Keys(state.Messages)),
		}
		return n.Reply(msg, resp)
	})

	// Ignore Maelstrom's suggested grid and use a star: the lowest node ID is
	// the hub and is connected to every other node; everyone else only talks
	// to the hub. Every node computes the same hub from the shared node list.
	n.Handle("topology", func(msg maelstrom.Message) error {
		hub := slices.Min(n.NodeIDs())

		state.Lock()
		defer state.Unlock()
		state.IsHub = n.ID() == hub
		if state.IsHub {
			state.Neighbors = slices.DeleteFunc(slices.Clone(n.NodeIDs()), func(id string) bool { return id == hub })
		} else {
			state.Neighbors = []string{hub}
		}
		resp := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
