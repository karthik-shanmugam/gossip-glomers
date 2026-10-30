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

type Topology struct {
	MessageType string              `json:"type"`
	Topologies  map[string][]string `json:"topology"`
}

type State struct {
	sync.RWMutex
	Messages  map[int]struct{}
	Neighbors []string
}

func NewState() *State {
	return &State{
		Messages:  make(map[int]struct{}),
		Neighbors: make([]string, 0),
	}
}

// propagate reads new messages off the queue and sends each one to every
// neighbor, retrying each neighbor until it replies with broadcast_ok.
func propagate(n *maelstrom.Node, state *State, queue <-chan int) {
	for message := range queue {
		// Copy the neighbors so we don't hold the lock while sending.
		state.RLock()
		neighbors := slices.Clone(state.Neighbors)
		state.RUnlock()

		body := Broadcast{MessageType: "broadcast", Message: message}
		for _, neighbor := range neighbors {
			// One goroutine per neighbor, so a slow or partitioned neighbor
			// doesn't hold up delivery to the others or block the queue.
			go sendUntilAcked(n, neighbor, body)
		}
	}
}

// sendUntilAcked sends body to dest and retries until it gets broadcast_ok.
func sendUntilAcked(n *maelstrom.Node, dest string, body Broadcast) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := n.SyncRPC(ctx, dest, body)
		cancel()
		if err == nil && resp.Type() == "broadcast_ok" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	n := maelstrom.NewNode()

	state := NewState()

	queue := make(chan int, 1000)
	go propagate(n, state, queue)

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
			queue <- broadcast.Message
		}

		resp := map[string]any{
			"type": "broadcast_ok",
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

	n.Handle("topology", func(msg maelstrom.Message) error {
		var topology Topology
		if err := json.Unmarshal(msg.Body, &topology); err != nil {
			return err
		}
		state.Lock()
		defer state.Unlock()
		state.Neighbors = topology.Topologies[msg.Dest]
		resp := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
