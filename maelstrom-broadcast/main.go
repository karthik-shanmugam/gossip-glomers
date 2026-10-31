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

type Propagation struct {
	Message int
	Src     string
}

func NewState() *State {
	return &State{
		Messages:  make(map[int]struct{}),
		Neighbors: make([]string, 0),
	}
}

// propagate reads new messages off the queue and sends each one to every
// neighbor, retrying each neighbor until it replies with broadcast_ok.
func propagate(n *maelstrom.Node, state *State, queue <-chan Propagation) {
	for propagation := range queue {
		// Copy the neighbors so we don't hold the lock while sending.
		state.RLock()
		neighbors := slices.Clone(state.Neighbors)
		state.RUnlock()

		body := Broadcast{MessageType: "broadcast", Message: propagation.Message}
		for _, neighbor := range neighbors {
			if neighbor != propagation.Src {
				go sendUntilAcked(n, neighbor, body)
			}
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

	queue := make(chan Propagation, 1000)
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
			queue <- Propagation{Message: broadcast.Message, Src: msg.Src}
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

	// Ignore Maelstrom's suggested grid and use a star: the lowest node ID is
	// the hub and is connected to every other node; everyone else only talks
	// to the hub. Every node computes the same hub from the shared node list.
	n.Handle("topology", func(msg maelstrom.Message) error {
		hub := slices.Min(n.NodeIDs())

		state.Lock()
		defer state.Unlock()
		if n.ID() == hub {
			state.Neighbors = slices.DeleteFunc(slices.Clone(n.NodeIDs()),func(id string) bool { return id == hub })
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
