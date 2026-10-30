package main

import (
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"

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

//	type TopologyList struct {
//		MessageType string `json:"type"`
//		Topologies TologyList `json:"message"`
//	}
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

func main() {
	n := maelstrom.NewNode()

	// messages := make(map[int]struct{})
	// var neighbors []string
	state := NewState()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcast Broadcast
		if err := json.Unmarshal(msg.Body, &broadcast); err != nil {
			return err
		}
		state.Lock()
		defer state.Unlock()
		_, alreadyReceived := state.Messages[broadcast.Message]
		if !alreadyReceived {
			for _, neighbor := range state.Neighbors {
				n.Send(neighbor, msg.Body)
			}
		}
		state.Messages[broadcast.Message] = struct{}{}

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
