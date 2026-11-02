package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Op is one micro-operation in a transaction. On the wire it's a 3-element
// array: ["r", key, null] for a read or ["w", key, value] for a write. Value
// is a pointer so a missing value (null) can be told apart from 0.
type Op struct {
	Kind  string
	Key   int
	Value *int
}

func (o *Op) UnmarshalJSON(data []byte) error {
	var fields [3]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if err := json.Unmarshal(fields[0], &o.Kind); err != nil {
		return err
	}
	if err := json.Unmarshal(fields[1], &o.Key); err != nil {
		return err
	}
	return json.Unmarshal(fields[2], &o.Value)
}

func (o Op) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{o.Kind, o.Key, o.Value})
}

type Txn struct {
	MessageType string `json:"type"`
	Ops         []Op   `json:"txn"`
}

// Timestamp orders transactions across the cluster. Counter is a Lamport
// clock and Node breaks ties, so no two transactions share a timestamp.
type Timestamp struct {
	Counter int    `json:"counter"`
	Node    string `json:"node"`
}

func (t Timestamp) Less(other Timestamp) bool {
	if t.Counter != other.Counter {
		return t.Counter < other.Counter
	}
	return t.Node < other.Node
}

// Replicated is one transaction's writes (the last value it wrote to each
// key), sent to the other nodes.
type Replicated struct {
	Timestamp Timestamp   `json:"timestamp"`
	Writes    map[int]int `json:"writes"`
}

type Replicate struct {
	MessageType string       `json:"type"`
	Txns        []Replicated `json:"txns"`
}

// NodeState is this node's full copy of the data. Each key remembers the
// timestamp of the write it holds, and a write only replaces it if it has a
// later timestamp (last writer wins), so every node ends up with the same
// order of writes for each key regardless of the order they arrive in.
type NodeState struct {
	sync.RWMutex
	Data       map[int]int
	Timestamps map[int]Timestamp
	Clock      int
}

func NewNodeState() *NodeState {
	return &NodeState{
		Data:       make(map[int]int),
		Timestamps: make(map[int]Timestamp),
	}
}

// applyLocked applies writes made at ts, keeping only the ones newer than
// what each key already holds. The caller must hold the lock.
func (s *NodeState) applyLocked(ts Timestamp, writes map[int]int) {
	s.Clock = max(s.Clock, ts.Counter)
	for key, value := range writes {
		if current, ok := s.Timestamps[key]; ok && !current.Less(ts) {
			continue
		}
		s.Data[key] = value
		s.Timestamps[key] = ts
	}
}

// Outbox holds the transactions that still need to be sent to one peer.
type Outbox struct {
	sync.Mutex
	Pending []Replicated
}

const (
	replicateInterval = 50 * time.Millisecond
	rpcTimeout        = time.Second
	// The Maelstrom library reads each message as one line with a size
	// limit, so a backlog built up during a partition is sent in batches of
	// at most this many transactions.
	maxBatchSize = 100
)

// replicate sends what's pending for peer once per interval, in batches,
// and retries until the peer acknowledges them. When a backlog needs more
// than one batch, the batches go back to back rather than one per interval.
// Applying a batch is idempotent (an already-applied write is never newer
// than itself), so resending after a timeout is safe.
func replicate(n *maelstrom.Node, peer string, outbox *Outbox) {
	ticker := time.NewTicker(replicateInterval)
	defer ticker.Stop()
	for range ticker.C {
		for {
			outbox.Lock()
			batch := slices.Clone(outbox.Pending[:min(len(outbox.Pending), maxBatchSize)])
			outbox.Unlock()
			if len(batch) == 0 {
				break
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			_, err := n.SyncRPC(ctx, peer, Replicate{MessageType: "replicate", Txns: batch})
			cancel()
			if err != nil {
				// Try again on the next interval.
				break
			}

			// Only drop what this batch carried; anything added since stays.
			outbox.Lock()
			outbox.Pending = outbox.Pending[len(batch):]
			outbox.Unlock()
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	state := NewNodeState()

	// One outbox per peer, created once the node list is known and only read
	// after that.
	outboxes := map[string]*Outbox{}

	n.Handle("init", func(msg maelstrom.Message) error {
		for _, peer := range n.NodeIDs() {
			if peer == n.ID() {
				continue
			}
			outboxes[peer] = &Outbox{}
			go replicate(n, peer, outboxes[peer])
		}
		return nil
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var replicate Replicate
		if err := json.Unmarshal(msg.Body, &replicate); err != nil {
			return err
		}
		state.Lock()
		for _, txn := range replicate.Txns {
			state.applyLocked(txn.Timestamp, txn.Writes)
		}
		state.Unlock()

		resp := map[string]any{
			"type": "replicate_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var txn Txn
		if err := json.Unmarshal(msg.Body, &txn); err != nil {
			return err
		}

		// Hold the lock for the whole transaction so it runs as one step.
		// Advancing the clock first gives this transaction a timestamp later
		// than anything this node has applied, so its own writes always win
		// here and later reads in the transaction see them.
		state.Lock()
		state.Clock++
		ts := Timestamp{Counter: state.Clock, Node: n.ID()}
		writes := map[int]int{}
		for i := range txn.Ops {
			op := &txn.Ops[i]
			switch op.Kind {
			case "r":
				if value, ok := state.Data[op.Key]; ok {
					op.Value = &value
				}
			case "w":
				// ts is newer than anything held, so write directly. (Going
				// through applyLocked would drop a second write to the same
				// key in this transaction, since its timestamp isn't newer.)
				writes[op.Key] = *op.Value
				state.Data[op.Key] = *op.Value
				state.Timestamps[op.Key] = ts
			}
		}
		state.Unlock()

		if len(writes) > 0 {
			for _, outbox := range outboxes {
				outbox.Lock()
				outbox.Pending = append(outbox.Pending, Replicated{Timestamp: ts, Writes: writes})
				outbox.Unlock()
			}
		}

		resp := map[string]any{
			"type": "txn_ok",
			"txn":  txn.Ops,
		}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
