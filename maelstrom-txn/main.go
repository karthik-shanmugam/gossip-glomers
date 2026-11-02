package main

import (
	"encoding/json"
	"log"

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

func main() {
	n := maelstrom.NewNode()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var txn Txn
		if err := json.Unmarshal(msg.Body, &txn); err != nil {
			return err
		}

		for i := range txn.Ops {
			op := &txn.Ops[i]
			switch op.Kind {
			case "r":
				// TODO: set op.Value to the current value of op.Key, or
				// leave it nil if the key doesn't exist.
			case "w":
				// TODO: write *op.Value to op.Key.
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
