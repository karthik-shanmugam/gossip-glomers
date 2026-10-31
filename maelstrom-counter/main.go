package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Add struct {
	MessageType string `json:"type"`
	Delta       int    `json:"delta"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("add", func(msg maelstrom.Message) error {
		var add Add
		if err := json.Unmarshal(msg.Body, &add); err != nil {
			return err
		}
		// TODO: apply add.Delta to the counter.

		resp := map[string]any{
			"type": "add_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// TODO: read the current counter value.
		value := 0

		resp := map[string]any{
			"type":  "read_ok",
			"value": value,
		}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
