package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Send struct {
	MessageType string `json:"type"`
	Key         string `json:"key"`
	Msg         int    `json:"msg"`
}

type Poll struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

type CommitOffsets struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

type ListCommittedOffsets struct {
	MessageType string   `json:"type"`
	Keys        []string `json:"keys"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		var send Send
		if err := json.Unmarshal(msg.Body, &send); err != nil {
			return err
		}
		// TODO: append send.Msg to the log for send.Key and get its offset.
		offset := 0

		resp := map[string]any{
			"type":   "send_ok",
			"offset": offset,
		}
		return n.Reply(msg, resp)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var poll Poll
		if err := json.Unmarshal(msg.Body, &poll); err != nil {
			return err
		}
		// TODO: for each key, collect [offset, msg] pairs starting at
		// poll.Offsets[key].
		msgs := map[string][][2]int{}

		resp := map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		}
		return n.Reply(msg, resp)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var commitOffsets CommitOffsets
		if err := json.Unmarshal(msg.Body, &commitOffsets); err != nil {
			return err
		}
		// TODO: record commitOffsets.Offsets as committed.

		resp := map[string]any{
			"type": "commit_offsets_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var listCommittedOffsets ListCommittedOffsets
		if err := json.Unmarshal(msg.Body, &listCommittedOffsets); err != nil {
			return err
		}
		// TODO: look up the committed offset for each key in
		// listCommittedOffsets.Keys. Keys with no committed offset can be left out.
		offsets := map[string]int{}

		resp := map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
