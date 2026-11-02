package main

import (
	"encoding/json"
	"log"
	"sync"

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

type Log struct {
	CommittedOffset *int
	Messages        []int
}

type State struct {
	sync.RWMutex
	Logs map[string]*Log
}

func NewLog() *Log {
	return &Log{}
}

func NewState() *State {
	return &State{
		Logs: make(map[string]*Log),
	}
}

func main() {
	n := maelstrom.NewNode()
	state := NewState()

	n.Handle("send", func(msg maelstrom.Message) error {
		var send Send
		if err := json.Unmarshal(msg.Body, &send); err != nil {
			return err
		}
		state.Lock()
		kafkaLog, ok := state.Logs[send.Key]
		if !ok {
			kafkaLog = NewLog()
			state.Logs[send.Key] = kafkaLog
		}
		kafkaLog.Messages = append(kafkaLog.Messages, send.Msg)
		offset := len(kafkaLog.Messages) - 1
		state.Unlock()

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
		msgs := map[string][][2]int{}
		state.RLock()
		for key, start := range poll.Offsets {
			kafkaLog, ok := state.Logs[key]
			if !ok {
				continue
			}
			for offset := max(start, 0); offset < len(kafkaLog.Messages); offset++ {
				msgs[key] = append(msgs[key], [2]int{offset, kafkaLog.Messages[offset]})
			}
		}
		state.RUnlock()

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
		state.Lock()
		for key, offset := range commitOffsets.Offsets {
			kafkaLog, ok := state.Logs[key]
			if !ok {
				kafkaLog = NewLog()
				state.Logs[key] = kafkaLog
			}
			committed := offset
			kafkaLog.CommittedOffset = &committed
		}
		state.Unlock()

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
		// Keys that have never been committed are left out.
		offsets := map[string]int{}
		state.RLock()
		for _, key := range listCommittedOffsets.Keys {
			kafkaLog, ok := state.Logs[key]
			if !ok || kafkaLog.CommittedOffset == nil {
				continue
			}
			offsets[key] = *kafkaLog.CommittedOffset
		}
		state.RUnlock()

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
