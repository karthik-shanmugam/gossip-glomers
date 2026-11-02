package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

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

const kvTimeout = time.Second

// Each log is stored in lin-kv as a single list of messages, where a
// message's offset is its index. Committed offsets are stored separately.
func logKey(key string) string {
	return "log/" + key
}

func committedKey(key string) string {
	return "committed/" + key
}

// readLog returns the log for key, both as the raw value lin-kv returned (to
// pass back as CompareAndSwap's from) and decoded into messages. A log that
// doesn't exist yet comes back as nil with no error.
func readLog(kv *maelstrom.KV, key string) (any, []int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	raw, err := kv.Read(ctx, logKey(key))
	cancel()
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	// JSON lists decode as []any of float64.
	var messages []int
	for _, message := range raw.([]any) {
		messages = append(messages, int(message.(float64)))
	}
	return raw, messages, nil
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var send Send
		if err := json.Unmarshal(msg.Body, &send); err != nil {
			return err
		}

		// Append with a read / compare-and-swap loop, retrying when another
		// send to the same key got in first.
		var offset int
		for {
			raw, messages, err := readLog(kv, send.Key)
			if err != nil {
				return err
			}
			messages = append(messages, send.Msg)

			ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
			err = kv.CompareAndSwap(ctx, logKey(send.Key), raw, messages, true)
			cancel()
			if err == nil {
				offset = len(messages) - 1
				break
			}
			// Any other error (e.g. a timeout) means the append may or may
			// not have happened, so don't retry and risk appending twice.
			// Returning it replies with a crash error, which Maelstrom treats
			// as indeterminate.
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return err
			}
		}

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
		for key, start := range poll.Offsets {
			_, messages, err := readLog(kv, key)
			if err != nil {
				return err
			}
			for offset := max(start, 0); offset < len(messages); offset++ {
				msgs[key] = append(msgs[key], [2]int{offset, messages[offset]})
			}
		}

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
		for key, offset := range commitOffsets.Offsets {
			ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
			err := kv.Write(ctx, committedKey(key), offset)
			cancel()
			if err != nil {
				return err
			}
		}

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
		for _, key := range listCommittedOffsets.Keys {
			ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
			offset, err := kv.ReadInt(ctx, committedKey(key))
			cancel()
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				continue
			}
			if err != nil {
				return err
			}
			offsets[key] = offset
		}

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
