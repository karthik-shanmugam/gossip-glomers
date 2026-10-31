package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Add struct {
	MessageType string `json:"type"`
	Delta       int    `json:"delta"`
}

const CounterKey = "thisisthekeyformycounter"

func syncDeltas(deltas <-chan int, kv *maelstrom.KV) {
	remainingDelta := 0
	for {
		if remainingDelta == 0 {
			remainingDelta = <-deltas
		} else {
			done := false
			for !done {
				select {
				case additionalDelta := <-deltas:
					remainingDelta += additionalDelta
				default:
					done = true
				}
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		lastRead, err := kv.ReadInt(ctx, CounterKey)
		cancel()
		// The key doesn't exist until the first successful update, so treat a
		// missing key as 0. The CompareAndSwap below creates it.
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			lastRead, err = 0, nil
		}
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := kv.CompareAndSwap(ctx, CounterKey, lastRead, lastRead+remainingDelta, true)
			cancel()
			if err == nil {
				remainingDelta = 0
			}
		}
		if remainingDelta > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	deltas := make(chan int, 100)

	go syncDeltas(deltas, kv)

	n.Handle("add", func(msg maelstrom.Message) error {
		var add Add
		if err := json.Unmarshal(msg.Body, &add); err != nil {
			return err
		}
		deltas <- add.Delta

		resp := map[string]any{
			"type": "add_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		value, err := kv.ReadInt(ctx, CounterKey)
		cancel()
		if err != nil {
			return err
		}

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
