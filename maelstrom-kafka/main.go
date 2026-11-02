package main

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Send struct {
	MessageType string `json:"type"`
	Key         string `json:"key"`
	Msg         int    `json:"msg"`
}

type SendOK struct {
	Offset int `json:"offset"`
}

type Poll struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

type PollOK struct {
	Msgs map[string][][2]int `json:"msgs"`
}

type CommitOffsets struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

type ListCommittedOffsets struct {
	MessageType string   `json:"type"`
	Keys        []string `json:"keys"`
}

type ListCommittedOffsetsOK struct {
	Offsets map[string]int `json:"offsets"`
}

// Each key is owned by exactly one node, which keeps that key's log and
// committed offset in memory. Clients can talk to any node; requests for
// keys owned by another node are forwarded to it with the owner_* messages,
// which take the same bodies as the client messages but only ever contain
// keys the receiving node owns.
type Log struct {
	CommittedOffset *int
	Messages        []int
}

type State struct {
	sync.RWMutex
	Logs map[string]*Log
}

func NewState() *State {
	return &State{
		Logs: make(map[string]*Log),
	}
}

func (s *State) send(key string, message int) int {
	s.Lock()
	defer s.Unlock()
	kafkaLog, ok := s.Logs[key]
	if !ok {
		kafkaLog = &Log{}
		s.Logs[key] = kafkaLog
	}
	kafkaLog.Messages = append(kafkaLog.Messages, message)
	return len(kafkaLog.Messages) - 1
}

func (s *State) poll(offsets map[string]int) map[string][][2]int {
	s.RLock()
	defer s.RUnlock()
	msgs := map[string][][2]int{}
	for key, start := range offsets {
		kafkaLog, ok := s.Logs[key]
		if !ok {
			continue
		}
		for offset := max(start, 0); offset < len(kafkaLog.Messages); offset++ {
			msgs[key] = append(msgs[key], [2]int{offset, kafkaLog.Messages[offset]})
		}
	}
	return msgs
}

func (s *State) commitOffsets(offsets map[string]int) {
	s.Lock()
	defer s.Unlock()
	for key, offset := range offsets {
		kafkaLog, ok := s.Logs[key]
		if !ok {
			kafkaLog = &Log{}
			s.Logs[key] = kafkaLog
		}
		committed := offset
		kafkaLog.CommittedOffset = &committed
	}
}

// listCommittedOffsets leaves out keys that have never been committed.
func (s *State) listCommittedOffsets(keys []string) map[string]int {
	s.RLock()
	defer s.RUnlock()
	offsets := map[string]int{}
	for _, key := range keys {
		kafkaLog, ok := s.Logs[key]
		if !ok || kafkaLog.CommittedOffset == nil {
			continue
		}
		offsets[key] = *kafkaLog.CommittedOffset
	}
	return offsets
}

const rpcTimeout = time.Second

// owner returns the node responsible for key. Every node computes the same
// answer from the shared node list.
func owner(n *maelstrom.Node, key string) string {
	nodeIDs := slices.Sorted(slices.Values(n.NodeIDs()))
	h := fnv.New32a()
	h.Write([]byte(key))
	return nodeIDs[h.Sum32()%uint32(len(nodeIDs))]
}

func groupOffsetsByOwner(n *maelstrom.Node, offsets map[string]int) map[string]map[string]int {
	groups := map[string]map[string]int{}
	for key, offset := range offsets {
		keyOwner := owner(n, key)
		if groups[keyOwner] == nil {
			groups[keyOwner] = map[string]int{}
		}
		groups[keyOwner][key] = offset
	}
	return groups
}

func groupKeysByOwner(n *maelstrom.Node, keys []string) map[string][]string {
	groups := map[string][]string{}
	for _, key := range keys {
		keyOwner := owner(n, key)
		groups[keyOwner] = append(groups[keyOwner], key)
	}
	return groups
}

// scatter calls fn for every owner concurrently, waits for all of them, and
// returns one of the errors if any failed.
func scatter[T any](groups map[string]T, fn func(owner string, group T) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(groups))
	for groupOwner, group := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(groupOwner, group); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	return <-errs
}

// call sends body to dest and decodes the reply into resp.
func call(n *maelstrom.Node, dest string, body any, resp any) error {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	msg, err := n.SyncRPC(ctx, dest, body)
	if err != nil {
		return err
	}
	return json.Unmarshal(msg.Body, resp)
}

func main() {
	n := maelstrom.NewNode()
	state := NewState()

	// Client-facing handlers. A failed forward of a send or commit means the
	// owner may or may not have applied it, so the error is returned rather
	// than retried; Maelstrom treats the resulting crash reply as
	// indeterminate.

	n.Handle("send", func(msg maelstrom.Message) error {
		var send Send
		if err := json.Unmarshal(msg.Body, &send); err != nil {
			return err
		}

		var offset int
		if keyOwner := owner(n, send.Key); keyOwner == n.ID() {
			offset = state.send(send.Key, send.Msg)
		} else {
			send.MessageType = "owner_send"
			var sendOK SendOK
			if err := call(n, keyOwner, send, &sendOK); err != nil {
				return err
			}
			offset = sendOK.Offset
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

		var mu sync.Mutex
		msgs := map[string][][2]int{}
		err := scatter(groupOffsetsByOwner(n, poll.Offsets), func(groupOwner string, offsets map[string]int) error {
			var part map[string][][2]int
			if groupOwner == n.ID() {
				part = state.poll(offsets)
			} else {
				var pollOK PollOK
				body := Poll{MessageType: "owner_poll", Offsets: offsets}
				if err := call(n, groupOwner, body, &pollOK); err != nil {
					return err
				}
				part = pollOK.Msgs
			}
			mu.Lock()
			defer mu.Unlock()
			for key, keyMsgs := range part {
				msgs[key] = keyMsgs
			}
			return nil
		})
		if err != nil {
			return err
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

		err := scatter(groupOffsetsByOwner(n, commitOffsets.Offsets), func(groupOwner string, offsets map[string]int) error {
			if groupOwner == n.ID() {
				state.commitOffsets(offsets)
				return nil
			}
			body := CommitOffsets{MessageType: "owner_commit_offsets", Offsets: offsets}
			return call(n, groupOwner, body, &struct{}{})
		})
		if err != nil {
			return err
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

		var mu sync.Mutex
		offsets := map[string]int{}
		err := scatter(groupKeysByOwner(n, listCommittedOffsets.Keys), func(groupOwner string, keys []string) error {
			var part map[string]int
			if groupOwner == n.ID() {
				part = state.listCommittedOffsets(keys)
			} else {
				var listOK ListCommittedOffsetsOK
				body := ListCommittedOffsets{MessageType: "owner_list_committed_offsets", Keys: keys}
				if err := call(n, groupOwner, body, &listOK); err != nil {
					return err
				}
				part = listOK.Offsets
			}
			mu.Lock()
			defer mu.Unlock()
			for key, offset := range part {
				offsets[key] = offset
			}
			return nil
		})
		if err != nil {
			return err
		}

		resp := map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		}
		return n.Reply(msg, resp)
	})

	// Internal handlers, only sent by other nodes and only for keys this
	// node owns.

	n.Handle("owner_send", func(msg maelstrom.Message) error {
		var send Send
		if err := json.Unmarshal(msg.Body, &send); err != nil {
			return err
		}
		resp := map[string]any{
			"type":   "owner_send_ok",
			"offset": state.send(send.Key, send.Msg),
		}
		return n.Reply(msg, resp)
	})

	n.Handle("owner_poll", func(msg maelstrom.Message) error {
		var poll Poll
		if err := json.Unmarshal(msg.Body, &poll); err != nil {
			return err
		}
		resp := map[string]any{
			"type": "owner_poll_ok",
			"msgs": state.poll(poll.Offsets),
		}
		return n.Reply(msg, resp)
	})

	n.Handle("owner_commit_offsets", func(msg maelstrom.Message) error {
		var commitOffsets CommitOffsets
		if err := json.Unmarshal(msg.Body, &commitOffsets); err != nil {
			return err
		}
		state.commitOffsets(commitOffsets.Offsets)
		resp := map[string]any{
			"type": "owner_commit_offsets_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("owner_list_committed_offsets", func(msg maelstrom.Message) error {
		var listCommittedOffsets ListCommittedOffsets
		if err := json.Unmarshal(msg.Body, &listCommittedOffsets); err != nil {
			return err
		}
		resp := map[string]any{
			"type":    "owner_list_committed_offsets_ok",
			"offsets": state.listCommittedOffsets(listCommittedOffsets.Keys),
		}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
