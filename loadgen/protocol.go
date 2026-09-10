package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const maxFrame = 8 * 1024 * 1024

type Command struct {
	Client   string  `json:"client"`
	Sequence uint64  `json:"sequence"`
	Kind     string  `json:"kind"`
	Key      string  `json:"key"`
	Value    string  `json:"value"`
	Expected *string `json:"expected"`
}
type Outcome struct {
	Value   *string `json:"value"`
	Swapped *bool   `json:"swapped"`
	Error   *string `json:"error"`
}
type Request struct {
	Op      string   `json:"op"`
	Command *Command `json:"command,omitempty"`
}
type Reply struct {
	Status string   `json:"status"`
	Result *Outcome `json:"result"`
	Leader uint64   `json:"leader_id"`
	Detail string   `json:"detail"`
}
type Client struct {
	nodes  map[uint64]string
	ids    []uint64
	conns  map[uint64]net.Conn
	leader *atomic.Uint64
}

func newClient(nodes map[uint64]string, leader *atomic.Uint64) *Client {
	ids := make([]uint64, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &Client{nodes: nodes, ids: ids, conns: make(map[uint64]net.Conn), leader: leader}
}
func writeFrame(w io.Writer, data []byte) error {
	if len(data) > maxFrame {
		return fmt.Errorf("frame exceeds limit")
	}
	b := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(b[:4], uint32(len(data)))
	copy(b[4:], data)
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func readFrame(r io.Reader) ([]byte, error) {
	h := make([]byte, 4)
	if _, e := io.ReadFull(r, h); e != nil {
		return nil, e
	}
	n := binary.BigEndian.Uint32(h)
	if n > maxFrame {
		return nil, fmt.Errorf("oversized response: %d", n)
	}
	b := make([]byte, int(n))
	_, e := io.ReadFull(r, b)
	return b, e
}
func (c *Client) close() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}
func (c *Client) call(id uint64, request []byte, deadline time.Time) (Reply, error) {
	var result Reply
	conn := c.conns[id]
	if conn == nil {
		address, ok := c.nodes[id]
		if !ok {
			return result, fmt.Errorf("unknown node")
		}
		// A failed endpoint must not consume the entire operation deadline.
		dialTimeout := time.Until(deadline)
		if dialTimeout > 200*time.Millisecond {
			dialTimeout = 200 * time.Millisecond
		}
		if dialTimeout <= 0 {
			return result, fmt.Errorf("deadline")
		}
		var e error
		conn, e = net.DialTimeout("tcp", address, dialTimeout)
		if e != nil {
			return result, e
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetNoDelay(true)
		}
		c.conns[id] = conn
	}
	_ = conn.SetDeadline(deadline)
	e := writeFrame(conn, request)
	if e == nil {
		var data []byte
		data, e = readFrame(conn)
		if e == nil {
			e = json.Unmarshal(data, &result)
		}
	}
	if e != nil {
		_ = conn.Close()
		delete(c.conns, id)
	}
	return result, e
}
func (c *Client) execute(command Command, deadline time.Time) (Reply, int) {
	payload, e := json.Marshal(Request{Op: "execute", Command: &command})
	if e != nil {
		return Reply{Status: "error", Detail: e.Error()}, 0
	}
	target := c.leader.Load()
	if _, ok := c.nodes[target]; !ok {
		target = c.ids[0]
	}
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		// A lost reply consumes at most 500 ms before a same-identity retry.
		hopDeadline := time.Now().Add(500 * time.Millisecond)
		if deadline.Before(hopDeadline) {
			hopDeadline = deadline
		}
		reply, err := c.call(target, payload, hopDeadline)
		if err == nil && reply.Status == "ok" && reply.Result != nil {
			c.leader.Store(target)
			return reply, attempts
		}
		if err == nil && reply.Status == "error" {
			return reply, attempts
		}
		if err == nil && reply.Leader != 0 && reply.Leader != target {
			if _, ok := c.nodes[reply.Leader]; ok {
				target = reply.Leader
				continue
			}
		}
		for i, id := range c.ids {
			if id == target {
				target = c.ids[(i+1)%len(c.ids)]
				break
			}
		}
		remaining := time.Until(deadline)
		if remaining > 5*time.Millisecond {
			time.Sleep(5 * time.Millisecond)
		}
	}
	return Reply{Status: "unknown", Detail: "operation deadline; may have committed"}, attempts
}
func parseNodes(s string) (map[uint64]string, error) {
	nodes := map[uint64]string{}
	for _, part := range strings.Split(s, ",") {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("nodes use id=host:port")
		}
		id, e := strconv.ParseUint(pair[0], 10, 64)
		if e != nil || id == 0 {
			return nil, fmt.Errorf("invalid node ID")
		}
		if _, ok := nodes[id]; ok {
			return nil, fmt.Errorf("duplicate node ID")
		}
		if _, _, e = net.SplitHostPort(pair[1]); e != nil {
			return nil, e
		}
		nodes[id] = pair[1]
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes")
	}
	return nodes, nil
}
