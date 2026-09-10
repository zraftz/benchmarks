// Native load generator. No third-party modules. Full histories are opt-in
// qualification evidence; normal measurement uses fixed-size histograms.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	nodes, output, history, session, namespace              string
	duration, timeout                                       time.Duration
	concurrency, payload, keyspace, readPercent, casPercent int
	rate                                                    float64
	seed                                                    int64
	operations                                              uint64
}
type Event struct {
	Command     Command `json:"command"`
	ScheduledNS int64   `json:"scheduled_ns"`
	StartNS     int64   `json:"start_ns"`
	EndNS       int64   `json:"end_ns"`
	Reply       Reply   `json:"reply"`
	Attempts    int     `json:"attempts"`
}
type Second struct {
	OK        uint64 `json:"ok"`
	Unknown   uint64 `json:"unknown"`
	Errors    uint64 `json:"errors"`
	MaximumNS uint64 `json:"maximum_ns"`
}
type stats struct {
	Attempted, OK, InWindow, Unknown, Errors, Attempts uint64
	Latency, AllLatency, Lateness                      Histogram
	Seconds                                            map[int]*Second
	Events                                             []Event
}

func (s *stats) record(e Event, window time.Duration, history bool) {
	s.Attempted++
	s.Attempts += uint64(e.Attempts)
	latency := uint64(max64(e.EndNS-e.ScheduledNS, 1))
	s.AllLatency.Add(latency)
	s.Lateness.Add(uint64(max64(e.StartNS-e.ScheduledNS, 1)))
	sec := int(e.EndNS / int64(time.Second))
	if s.Seconds[sec] == nil {
		s.Seconds[sec] = &Second{}
	}
	if e.Reply.Status == "ok" && e.Reply.Result != nil && e.Reply.Result.Error == nil {
		s.OK++
		if e.EndNS <= int64(window) {
			s.InWindow++
		}
		s.Latency.Add(latency)
		s.Seconds[sec].OK++
		if latency > s.Seconds[sec].MaximumNS {
			s.Seconds[sec].MaximumNS = latency
		}
	} else if e.Reply.Status == "unknown" {
		s.Unknown++
		s.Seconds[sec].Unknown++
	} else {
		s.Errors++
		s.Seconds[sec].Errors++
	}
	if history {
		s.Events = append(s.Events, e)
	}
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func run(o options, nodes map[uint64]string) (map[string]any, []Event, error) {
	start := time.Now()
	end := start.Add(o.duration)
	var leader atomic.Uint64
	var ordinal atomic.Uint64
	results := make(chan *stats, o.concurrency)
	var workers sync.WaitGroup
	jobs := make(chan time.Time, o.concurrency)
	work := func(worker int) {
		defer workers.Done()
		c := newClient(nodes, &leader)
		defer c.close()
		rng := rand.New(rand.NewSource(o.seed + int64(worker)*1000003))
		sequence := uint64(0)
		s := stats{Seconds: map[int]*Second{}}
		do := func(scheduled time.Time) {
			sequence++
			kind := "put"
			choice := rng.Intn(100)
			if choice < o.readPercent {
				kind = "get"
			} else if choice < o.readPercent+o.casPercent {
				kind = "cas"
			}
			key := fmt.Sprintf("%s/k%d", o.namespace, rng.Intn(o.keyspace))
			value := fmt.Sprintf("%08x:%016x:%s/", worker, sequence, o.session)
			if len(value) > o.payload {
				value = value[:o.payload]
			} else {
				value += strings.Repeat("x", o.payload-len(value))
			}
			// CAS is deliberately create-if-absent (expected=null), not a hidden read-modify-write pair.
			command := Command{Client: fmt.Sprintf("%s/%d", o.session, worker), Sequence: sequence, Kind: kind, Key: key, Value: value}
			actual := time.Now()
			reply, attempts := c.execute(command, actual.Add(o.timeout))
			event := Event{Command: command, ScheduledNS: scheduled.Sub(start).Nanoseconds(), StartNS: actual.Sub(start).Nanoseconds(), EndNS: time.Since(start).Nanoseconds(), Reply: reply, Attempts: attempts}
			s.record(event, o.duration, o.history != "")
		}
		if o.rate > 0 {
			for scheduled := range jobs {
				do(scheduled)
			}
		} else {
			for time.Now().Before(end) {
				n := ordinal.Add(1)
				if o.operations > 0 && n > o.operations {
					break
				}
				do(time.Now())
			}
		}
		results <- &s
	}
	for i := 0; i < o.concurrency; i++ {
		workers.Add(1)
		go work(i)
	}
	var offered, dropped uint64
	var dispatchLateness Histogram
	if o.rate > 0 {
		for i := uint64(0); ; i++ {
			scheduled := start.Add(time.Duration(float64(i) * float64(time.Second) / o.rate))
			if !scheduled.Before(end) || (o.operations > 0 && i >= o.operations) {
				break
			}
			if delay := time.Until(scheduled); delay > 0 {
				time.Sleep(delay)
			}
			offered++
			late := time.Since(scheduled)
			dispatchLateness.Add(uint64(max64(int64(late), 1)))
			// Already-overdue arrivals remain offered work. Do not pause the clock
			// and turn an open-loop test into a deceptively healthy closed loop.
			select {
			case jobs <- scheduled:
			default:
				dropped++
			}
		}
		close(jobs)
	}
	workers.Wait()
	close(results)
	wall := time.Since(start)
	measure := o.duration
	if o.operations > 0 && wall < measure {
		measure = wall
	}
	total := stats{Seconds: map[int]*Second{}}
	events := []Event{}
	for s := range results {
		total.Attempted += s.Attempted
		total.OK += s.OK
		total.InWindow += s.InWindow
		total.Unknown += s.Unknown
		total.Errors += s.Errors
		total.Attempts += s.Attempts
		total.Latency.Merge(s.Latency)
		total.AllLatency.Merge(s.AllLatency)
		total.Lateness.Merge(s.Lateness)
		for sec, v := range s.Seconds {
			if total.Seconds[sec] == nil {
				total.Seconds[sec] = &Second{}
			}
			t := total.Seconds[sec]
			t.OK += v.OK
			t.Unknown += v.Unknown
			t.Errors += v.Errors
			if v.MaximumNS > t.MaximumNS {
				t.MaximumNS = v.MaximumNS
			}
		}
		events = append(events, s.Events...)
	}
	if o.rate == 0 {
		offered = total.Attempted
	}
	if o.operations > 0 && wall < o.duration {
		total.InWindow = total.OK
	}
	if offered != total.Attempted+dropped {
		return nil, nil, fmt.Errorf("accounting invariant failed")
	}
	seconds := []map[string]any{}
	indexes := []int{}
	for sec := range total.Seconds {
		indexes = append(indexes, sec)
	}
	sort.Ints(indexes)
	for _, sec := range indexes {
		v := total.Seconds[sec]
		seconds = append(seconds, map[string]any{"second": sec, "ok": v.OK, "unknown": v.Unknown, "errors": v.Errors, "max_latency_ms": float64(v.MaximumNS) / 1e6})
	}
	result := map[string]any{"schema": 1, "kind": "networked-kv", "contract": "durable-log+durable-application-v1/logged-reads",
		"generator":     map[string]any{"go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "logical_cpus": runtime.NumCPU(), "history_enabled": o.history != ""},
		"config":        map[string]any{"nodes": nodes, "concurrency": o.concurrency, "rate": o.rate, "duration_seconds": o.duration.Seconds(), "timeout_seconds": o.timeout.Seconds(), "payload_bytes": o.payload, "keyspace": o.keyspace, "read_percent": o.readPercent, "cas_percent": o.casPercent, "seed": o.seed, "session": o.session, "namespace": o.namespace, "operation_limit": o.operations},
		"start_unix_ns": start.UnixNano(), "measurement_seconds": measure.Seconds(), "wall_seconds": wall.Seconds(), "offered": offered, "attempted": total.Attempted, "not_issued": dropped, "ok": total.OK, "completed_in_window": total.InWindow, "unknown": total.Unknown, "errors": total.Errors, "network_attempts": total.Attempts,
		"successful_ops_per_second": float64(total.InWindow) / measure.Seconds(), "success_latency": total.Latency.Summary(), "all_dispatched_latency": total.AllLatency.Summary(), "worker_start_lateness": total.Lateness.Summary(), "scheduler_lateness": dispatchLateness.Summary(),
		"success_histogram": total.Latency, "all_histogram": total.AllLatency, "seconds": seconds}
	return result, events, nil
}
func main() {
	o := options{}
	flag.StringVar(&o.nodes, "nodes", "", "comma-separated id=host:port")
	flag.StringVar(&o.output, "output", "result.json", "result path")
	flag.StringVar(&o.history, "history", "", "optional full operation history (qualification only)")
	flag.StringVar(&o.session, "session", "", "unique client-session prefix (required)")
	flag.StringVar(&o.namespace, "namespace", "bench", "key namespace")
	flag.DurationVar(&o.duration, "duration", 60*time.Second, "offered-load interval")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Second, "whole operation timeout, including retries")
	flag.IntVar(&o.concurrency, "concurrency", 64, "workers, each one request at a time")
	flag.IntVar(&o.payload, "payload", 512, "value bytes")
	flag.IntVar(&o.keyspace, "keyspace", 10000, "key count")
	flag.IntVar(&o.readPercent, "read-percent", 0, "logged reads, percent")
	flag.IntVar(&o.casPercent, "cas-percent", 0, "create-if-absent CAS, percent")
	flag.Float64Var(&o.rate, "rate", 0, "scheduled operations/s; 0 selects closed loop")
	flag.Int64Var(&o.seed, "seed", 1, "workload seed")
	flag.Uint64Var(&o.operations, "operations", 0, "optional operation cap, useful for qualification")
	flag.Parse()
	if o.session == "" || len(o.session) > 100 || o.duration <= 0 || o.duration > 24*time.Hour || o.timeout <= 0 || o.timeout > time.Minute || o.concurrency < 1 || o.concurrency > 1024 || o.payload < 1 || o.payload > 65536 || o.keyspace < 1 || math.IsNaN(o.rate) || math.IsInf(o.rate, 0) || len(o.namespace) > 200 || o.rate < 0 || o.rate > 1e7 || o.readPercent < 0 || o.casPercent < 0 || o.readPercent+o.casPercent > 100 {
		fatal(fmt.Errorf("invalid options"))
	}
	nodes, e := parseNodes(o.nodes)
	if e != nil {
		fatal(e)
	}
	result, events, e := run(o, nodes)
	if e != nil {
		fatal(e)
	}
	if o.history != "" {
		sort.Slice(events, func(i, j int) bool { return events[i].StartNS < events[j].StartNS })
		f, e := os.OpenFile(o.history, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if e != nil {
			fatal(e)
		}
		w := bufio.NewWriter(f)
		enc := json.NewEncoder(w)
		for _, event := range events {
			if e = enc.Encode(event); e != nil {
				fatal(e)
			}
		}
		if e = w.Flush(); e != nil {
			fatal(e)
		}
		if e = f.Sync(); e != nil {
			fatal(e)
		}
		if e = f.Close(); e != nil {
			fatal(e)
		}
	}
	data, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		fatal(e)
	}
	f, e := os.OpenFile(o.output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		fatal(e)
	}
	if _, e = f.Write(append(data, '\n')); e != nil {
		fatal(e)
	}
	if e = f.Sync(); e != nil {
		fatal(e)
	}
	if e = f.Close(); e != nil {
		fatal(e)
	}
	fmt.Printf("ok=%v unknown=%v not_issued=%v successful_ops/s=%.1f\n", result["ok"], result["unknown"], result["not_issued"], result["successful_ops_per_second"])
}
func fatal(e error) { fmt.Fprintln(os.Stderr, e); os.Exit(1) }
