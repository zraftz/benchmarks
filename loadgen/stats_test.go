package main

import (
	"testing"
	"time"
)

func TestLatencySeparatesWaitingFromExecution(t *testing.T) {
	s := stats{Seconds: map[int]*Second{}}
	s.record(Event{ScheduledNS: 0, StartNS: 80_000_000, EndNS: 100_000_000,
		Attempts: 1, Reply: Reply{Status: "ok", Result: &Outcome{}}}, time.Second, false)
	if s.Latency.Maximum != 100_000_000 || s.Lateness.Maximum != 80_000_000 || s.Execution.Maximum != 20_000_000 {
		t.Fatal("scheduled latency, waiting, and execution must retain their own intervals")
	}
	s.record(Event{ScheduledNS: 0, StartNS: 5_000_000, EndNS: 200_000_000,
		Attempts: 2, Reply: Reply{Status: "unknown"}}, time.Second, false)
	if s.Execution.Count != 1 || s.AllExecution.Count != 2 || s.AllExecution.Maximum != 195_000_000 {
		t.Fatal("failed/unknown execution belongs in the all-dispatched distribution only")
	}
}
