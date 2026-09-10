package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
)

func TestHistogramBounds(t *testing.T) {
	for n := uint64(1); n < 100000000000; n = n + n/10 + 1 {
		var h Histogram
		h.Add(n)
		if p := h.Percentile(.99); p != n {
			t.Fatalf("single observation %d != %d", p, n)
		}
	}
	var h Histogram
	for n := uint64(1); n <= 10000; n++ {
		h.Add(n)
	}
	p := h.Percentile(.99)
	if p < 9900 || p > 10000 {
		t.Fatal(p)
	}
	if h.Count != 10000 || h.Maximum != 10000 {
		t.Fatal(h)
	}
}
func TestHistogramMerge(t *testing.T) {
	var a, b Histogram
	a.Add(100)
	b.Add(200)
	a.Merge(b)
	if a.Count != 2 || a.Percentile(1) != 200 {
		t.Fatal(a)
	}
}
func TestFrameRoundTrip(t *testing.T) {
	var b bytes.Buffer
	if e := writeFrame(&b, []byte("hello")); e != nil {
		t.Fatal(e)
	}
	v, e := readFrame(&b)
	if e != nil || string(v) != "hello" {
		t.Fatal(v, e)
	}
}
func TestRejectLargeFrame(t *testing.T) {
	h := make([]byte, 4)
	binary.BigEndian.PutUint32(h, math.MaxUint32)
	if _, e := readFrame(bytes.NewReader(h)); e == nil {
		t.Fatal("accepted oversize")
	}
}
func TestTruncatedFrame(t *testing.T) {
	if _, e := readFrame(bytes.NewReader([]byte{0, 0, 0, 5, 'x'})); e != io.ErrUnexpectedEOF {
		t.Fatal(e)
	}
}
func TestNodeParser(t *testing.T) {
	if _, e := parseNodes("1=127.0.0.1:1,1=127.0.0.1:2"); e == nil {
		t.Fatal("duplicate accepted")
	}
	if n, e := parseNodes("1=[::1]:100"); e != nil || len(n) != 1 {
		t.Fatal(n, e)
	}
}
