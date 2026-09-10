package main

import (
	"math"
	"math/bits"
)

// Logarithmic histogram with 64 subdivisions per power of two. Percentiles
// return bucket upper bounds (<= ~1.563% relative bucket width above 64 ns).
// Fixed memory: no latency sample reservoir and no silent sample eviction.
type Histogram struct {
	Bins    [4096]uint64 `json:"bins"`
	Count   uint64       `json:"count"`
	Maximum uint64       `json:"maximum_ns"`
}

func (h *Histogram) Add(n uint64) {
	if n == 0 {
		n = 1
	}
	exp := bits.Len64(n)
	if exp >= 64 {
		exp = 63
		n = (uint64(1) << 63) - 1
	}
	base := uint64(1) << (exp - 1)
	width := base >> 6
	if width == 0 {
		width = 1
	}
	offset := (n - base) / width
	if offset > 63 {
		offset = 63
	}
	h.Bins[exp*64+int(offset)]++
	h.Count++
	if n > h.Maximum {
		h.Maximum = n
	}
}
func (h *Histogram) Merge(other Histogram) {
	for i, n := range other.Bins {
		h.Bins[i] += n
	}
	h.Count += other.Count
	if other.Maximum > h.Maximum {
		h.Maximum = other.Maximum
	}
}
func (h Histogram) Percentile(p float64) uint64 {
	if h.Count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(float64(h.Count) * p))
	if rank < 1 {
		rank = 1
	}
	var count uint64
	for i, n := range h.Bins {
		count += n
		if count >= rank {
			exp, offset := i/64, i%64
			if exp == 0 {
				return 1
			}
			base := uint64(1) << (exp - 1)
			width := base >> 6
			if width == 0 {
				width = 1
			}
			upper := base + uint64(offset+1)*width - 1
			if upper > h.Maximum {
				upper = h.Maximum
			}
			return upper
		}
	}
	return h.Maximum
}
func (h Histogram) Summary() map[string]any {
	return map[string]any{"count": h.Count, "p50_ms": float64(h.Percentile(.50)) / 1e6,
		"p95_ms": float64(h.Percentile(.95)) / 1e6, "p99_ms": float64(h.Percentile(.99)) / 1e6,
		"p999_ms": float64(h.Percentile(.999)) / 1e6, "max_ms": float64(h.Maximum) / 1e6,
		"quantization": "64 subdivisions per power of two; upper-bound percentiles"}
}
