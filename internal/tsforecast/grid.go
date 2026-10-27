package tsforecast

import (
	"fmt"
	"math"
	"sort"

	"github.com/akkio-inc/igloo/internal/models"
)

// Grid is a series laid out on regular steps.
type Grid struct {
	Freq  models.Frequency
	Start int64
	// Values holds each step's value, NaN where there is none.
	Values []float32
	// Points counts the steps with a value.
	Points int
}

// End is the timestamp of the step after the last.
func (g Grid) End() int64 { return g.Freq.At(g.Start, len(g.Values)) }

// At is step k's timestamp.
func (g Grid) At(k int) int64 { return g.Freq.At(g.Start, k) }

// Layout lays points out on a grid of freq's steps, or of the most common
// gap between their timestamps: each step the mean of its points, NaN
// where it has none. Points whose value is NaN count for the timestamps
// alone.
func Layout(ts []int64, ys []float64, freq *models.Frequency) (Grid, error) {
	type point struct {
		t int64
		y float64
	}
	pts := make([]point, 0, len(ts))
	for i, t := range ts {
		pts = append(pts, point{t, ys[i]})
	}
	if len(pts) == 0 {
		return Grid{}, fmt.Errorf("%w: the series has no points", ErrInput)
	}
	sort.SliceStable(pts, func(a, b int) bool { return pts[a].t < pts[b].t })
	g := Grid{Start: pts[0].t}
	if freq != nil && (freq.Micros > 0 || freq.Months > 0) {
		g.Freq = *freq
	} else {
		var distinct []int64
		for _, p := range pts {
			if len(distinct) == 0 || distinct[len(distinct)-1] != p.t {
				distinct = append(distinct, p.t)
			}
		}
		f, err := models.InferFrequency([][]int64{distinct})
		if err != nil {
			return Grid{}, fmt.Errorf("%w: %v", ErrInput, err)
		}
		g.Freq = f
	}
	n := g.Freq.Index(g.Start, pts[len(pts)-1].t) + 1
	if n > 20*len(pts)+1000 {
		return Grid{}, fmt.Errorf("%w: the series' timestamps are too sparse for a step of %s; give the frequency", ErrInput, g.Freq.Name)
	}
	sums := make([]float64, n)
	counts := make([]int, n)
	for _, p := range pts {
		if math.IsNaN(p.y) {
			continue
		}
		k := g.Freq.Index(g.Start, p.t)
		sums[k] += p.y
		counts[k]++
	}
	g.Values = make([]float32, n)
	for k := range g.Values {
		if counts[k] == 0 {
			g.Values[k] = float32(math.NaN())
			continue
		}
		g.Values[k] = float32(sums[k] / float64(counts[k]))
		g.Points++
	}
	return g, nil
}
