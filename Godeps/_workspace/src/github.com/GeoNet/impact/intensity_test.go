package impact

import (
	"testing"
)

type TestIntensities struct {
	v float64
	i int32
}

// taken from standard shakemap plots .... (converted to m/s)
var TestIntensitySlice = []TestIntensities{
	// original tests
	{0.01 / 100.0, 1},
	{0.09 / 100.0, 2},
	{1.9 / 100.0, 5},
	{5.8 / 100.0, 6},
	{11.0 / 100.0, 7},
	{22.0 / 100.0, 8},
	{43.0 / 100.0, 8},
	{83.0 / 100.0, 9},
	{161.0 / 100.0, 10},
	// bounds tests
	{0.0, 1},
	{-1.0, 1},
	{1.0e+10, 12},
}

func TestIntensity(t *testing.T) {

	for i := range TestIntensitySlice {
		if Intensity(TestIntensitySlice[i].v) == TestIntensitySlice[i].i {
			continue
		}
		t.Errorf("invalid rawintensity [%g cm/s]: %d (calculated) != %d (expected)", 100.0*TestIntensitySlice[i].v, Intensity(TestIntensitySlice[i].v), TestIntensitySlice[i].i)
	}
}
