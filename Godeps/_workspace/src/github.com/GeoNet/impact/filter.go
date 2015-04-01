package impact

import (
	"math"
)

type HighPass struct {
	a, b float64 // filter coeffs
	x, y float64 // previous input & output
}

func NewHighPass(gain float64, q float64) *HighPass {
	f := new(HighPass)

	f.a = (1.0 + q) / (2.0 * gain)
	f.b = q

	f.x = 0.0
	f.y = math.NaN()

	return f
}

func (f *HighPass) Reset() {
	f.y = math.NaN()
}

func (f *HighPass) Set(y float64) {
	f.y = y
}

func (f *HighPass) Sample(x float64) float64 {
	var y float64

	if math.IsNaN(f.y) {
		f.x = x
		f.y = 0.0
	}

	y = (f.a*(x-f.x) + f.b*f.y)

	f.x = x
	f.y = y

	return y
}

type Integrator struct {
	a, b float64 // filter coeffs
	x, y float64 // previous input & output
}

func NewIntegrator(gain float64, dt float64, q float64) *Integrator {
	f := new(Integrator)

	f.a = (1.0 + q) * dt / (4.0 * gain)
	f.b = q

	f.x = 0.0
	f.y = math.NaN()

	return f
}

func (f *Integrator) Reset() {
	f.y = math.NaN()
}

func (f *Integrator) Set(y float64) {
	f.y = y
}

func (f *Integrator) Sample(x float64) float64 {
	var y float64

	if math.IsNaN(f.y) {
		f.x = x
		f.y = 0.0
	}

	y = f.a*(x+f.x) + f.b*f.y

	f.x = x
	f.y = y

	return y
}
