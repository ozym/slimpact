package impact

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"math"
	"regexp"
	"time"
)

// decode channel names
const (
	ACCELERATION string = `^[A-Z0-9\_]+_[A-Z]N[A-Z0-9]$`
	VELOCITY     string = `^[A-Z0-9\_]+_[A-Z]H[A-Z0-9]$`
)

// running stream state information
type Stream struct {
	Name      string  // station name
	Latitude  float32 // station latitude
	Longitude float32 // station longitude
	Rate      float64 // stream sampling rate
	Gain      float64 // stream gain
	Q         float64 // high-pass filter coeff

	h *HighPass   // high-pass filter
	i *Integrator // intergrator

	mmi   int32     // the last intesity sent
	flush time.Time // previous flush
	last  time.Time // previous packet

	level     int32         // the noise threshold level
	probation time.Duration // the noise probation period

	jailed bool      // it's been too noisy
	good   time.Time // the last good data time
	bad    time.Time // the last bad data time
}

// pull in public stream information from a json config file
func LoadStreams(config string) map[string]*Stream {
	f, err := ioutil.ReadFile(config)
	if err != nil {
		log.Printf("Could not load config file: \"%s\"\n", config)
		log.Fatal(err)
	}

	var s map[string]*Stream
	err = json.Unmarshal(f, &s)
	if err != nil {
		log.Printf("Could not parse config file: \"%s\"\n", config)
		log.Fatal(err)
	}

	return s
}

// initialize a stream, setting type of input and filters
func (s *Stream) Init(srcname string, probation time.Duration, level int32) (bool, error) {

	s.probation = probation
	s.level = level

	s.h = nil
	s.i = nil

	// update structure and filters
	if regexp.MustCompile(VELOCITY).MatchString(srcname) {
		if s.Q > 0.0 {
			s.h = NewHighPass(s.Gain, s.Q)
		}
	} else if regexp.MustCompile(ACCELERATION).MatchString(srcname) {
		if s.Q > 0.0 {
			s.h = NewHighPass(s.Gain, s.Q)
			s.i = NewIntegrator(1.0, 1.0/s.Rate, s.Q)
		}
	} else {
		return false, errors.New("unable to match srcname for velocity or acceleration")
	}

	return true, nil
}

// time to send a message, either timeout or different value
func (s *Stream) Flush(d time.Duration, mmi int32) bool {

	// same intensity?
	if s.mmi == mmi {
		// ignore times
		if d == 0 {
			return false
		}
		// too soon?
		if time.Since(s.flush).Seconds() < d.Seconds() {
			return false
		}
	}

	// keep state
	s.flush = time.Now()
	s.mmi = mmi

	// a noisy stream
	if s.mmi > s.level {
		// should be jailed ...
		if s.last.Sub(s.good) > s.probation {
			s.jailed = true
		}
		s.bad = s.last
	} else {
		if s.last.Sub(s.bad) > s.probation {
			s.jailed = false
		}
		s.good = s.last
	}

	// skip as noisy
	if s.jailed {
		return false
	}

	return true
}

// given an array of samples .. pass them through a block at a time
func (s *Stream) ProcessSamples(source string, srcname string, starttime time.Time, samples []int32) (Message, error) {

	// resulting possible message
	m := Message{Source: source, Quality: "measured", Latitude: s.Latitude, Longitude: s.Longitude, Comment: s.Name}

	// need a sampling rate
	if !(s.Rate > 0.0) {
		return m, errors.New("invalid sampling rate")
	}
	// check we have samples
	if !(len(samples) > 0) {
		return m, errors.New("no samples given")
	}
	// need high pass filter at least
	if s.i != nil && s.h == nil {
		return m, errors.New("filter not fully initialised")
	}

	// has there been a break?
	if math.Abs(starttime.Sub(s.last).Seconds()-1.0/s.Rate) > (0.5 / s.Rate) {
		log.Printf("[%s] reset stream: %s\n", srcname, starttime)

		// reset filters
		if s.h != nil {
			s.h.Reset()
		}
		if s.i != nil {
			s.i.Reset()
		}

		// first run it backwards (a pre-conditioning strategy)
		for i := range samples {
			if s.i != nil {
				s.h.Sample(s.i.Sample((float64)(samples[len(samples)-i-1])))
			} else if s.h != nil {
				s.h.Sample((float64)(samples[len(samples)-i-1]))
			}
		}

		// reset the noise times
		s.bad = time.Unix(0, 0)
		s.good = time.Unix(0, 0)
	}

	// reset time
	m.Time = starttime
	m.MMI = Intensity(0)

	// find max velocity
	var max float64 = 0.0
	for i := range samples {
		var f float64
		if s.i != nil {
			f = s.h.Sample(s.i.Sample((float64)(samples[i])))
		} else if s.h != nil {
			f = s.h.Sample((float64)(samples[i]))
		} else {
			f = (float64)(samples[i]) / s.Gain
		}

		if math.Abs(f) > max {
			max = math.Abs(f)
			m.Time = starttime.Add((time.Duration)((float64)(time.Second) * (float64)(i) / s.Rate))
			m.MMI = Intensity(max)
		}
	}

	// get ready for next packet
	s.last = starttime.Add((time.Duration)((float64)(time.Second) * (float64)(len(samples)-1) / s.Rate))

	return m, nil
}
