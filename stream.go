package main

import (
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ozym/impact"
)

var Q = map[int]float64{200: 0.98829, 100: 0.97671, 50: 0.95395}

const RFC3339Z = "2006-01-02T15:04:05"

type Stream struct {
	// stream identification
	Network  string
	Station  string
	Location string
	Channel  string

	// station location
	Latitude  float64
	Longitude float64

	// stream configuration
	Rate  float64 // sample rate in samples/second
	Units string  // sensor units (i.e. M/S)
	Gain  float64 // stream sensitivity in counts per unit

	// sample processing ...
	Filter impact.Filter

	// current status
	MMI    int32
	Update time.Time

	// noise analysis
	Level     int32
	Probation time.Duration
	Good      time.Time
	Bad       time.Time
	Jailed    bool

	// time of last sample processed
	Last time.Time
}

type Streams struct {
	Map map[string]*Stream
}

func NewStreams() Streams {
	return Streams{Map: make(map[string]*Stream)}
}

func (s *Streams) Load(fdsn string, level int32, probation time.Duration, whence time.Time) error {

	v := url.Values{}
	v.Set("level", "channel")
	v.Set("format", "text")
	v.Set("endafter", whence.UTC().Format(RFC3339Z))
	v.Set("startbefore", whence.UTC().Format(RFC3339Z))

	u := url.URL{
		Host:     fdsn,
		Scheme:   "http",
		Path:     "/fdsnws/station/1/query",
		RawQuery: v.Encode(),
	}

	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "|")
		if !(len(parts) > 16) {
			continue
		}

		lat, err := strconv.ParseFloat(parts[4], 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(parts[5], 64)
		if err != nil {
			return err
		}

		rate, err := strconv.ParseFloat(parts[14], 64)
		if err != nil {
			return err
		}
		gain, err := strconv.ParseFloat(parts[11], 64)
		if err != nil {
			return err
		}

		q, ok := Q[int(rate)]
		if !ok {
			continue
		}
		units := parts[13]

		var filter impact.Filter
		switch units {
		case "M/S**2":
			filter = impact.NewAcceleration(gain, 1.0/rate, q)
		case "M/S":
			filter = impact.NewVelocity(gain, q)
		default:
			continue
		}

		stream := &Stream{
			Network:   parts[0],
			Station:   parts[1],
			Location:  parts[2],
			Channel:   parts[3],
			Latitude:  lat,
			Longitude: lon,
			Filter:    filter,
			Units:     units,
			Gain:      gain,
			Rate:      rate,
			Level:     level,
			Probation: probation,
		}

		srcname := strings.Join(parts[0:4], "_")
		if _, ok := s.Map[srcname]; !ok {
			s.Map[srcname] = stream
		}
		if !s.Map[srcname].Equal(stream) {
			s.Map[srcname] = stream
		}
	}

	return nil
}

func (s *Streams) Find(srcname string) *Stream {
	if _, ok := s.Map[srcname]; !ok {
		return nil
	}
	return s.Map[srcname]
}

func (s *Stream) Source() string {
	return strings.Join([]string{s.Network, s.Station}, "_")
}

func (s *Stream) Key() string {
	return strings.Join([]string{s.Network, s.Station, s.Location, s.Channel}, "_")
}

func (s *Stream) Equal(stream *Stream) bool {
	if s.Key() != stream.Key() {
		return false
	}
	if s.Latitude != stream.Latitude {
		return false
	}
	if s.Longitude != stream.Longitude {
		return false
	}
	if s.Units != stream.Units {
		return false
	}
	if s.Gain != stream.Gain {
		return false
	}
	if s.Rate != stream.Rate {
		return false
	}
	return true
}

func (s *Stream) Interval(length float64) time.Duration {
	return (time.Duration)(float64(time.Second) * length / s.Rate)
}

func (s *Stream) Gap(first time.Time, rate float64) bool {
	var delta float64
	if rate > 0 {
		delta = 1.0 / rate
	}
	switch {
	case s.Last.IsZero():
		return true
	case math.Abs(first.Sub(s.Last).Seconds()-delta) > (0.5 * delta):
		return true
	default:
		return false
	}
}

func (s *Stream) Reset(samples []int32) {
	// reset the filter
	s.Filter.Reset()

	// first run it backwards (a pre-conditioning strategy)
	for i := range samples {
		s.Filter.Sample((float64)(samples[len(samples)-i-1]))
	}

	// reset the noise times
	s.Bad = time.Unix(0, 0)
	s.Good = time.Unix(0, 0)
}

func (s *Stream) Intensity(first time.Time, samples []int32) (time.Time, int32) {
	var max float64

	t := first
	for i := range samples {
		f := s.Filter.Sample((float64)(samples[i]))
		if math.Abs(f) > max {
			max = math.Abs(f)
			t = first.Add(s.Interval(float64(i)))
		}
	}

	return t, impact.Intensity(max)
}

func (s *Stream) Message(at time.Time, mmi int32) impact.Message {
	return impact.Message{
		Source:    s.Source(),
		Quality:   "measured",
		Latitude:  float32(s.Latitude),
		Longitude: float32(s.Longitude),
		Time:      at,
		MMI:       mmi,
		Comment:   s.Key(),
	}
}

// time to send a message, either timeout or different value
func (s *Stream) Flush(d time.Duration, mmi int32) bool {

	// same intensity?
	if s.MMI == mmi {
		// ignore times
		if d == 0 {
			return false
		}
		// too soon?
		if time.Since(s.Update).Seconds() < d.Seconds() {
			return false
		}
	}

	// keep state
	s.Update = time.Now()
	s.MMI = mmi

	// a noisy stream
	if s.MMI > s.Level {
		// should be jailed ...
		if s.Last.Sub(s.Good) > s.Probation {
			s.Jailed = true
		}
		s.Bad = s.Last
	} else {
		if s.Last.Sub(s.Bad) > s.Probation {
			s.Jailed = false
		}
		s.Good = s.Last
	}

	// skip as noisy
	if s.Jailed {
		return false
	}

	return true
}