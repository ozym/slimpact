package main

import (
	"expvar"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ozym/impact"
)

var Q = map[int]float64{200: 0.98829, 100: 0.97671, 50: 0.95395}

const RFC3339Z = "2006-01-02T15:04:05"

var (
	fdsnURL      = expvar.NewString("fdsn_url")
	fdsnStreams  = expvar.NewInt("fdsn_streams")
	totalStreams = expvar.NewInt("total_streams")
)

type Streams struct {
	Map       map[string]*Stream
	Level     int32
	Probation time.Duration
}

func NewStreams(level int32, probation time.Duration) Streams {
	return Streams{
		Map:       make(map[string]*Stream),
		Level:     level,
		Probation: probation,
	}
}

func (s *Streams) Load(fdsn string, whence time.Time) error {

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

	var count int64
	defer func() { fdsnStreams.Set(count) }()
	defer func() { totalStreams.Set(int64(len(s.Map))) }()

	fdsnURL.Set(u.String())
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
			Level:     s.Level,
			Probation: s.Probation,
		}

		srcname := strings.Join(parts[0:4], "_")
		if _, ok := s.Map[srcname]; !ok {
			s.Map[srcname] = stream
		}
		if !s.Map[srcname].Equal(stream) {
			s.Map[srcname] = stream
		}
		count++
	}

	return nil
}

func (s *Streams) Find(srcname string) *Stream {
	if _, ok := s.Map[srcname]; !ok {
		return nil
	}
	return s.Map[srcname]
}
