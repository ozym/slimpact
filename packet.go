package main

import (
	"expvar"
	"fmt"
	"strconv"
	"time"

	"github.com/GeoNet/mseed"
	"github.com/ozym/impact"
)

var (
	mmiEstimation = expvar.NewMap("mmi_estimations_per_minute")
	packetRate    = expvar.NewInt("slink_packets_per_minute")
)

func (s *Streams) Process(msr *mseed.MSRecord, flush time.Duration) (*impact.Message, error) {
	// find the expected stream key
	srcname := msr.SrcName(0)

	// check the rate is greater than zero ...
	rate := msr.Samprate()
	if !(rate > 0.0) {
		//return nil, fmt.Errorf("%s: non positive sampling rate", srcname)
		return nil, nil
	}

	stream := s.Find(srcname)
	if stream == nil {
		return nil, nil
		//return nil, fmt.Errorf("%s: no stream configured", srcname)
	}

	// recover amplitude samples
	samples, err := msr.DataSamples()
	if err != nil {
		return nil, fmt.Errorf("%s: data sample problem! %s", srcname, err)
	}

	packetRate.Add(1)
	go func() {
		time.Sleep(time.Minute)
		packetRate.Add(-1)
	}()

	// check whether the filters need resetting
	if stream.Gap(msr.Starttime(), float64(rate)) {
		stream.Reset(samples)
	}
	// remember the last sample time
	stream.Last = msr.Endtime()

	// recover intensity information
	message := stream.Message(stream.Intensity(msr.Starttime(), samples))
	mmiEstimation.Add(strconv.Itoa(int(message.MMI)), 1)
	go func(mmi int) {
		time.Sleep(time.Minute)
		mmiEstimation.Add(strconv.Itoa(mmi), -1)
	}(int(message.MMI))

	// should the message be sent
	if stream.Flush(flush, message.MMI) {
		return &message, nil
	}

	return nil, nil
}
