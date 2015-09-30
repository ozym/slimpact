package main

import (
	"encoding/json"
	"flag"
	//	"fmt"
	"github.com/GeoNet/mseed"
	"github.com/GeoNet/slink"
	//	"math"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
	"log"
	"net"
	"net/http"
	"os"
	//"strings"
	"time"

	"github.com/ozym/impact"

	"expvar"
)

var (
	awsMessages = expvar.NewInt("aws_messages_rate")
)

func main() {
	var Q *sqs.Queue

	// runtime settings
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "make noise")
	var dryrun bool
	flag.BoolVar(&dryrun, "dry-run", false, "don't actually send the messages")

	// streaming channel information
	var config string
	flag.StringVar(&config, "config", "impact.json", "provide a streams config file")

	var fdsn string
	flag.StringVar(&fdsn, "fdsn", "service.geonet.org.nz", "provide fdsn station config server")
	var reload time.Duration
	flag.DurationVar(&reload, "reload", time.Minute, "how long to wait between fdsn station queries")

	// amazon queue details
	var region string
	flag.StringVar(&region, "region", "", "provide AWS region, overides env variable \"AWS_REGION\"")
	var queue string
	flag.StringVar(&queue, "queue", "", "send messages to the SQS queue, overides env variable \"AWS_QUEUE\"")
	var key string
	flag.StringVar(&key, "key", "", "AWS access key id, overrides env and credentials file (default profile)")
	var secret string
	flag.StringVar(&secret, "secret", "", "AWS secret key id, overrides env and credentials file (default profile)")

	// seedlink options
	var netdly int
	flag.IntVar(&netdly, "netdly", 0, "provide network delay")
	var netto int
	flag.IntVar(&netto, "netto", 300, "provide network timeout")
	var keepalive int
	flag.IntVar(&keepalive, "keepalive", 0, "provide keep-alive")
	var selectors string
	flag.StringVar(&selectors, "selectors", "???", "provide channel selectors")
	var streams string
	flag.StringVar(&streams, "streams", "*_*", "provide streams")

	// heartbeat flush interval
	var flush time.Duration
	flag.DurationVar(&flush, "flush", 300.0*time.Second, "how often to send heartbeat messages")

	// noisy channel detection
	var probation time.Duration
	flag.DurationVar(&probation, "probation", 10.0*time.Minute, "noise probation window")
	var level int
	flag.IntVar(&level, "level", 2, "noise threshold level")

	// problem sending messages
	var resends int
	flag.IntVar(&resends, "resends", 6, "how many times to try and resend a message")
	var wait time.Duration
	flag.DurationVar(&wait, "wait", 5*time.Second, "how long to wait between message resends")

	flag.Parse()

	if !dryrun {
		if region == "" {
			region = os.Getenv("AWS_IMPACT_REGION")
			if region == "" {
				log.Fatalf("unable to find region in environment or command line [AWS_IMPACT_REGION]")
			}
		}

		if queue == "" {
			queue = os.Getenv("AWS_IMPACT_QUEUE")
			if queue == "" {
				log.Fatalf("unable to find queue in environment or command line [AWS_IMPACT_QUEUE]")
			}
		}

		// configure amazon ...
		R := aws.GetRegion(region)
		// fall through to env then credentials file
		A, err := aws.GetAuth(key, secret, "", time.Now().Add(30*time.Minute))
		if err != nil {
			log.Fatalf("unable to get amazon auth: %s\n", err)
		}

		S := sqs.New(A, R)
		Q, err = S.GetQueue(queue)
		if err != nil {
			log.Fatalf("unable to get amazon queue: %s [%s/%s]\n", err, queue, region)
		}
	}

	// for internal instrumentation
	sock, err := net.Listen("tcp", "localhost:8123")
	if err != nil {
		panic(err)
	}
	go func() {
		http.Serve(sock, nil)
	}()

	// who to call ...
	server := "localhost:18000"
	if flag.NArg() > 0 {
		server = flag.Arg(0)
	}

	// initial seedlink handle
	slconn := slink.NewSLCD()
	defer slink.FreeSLCD(slconn)

	// seedlink settings
	slconn.SetNetDly(netdly)
	slconn.SetNetTo(netto)
	slconn.SetKeepAlive(keepalive)

	// conection
	slconn.SetSLAddr(server)
	defer slconn.Disconnect()

	// configure streams selectors to recover
	slconn.ParseStreamList(streams, selectors)

	// make space for miniseed blocks
	msr := mseed.NewMSRecord()
	defer mseed.FreeMSRecord(msr)

	state := NewStreams(int32(level), probation)
	if err := state.Load(fdsn, time.Now()); err != nil {
		log.Fatal(err)
	}
	next := time.Now().Add(reload)

	// output channel
	result := make(chan impact.Message)
	go func() {
		for m := range result {
			mm, err := json.Marshal(m)
			if err != nil {
				log.Printf("unable to marshal message: %s\n", err)
				continue
			}
			if verbose {
				log.Println(string(mm))
			}
			if !dryrun {
				for n := 0; n < resends; n++ {
					_, err := Q.SendMessage(string(mm))
					if err != nil {
						log.Printf("unable to send message [#%d/%d]: %s\n", n+1, resends, err)
						log.Printf("sleeping %s\n", wait)
						time.Sleep(wait)
					}
					break
				}
			}
			awsMessages.Add(1)
			go func() {
				time.Sleep(flush)
				awsMessages.Add(-1)
			}()
		}
	}()

	for {
		// recover packet ...
		p, rc := slconn.Collect()
		if rc != slink.SLPACKET {
			break
		}
		// just in case we're shutting down
		if p.PacketType() != slink.SLDATA {
			continue
		}

		// time to check for new configurations ....
		if time.Now().After(next) {
			if err := state.Load(fdsn, time.Now()); err != nil {
				log.Fatal(err)
			}
			next = time.Now().Add(reload)
		}

		// decode miniseed block
		buf := p.GetMSRecord()
		msr.Unpack(buf, 512, 1, 0)

		msg, err := state.Process(msr, flush)
		if err != nil {
			log.Println(err)
			continue
		}
		if msg != nil {
			result <- *msg
		}
	}
}
