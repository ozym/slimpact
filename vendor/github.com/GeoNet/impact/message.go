package impact

import (
	"time"
)

type Message struct {
	Source    string    `json:"source"`
	Quality   string    `json:"quality"`
	Latitude  float32   `json:"latitude"`
	Longitude float32   `json:"longitude"`
	Time      time.Time `json:"time"`
	MMI       int32     `json:"MMI"`
	Comment   string    `json:"comment"`
}
