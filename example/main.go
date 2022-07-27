package main

import (
	"context"
	"delayqueue/consummer"
	"delayqueue/producer"
	"sync"
	"time"
)

// testDelayQueue Latency to be concerned about
var testDelayQueue = []time.Duration{
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
	10 * time.Minute,
	20 * time.Minute,
	30 * time.Minute,
	time.Hour,
	3 * time.Hour,
	6 * time.Hour,
}

var testNet = []string{"192.168.31.202:9092"}

func main() {
	dpConsumer, err := consummer.NewController("testDp", testNet, testDelayQueue)
	if err != nil {
		panic(err)
	}
	err = dpConsumer.Run(context.Background())
	if err != nil {
		panic(err)
	}

	dpProducer, err := producer.NewDQProducer("testDp", testDelayQueue, testNet)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				for _, delayTime := range testDelayQueue {
					_ = dpProducer.Send(delayTime, "hell World", "testTopic")
				}
			}
		}()
	}

	time.Sleep(24 * time.Hour)
	dpConsumer.Close()
	_ = dpProducer.Close()
}
