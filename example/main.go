package main

import (
	"context"
	"delayqueue/consummer"
	"delayqueue/producer"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
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

	handle := RealTopicHandle{}
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go handle.Run(ctx, []string{"testTopic"}, wg)

	time.Sleep(24 * time.Hour)
	dpConsumer.Close()
	cancel()
	_ = dpProducer.Close()
}

type RealTopicHandle struct {
}

func (h RealTopicHandle) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h RealTopicHandle) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h RealTopicHandle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *RealTopicHandle) Run(ctx context.Context, topics []string, wg *sync.WaitGroup) {
	defer wg.Done()
	log.SetFlags(log.Lshortfile | log.Ltime | log.Ldate)
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 如果启用，成功传递的消息将在成功通道返回(默认禁用).
	config.Producer.Return.Successes = true

	group, err := sarama.NewConsumerGroup(testNet, "RealTopic-group", config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err = range group.Errors() {
			log.Print("err : ", err.Error())
		}
	}()

	for {
		err = group.Consume(ctx, topics, h)
		if err == nil {
			return
		} else {
			fmt.Println(err)
		}
	}
}
