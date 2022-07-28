package consummer

import (
	"context"
	"errors"
	"fmt"
	"github.com/QbCheng/delayqueue/logger"
	dpPayload "github.com/QbCheng/delayqueue/payload"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

// DelayResetErr Delay, reset session
var DelayResetErr = errors.New("Delayed resetting session. ")

// Processor processor
type Processor struct {
	cg         sarama.ConsumerGroup // Consumer group for delayed messages.
	delayTopic string               // Delayed Queue Topic
	cgName     string               // Consumer group name

	waitTime      time.Duration // Waiting time
	waitPartition int32         // Partitions waiting to be restored
	log           logger.Logger // Log

	forward sarama.SyncProducer // Producer of delivery

	ctx    context.Context
	closed bool
}

// NewProcessor create a Processor
func NewProcessor(adds []string, name string, delayTime time.Duration, log logger.Logger) (*Processor, error) {
	ret := &Processor{}
	ret.delayTopic = fmt.Sprintf("%s-delay-second-%d", name, delayTime/time.Second)
	ret.cgName = fmt.Sprintf("%s-consumer-group", ret.delayTopic)
	ret.log = log

	var err error
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.ClientID = DQConsumerClientId

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	ret.cg, err = sarama.NewConsumerGroup(adds, ret.cgName, config)
	if err != nil {
		return nil, err
	}
	ret.forward, err = sarama.NewSyncProducer(adds, config)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// Log Processor Logs
func (h *Processor) Log(layout string, data ...interface{}) {
	h.log.Print("Processor : "+layout, data...)
}

// delay Start delay
func (h *Processor) delay() {
	// 暂停所有分期的消费
	h.cg.Pause(map[string][]int32{
		h.delayTopic: {h.waitPartition},
	})
}

// waitRecovery Waiting for the delay to end
func (h *Processor) waitRecovery() {
	timer := time.NewTimer(h.waitTime)
	select {
	case <-timer.C:
		h.cg.Resume(map[string][]int32{
			h.delayTopic: {h.waitPartition},
		})
		h.waitTime = 0
		h.waitPartition = 0
	case <-h.ctx.Done():
		// 外部关闭
		h.closed = true
	}
}

func (h *Processor) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Processor) Cleanup(_ sarama.ConsumerGroupSession) error {
	if h.waitTime > 0 {
		return DelayResetErr
	}
	return nil
}

func (h *Processor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload, err := dpPayload.LoadPayload(msg.Value)
		if err != nil {
			// Abnormal data that cannot be resolved. Confirm directly.
			h.Log("Payload Cannot be resolved. {data : %s, err : %v}", msg.Value, err)
			session.MarkMessage(msg, "Payload Cannot be resolved")
			continue
		}
		waitTime, err := payload.WaitTime()
		if err != nil {
			// Abnormal data that cannot be resolved. Confirm directly.
			h.Log("The time format of payload is abnormal.. {data : %s, err : %v}", msg.Value, err)
			session.MarkMessage(msg, "The time format of payload is abnormal.")
			continue
		}
		if waitTime > 0 {
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "forward failure")
			// Set the waiting time
			h.waitTime = waitTime
			h.waitPartition = msg.Partition
			return nil
		} else {
			forwardMsg := &sarama.ProducerMessage{Topic: payload.GetProcessTopic(), Value: sarama.StringEncoder(payload.GetPayload())}
			_, _, err = h.forward.SendMessage(forwardMsg)
			if err != nil {
				session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "forward failure")
				continue
			}
			session.MarkMessage(msg, "done")
		}
	}
	return nil
}

// Run Processor start
// Repeat startup is not allowed.
func (h *Processor) Run(ctx context.Context, wg *sync.WaitGroup) {
	h.ctx = ctx
	defer func() {
		err := h.cg.Close()
		if err != nil {
			h.Log("Consumer shutdown failure : %v", err)
		}
		err = h.forward.Close()
		if err != nil {
			h.Log("Producer shutdown failure : %v", err)
		}
		h.Log("%s : success close.", h.delayTopic)
		wg.Done()
	}()

	// Track errors
	go func() {
		for err := range h.cg.Errors() {
			if !errors.Is(err, DelayResetErr) {
				h.Log("Consumption failed. {err : %v}", err)
			}
		}
	}()

	for {
		err := h.cg.Consume(ctx, []string{h.delayTopic}, h)
		if err == nil {
			h.Log("Safety shutdown")
			return
		} else if errors.Is(err, DelayResetErr) {
			h.Log("Reset session")
			h.delay()
			h.waitRecovery()
			if h.closed {
				return
			}
		} else {
			h.Log("err : %v", err)
		}
	}
}
