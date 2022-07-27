package consummer

import (
	"context"
	"delayqueue/logger"
	dpPayload "delayqueue/payload"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

/*
todo 同步生产者生产确认非常慢.
*/

// DelayResetErr 自定义错误.  不是错误. 延迟时重置 Session
var DelayResetErr = errors.New("Delayed resetting session. ")

var RepeatedStartErr = errors.New("The processor has been started. ")

// Processor 处理器
type Processor struct {
	cg         sarama.ConsumerGroup // 延迟消息的消费者组.
	delayTopic string               // 延迟队列的主题
	cgName     string               // 消费者组名

	waitTime      time.Duration // 等待时间
	waitPartition int32         // 等待恢复的分区
	log           logger.Logger // 日志

	forward sarama.SyncProducer // 交付的生产者

	ctx    context.Context
	closed bool
}

// NewProcessor 创建一个处理器
func NewProcessor(adds []string, name string, delayTime time.Duration, log logger.Logger) (*Processor, error) {
	ret := &Processor{}
	ret.delayTopic = fmt.Sprintf("%s-delay-second-%d", name, delayTime/time.Second)
	ret.cgName = fmt.Sprintf("%s-consumer-group", ret.delayTopic)
	ret.log = log

	// 创建 kafka 消费者
	// 创建 kafka 消费者
	var err error
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.ClientID = DQConsumerClientId

	// 消费者配置
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 生产者配置
	config.Producer.RequiredAcks = sarama.WaitForAll // 等待所有的确认, 防止消息丢失
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

// Log 日志
func (h *Processor) Log(layout string, data ...interface{}) {
	h.log.Print("Processor : "+layout, data...)
}

// delay 延迟
func (h *Processor) delay() {
	// 暂停所有分期的消费
	h.cg.Pause(map[string][]int32{
		h.delayTopic: {h.waitPartition},
	})
}

// recovery 等待恢复 kafka 抓取
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
		h.Log("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		payload, err := dpPayload.LoadPayload(msg.Value)
		if err != nil {
			// 无法解析的异常数据. 直接确认.
			h.Log("Payload Cannot be resolved. {data : %s, err : %v}", msg.Value, err)
			session.MarkMessage(msg, "Payload Cannot be resolved")
			continue
		}
		waitTime, err := payload.WaitTime()
		if err != nil {
			// 无法解析的异常数据. 直接确认.
			h.Log("The time format of payload is abnormal.. {data : %s, err : %v}", msg.Value, err)
			session.MarkMessage(msg, "The time format of payload is abnormal.")
			continue
		}
		if waitTime > 0 {
			// 回退
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "forward failure")
			// 设置等待时间
			h.waitTime = waitTime
			h.waitPartition = msg.Partition
			return nil
		} else {
			// 直接处理
			forwardMsg := &sarama.ProducerMessage{Topic: payload.GetProcessTopic(), Value: sarama.StringEncoder(payload.GetPayload())}
			_, _, err = h.forward.SendMessage(forwardMsg)
			if err != nil {
				// 发送失败
				session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "forward failure")
				continue
			}
			session.MarkMessage(msg, "done")
		}
	}
	return nil
}

// Run 启动
// 不允许重复启动. 重复启动, 会导致 先启动的消费者组停止消费
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
