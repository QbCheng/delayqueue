package producer

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"kafka-study/delayqueue/logger"
	dpPayload "kafka-study/delayqueue/payload"
	"time"
)

const DQProducerClientId = "delayQueue-producer"

var ErrRegisteredDelayQueueNotEmpty = errors.New("The registered delay queue cannot be empty ")
var ErrDelayTimeNotRegister = errors.New("The delay is not registered. Check whether the delay is sent by mistake ")

type Options func(*DQProducer)

type option struct {
	logger   logger.Logger
	timezone string
}

// WithLogger 日志
func WithLogger(logger logger.Logger) Options {
	return func(rm *DQProducer) {
		rm.option.logger = logger
	}
}

// WithTimezone 设置时区
func WithTimezone(timezone string) Options {
	return func(rm *DQProducer) {
		rm.option.timezone = timezone
	}
}

type DQProducer struct {
	delayTime     time.Duration // 延迟的时间
	name          string
	registerTopic map[time.Duration]string // 注册的主题
	sarama.SyncProducer
	option option
}

// NewDQProducer 创建一个生产者
func NewDQProducer(name string, registerDelay []time.Duration, addr []string, options ...Options) (*DQProducer, error) {
	if len(registerDelay) == 0 {
		return nil, ErrRegisteredDelayQueueNotEmpty
	}
	ret := &DQProducer{}
	ret.name = name
	ret.registerTopic = map[time.Duration]string{}
	for _, v := range registerDelay {
		ret.registerTopic[v] = fmt.Sprintf("%s-delay-second-%d", name, v/time.Second)
	}

	ret.option = option{
		logger:   nil,
		timezone: "UTC", // 默认是标准时区
	}

	for i := range options {
		options[i](ret)
	}

	var err error
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.ClientID = DQProducerClientId
	config.Producer.RequiredAcks = sarama.WaitForAll // 等待所有的确认, 防止消息丢失
	config.Producer.Return.Successes = true
	ret.SyncProducer, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		return nil, err
	}

	// 初始化时区
	time.Local, err = time.LoadLocation(ret.option.timezone)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// Send 发送一条延时消息
// 注意.
// 延迟时间必须是已经注册的.
// 注册延迟时间需要使用 库中的消费者指定的一致. 否则将不会被消费
func (s *DQProducer) Send(delayTime time.Duration, payload string, processTopic string) error {
	topic := s.RegisterTopic(delayTime)
	if len(topic) == 0 {
		return ErrDelayTimeNotRegister
	}
	msg := &sarama.ProducerMessage{Topic: topic, Value: dpPayload.DpPayload{
		Deadline:     time.Now().Add(s.delayTime).Format(dpPayload.TimeLayoutNano),
		Payload:      payload,
		ProcessTopic: processTopic,
	}}
	_, _, err := s.SendMessage(msg)
	if err != nil {
		return err
	}
	return nil
}

// RegisterTopic 获取注册的延时消息的主题.
func (s *DQProducer) RegisterTopic(delayTime time.Duration) string {
	if topic, ok := s.registerTopic[delayTime]; ok {
		return topic
	}
	return ""
}
