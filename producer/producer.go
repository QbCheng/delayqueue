package producer

import (
	"delayqueue/logger"
	dpPayload "delayqueue/payload"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
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

// WithLogger Setting the Log Interface
func WithLogger(logger logger.Logger) Options {
	return func(rm *DQProducer) {
		rm.option.logger = logger
	}
}

// WithTimezone Set the timezone
func WithTimezone(timezone string) Options {
	return func(rm *DQProducer) {
		rm.option.timezone = timezone
	}
}

type DQProducer struct {
	name          string
	registerTopic map[time.Duration]string // registered topics
	sarama.SyncProducer
	option option
}

// NewDQProducer create a producer
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
		timezone: "UTC",
	}

	for i := range options {
		options[i](ret)
	}

	var err error
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.ClientID = DQProducerClientId
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	ret.SyncProducer, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		return nil, err
	}

	time.Local, err = time.LoadLocation(ret.option.timezone)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// Send a delay message
// note.
// 		The delay time must be registered.
// 		Registered delay messages must be consumed.
func (s *DQProducer) Send(delayTime time.Duration, payload string, processTopic string) error {
	topic := s.RegisterTopic(delayTime)
	if len(topic) == 0 {
		return ErrDelayTimeNotRegister
	}
	msg := &sarama.ProducerMessage{Topic: topic, Value: dpPayload.DpPayload{
		Deadline:     time.Now().Add(delayTime).Format(dpPayload.TimeLayoutNano),
		Payload:      payload,
		ProcessTopic: processTopic,
	}}
	_, _, err := s.SendMessage(msg)
	if err != nil {
		return err
	}
	return nil
}

// RegisterTopic Get topic by delay time.
func (s *DQProducer) RegisterTopic(delayTime time.Duration) string {
	if topic, ok := s.registerTopic[delayTime]; ok {
		return topic
	}
	return ""
}
