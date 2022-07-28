package consummer

import (
	"context"
	"github.com/QbCheng/delayqueue/logger"
	"sync"
	"time"
)

type Options func(*Controller)

type option struct {
	logger   logger.Logger
	timezone string
}

// WithLogger Setting the Log Interface
func WithLogger(logger logger.Logger) Options {
	return func(rm *Controller) {
		rm.option.logger = logger
	}
}

// WithTimezone Set the timezone
func WithTimezone(timezone string) Options {
	return func(rm *Controller) {
		rm.option.timezone = timezone
	}
}

const DQConsumerClientId = "delayQueue-consumer"

type Controller struct {
	name      string
	processor map[time.Duration]*Processor

	addr  []string
	close context.CancelFunc

	wg     *sync.WaitGroup
	option option
}

// NewController Create a controller
func NewController(name string, addr []string, delayTime []time.Duration, options ...Options) (*Controller, error) {
	ret := &Controller{}
	ret.name = name
	ret.addr = addr
	ret.wg = &sync.WaitGroup{}
	ret.processor = map[time.Duration]*Processor{}

	ret.option = option{
		logger:   logger.NewDefaultLog(),
		timezone: "UTC", // 默认是标准时区
	}

	for i := range options {
		options[i](ret)
	}

	var err error

	time.Local, err = time.LoadLocation(ret.option.timezone)
	if err != nil {
		return nil, err
	}

	for _, v := range delayTime {
		ret.processor[v] = nil
	}

	return ret, nil
}

// Log Controller Logs
func (c *Controller) Log(layout string, data ...interface{}) {
	c.option.logger.Print("Controller : "+layout, data...)
}

// Run Start consuming delayed tasks
func (c *Controller) Run(ctx context.Context) error {
	var child context.Context
	child, c.close = context.WithCancel(ctx)

	for delayTime := range c.processor {
		var err error
		c.processor[delayTime], err = NewProcessor(c.addr, c.name, delayTime, c.option.logger)
		if err != nil {
			return err
		}
		c.wg.Add(1)
		go c.processor[delayTime].Run(child, c.wg)
	}
	return nil
}

// Close safety shutdown
func (c *Controller) Close() bool {
	// 关闭
	c.close()
	c.wg.Done()
	return true
}
