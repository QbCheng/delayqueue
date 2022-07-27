package consummer

import (
	"context"
	"delayqueue/logger"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestNewProcessor(t *testing.T) {
	var err error
	time.Local, err = time.LoadLocation("UTC")

	consumer, err := NewProcessor(testNet, "testDp", time.Minute, logger.NewDefaultLog())
	assert.NoError(t, err)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go consumer.Run(ctx, wg)
	time.Sleep(3 * time.Minute)
	cancel()
	wg.Wait()
}
