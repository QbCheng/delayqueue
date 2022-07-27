package consummer

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var testNet = []string{"192.168.31.202:9092"}

/*
需要启动的队列
*/
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

func TestController(t *testing.T) {
	dpQueue, err := NewController("testDp", testNet, testDelayQueue)
	assert.NoError(t, err)
	err = dpQueue.Run(context.Background())
	assert.NoError(t, err)
	time.Sleep(time.Minute * 3)
	dpQueue.Close()
}
