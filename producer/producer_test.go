package producer

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

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

func TestNewDQProducer(t *testing.T) {

	// 创建一个1分钟的延迟主题
	producer, err := NewDQProducer("testDp", testDelayQueue, []string{"192.168.31.202:9092"})
	assert.NoError(t, err)
	wg := &sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err = producer.Send(10*time.Minute, []byte("hell World"), "testTopic")
				err = producer.Send(60*time.Second, []byte("hell World"), "testTopic")
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()
}
