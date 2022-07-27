package payload

import (
	"encoding/json"
	"time"
)

const (
	TimeLayoutNano = "2006-01-02 15:04:05.999999999"
)

/*
特别注意:
	因为存在时间问题, 需要注意不同服务器之间的时区, 和时间差.
	需要对所有的服务器进行一个时间的同步校准, 否则会有较大的偏差
*/

// DpPayload 延迟队列数据
type DpPayload struct {
	Deadline     string `json:"deadline"`      // 最后期限
	Payload      string `json:"payload"`       // 真实负载
	ProcessTopic string `json:"process_topic"` // 到期之后放入的队列
}

func LoadPayload(payload []byte) (res DpPayload, err error) {
	err = json.Unmarshal(payload, &res)
	return
}

// WaitTime 获取需要等待的时间.
// 大于0需要等待
// 小于或者等于0, 不需要等待
func (p DpPayload) WaitTime() (time.Duration, error) {
	deadline, err := time.ParseInLocation(TimeLayoutNano, p.Deadline, time.Local)
	if err != nil {
		return 0, err
	}
	return deadline.Sub(time.Now()), nil
}

// GetProcessTopic 获取实际的处理 Topic
func (p DpPayload) GetProcessTopic() string {
	return p.ProcessTopic
}

// GetPayload 获得延迟消息的有效负载
func (p DpPayload) GetPayload() string {
	return p.Payload
}

func (p DpPayload) Encode() ([]byte, error) {
	return json.Marshal(p)
}

func (p DpPayload) Length() int {
	data, _ := json.Marshal(p)
	return len(data)
}
