package payload

import (
	"encoding/json"
	"time"
)

const (
	TimeLayoutNano = "2006-01-02 15:04:05.999999999"
)

// DpPayload Delay queue payload
type DpPayload struct {
	Deadline     string `json:"deadline"`      // 最后期限
	Payload      string `json:"payload"`       // 真实负载
	ProcessTopic string `json:"process_topic"` // 到期之后放入的队列
}

func LoadPayload(payload []byte) (res DpPayload, err error) {
	err = json.Unmarshal(payload, &res)
	return
}

// WaitTime Get the time to wait.
// > 0 need to wait
// <= 0 no need to wait
func (p DpPayload) WaitTime() (time.Duration, error) {
	deadline, err := time.ParseInLocation(TimeLayoutNano, p.Deadline, time.Local)
	if err != nil {
		return 0, err
	}
	return deadline.Sub(time.Now()), nil
}

// GetProcessTopic Get the actual processing Topic
func (p DpPayload) GetProcessTopic() string {
	return p.ProcessTopic
}

// GetPayload Get payload of delayed message
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
