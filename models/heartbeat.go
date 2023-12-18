package models

// 主节点发送的心跳包
type Heartbeat struct {
	Priority  int   `json:"priority"`
	Timestamp int64 `json:"timestamp"`
}
