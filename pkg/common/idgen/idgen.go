package idgen

import (
	"sync"
	"time"
)

type IDGenerator interface {
	NextID() uint64
}

type TimestampIDGenerator struct {
	mu            sync.Mutex
	lastTimestamp int64
}

func NewTimestampIDGenerator() *TimestampIDGenerator {
	return &TimestampIDGenerator{}
}

func (g *TimestampIDGenerator) NextID() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 获取当前时间戳
	timestamp := time.Now().UnixNano()

	// 确保生成的ID是单调递增的
	if timestamp <= g.lastTimestamp {
		timestamp = g.lastTimestamp + 1
	}

	g.lastTimestamp = timestamp
	return uint64(timestamp)
}

var defaultGenerator IDGenerator = NewTimestampIDGenerator()

func SetDefaultGenerator(generator IDGenerator) {
	defaultGenerator = generator
}

func NextID() uint64 {
	return defaultGenerator.NextID()
}
