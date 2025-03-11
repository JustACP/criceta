package utils

import (
	"encoding/hex"
	"hash/crc32"

	"lukechampine.com/blake3"
)

// UpdateCRC32 使用IEEE多项式更新CRC32校验和
func UpdateCRC32(crc uint32, data []byte) uint32 {
	newCRC := crc
	return crc32.Update(newCRC, crc32.IEEETable, data)
}

// Hasher 提供哈希计算功能的接口
type Hasher struct {
	hasher *blake3.Hasher
}

// NewHasher 创建一个新的Hasher实例
func NewHasher() *Hasher {
	return &Hasher{
		hasher: blake3.New(32, nil),
	}
}

// Write 写入数据到哈希计算器
func (h *Hasher) Write(data []byte) {
	h.hasher.Write(data)
}

// Sum 计算并返回哈希值的十六进制字符串
func (h *Hasher) Sum() string {
	hashBytes := h.hasher.Sum(nil)
	return hex.EncodeToString(hashBytes)
}
