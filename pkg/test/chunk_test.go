package test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/stretchr/testify/assert"
)

const testFilePath = "./testdata/test_chunk.bin"
const testFileDir = "./testdata/"

func setupTestFile(t *testing.T) {
	err := os.RemoveAll(testFilePath)
	assert.NoError(t, err)
}

func setupBenchmarkFile(b *testing.B) {
	err := os.RemoveAll(testFilePath)
	if err != nil {
		b.Fatal(err)
	}
}

func TestNewChunk(t *testing.T) {
	setupTestFile(t)

	cf, err := chunk.New(testFilePath)
	assert.NoError(t, err)
	assert.NotNil(t, cf)
	assert.NotNil(t, cf.Head)
	assert.NotNil(t, cf.File)

	err = cf.Close()
	assert.NoError(t, err)

}

func TestOpenChunkFile(t *testing.T) {
	setupTestFile(t)

	// Create a new chunk file first
	cf, err := chunk.New(testFilePath)
	assert.NoError(t, err)
	err = cf.Close()
	assert.NoError(t, err)

	// Test opening existing file
	cf, err = chunk.Open(testFilePath)
	assert.NoError(t, err)
	assert.NotNil(t, cf)

	err = cf.Close()
	assert.NoError(t, err)

	// Test opening non-existent file
	_, err = chunk.Open("nonexistent.bin")
	assert.Error(t, err)
}

func TestAppend(t *testing.T) {
	setupTestFile(t)

	cf, err := chunk.New(testFilePath)
	assert.NoError(t, err)

	testData := []byte("test data")
	n, err := cf.Append(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)

	// check crc hash size
	reopen, err := chunk.Open(testFilePath)
	assert.NoError(t, err)

	origin, err := json.Marshal(cf.Head)
	assert.NoError(t, err)

	now, err := json.Marshal(reopen.Head)
	assert.NoError(t, err)
	assert.Equal(t, string(origin), string(now))

	err = cf.Close()
	assert.NoError(t, err)
}

func TestStat(t *testing.T) {
	setupTestFile(t)

	cf, err := chunk.New(testFilePath)
	assert.NoError(t, err)

	testData := []byte("test data")
	_, err = cf.Append(testData)
	assert.NoError(t, err)

	stat, err := cf.Stat(nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(len(testData)), stat.Size)
	assert.NotEmpty(t, stat.Hash)
	assert.NotZero(t, stat.CRC)

	err = cf.Close()
	assert.NoError(t, err)
}

func TestClose(t *testing.T) {
	setupTestFile(t)

	cf, err := chunk.New(testFilePath)
	assert.NoError(t, err)

	err = cf.Close()
	assert.NoError(t, err)

	// Test double close
	err = cf.Close()
	assert.Error(t, err)
}

func TestUpdateOptions(t *testing.T) {
	setupTestFile(t)

	cf, err := chunk.New(testFilePath)
	assert.NoError(t, err)

	testData := []byte("test data")
	_, err = cf.Append(testData)
	assert.NoError(t, err)

	stat, err := cf.Stat(nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(len(testData)), stat.Size)
	assert.NotEmpty(t, stat.Hash)
	assert.NotZero(t, stat.CRC)

	err = cf.Close()
	assert.NoError(t, err)
}

func BenchmarkAppend(b *testing.B) {
	setupBenchmarkFile(b)

	cf, err := chunk.New(testFilePath)
	if err != nil {
		b.Fatal(err)
	}
	defer cf.Close()

	data := make([]byte, 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := cf.Append(data)
		if err != nil {
			b.Fatal(err)
		}
	}

}

func TestRemoveFile(t *testing.T) {
	err := os.RemoveAll(testFileDir)
	assert.NoError(t, err)
}
