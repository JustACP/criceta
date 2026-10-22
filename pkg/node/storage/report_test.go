package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReportJournalAppendAckAndReload 验证变更日志的追加、ACK 裁剪与重启恢复。
func TestReportJournalAppendAckAndReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, reportJournalName)

	journal, err := openReportJournal(path)
	require.NoError(t, err)

	first, err := journal.append(reportEvent{Op: reportOpUpsert, ChunkID: 1, FileID: 1, Version: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.Seq)

	_, err = journal.append(reportEvent{Op: reportOpUpsert, ChunkID: 1, FileID: 1, Version: 2})
	require.NoError(t, err)

	assert.Len(t, journal.snapshot(10), 2)

	// ACK 第一条后只剩一条，且日志文件同步裁剪
	require.NoError(t, journal.ack(first.Seq))
	assert.Len(t, journal.snapshot(10), 1)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, 1, countLines(string(data)))

	// 重新打开（模拟进程重启）：待上报事件与序号都要恢复
	reopened, err := openReportJournal(path)
	require.NoError(t, err)
	assert.Len(t, reopened.snapshot(10), 1)

	next, err := reopened.append(reportEvent{Op: reportOpDelete, ChunkID: 1, FileID: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(3), next.Seq, "seq must keep increasing across restarts")

	// 原子重写不残留临时文件
	_, err = os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(err))
}

func countLines(s string) int {
	lines := 0
	for _, r := range s {
		if r == '\n' {
			lines++
		}
	}
	return lines
}
