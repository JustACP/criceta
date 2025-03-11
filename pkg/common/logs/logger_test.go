package logs_test

import (
	"testing"

	"github.com/JustACP/criceta/pkg/common/logs"
)

func TestLogger(t *testing.T) {
	logs.Error("This is a error msg")
}
