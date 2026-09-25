// Package common 提供 client 和 server 共享的协议定义和工具函数
package protocol

import (
	"errors"
	"io"
	"net"
	"strings"

	"github.com/gorilla/websocket"
)

// shortID 返回短格式的连接 ID（用于日志）。
// 默认截取前 8 位；但 prebind 连接的 connID 形如 "prebind-<uuid>"，
// 前缀本身恰好 8 字符会占满窗口导致 UUID 完全不可见，
// 因此对此类 ID 保留前缀 + 后续 8 位（共 16 位），便于区分不同 prebind 轮次
// 并对照 hotpair 表键。纯展示层调整，不影响协议。
func ShortID(id string) string {
	if strings.HasPrefix(id, "prebind-") && len(id) >= 16 {
		return id[:16]
	}
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}

// IsNormalCloseError 判断是否为正常的关闭错误
func IsNormalCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived:
			return true
		}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// 检查错误消息（TLS 连接关闭等情况）
	errStr := err.Error()
	return ContainsString(errStr, "tls: bad record MAC") ||
		ContainsString(errStr, "use of closed network connection") ||
		ContainsString(errStr, "connection reset by peer") ||
		ContainsString(errStr, "broken pipe") ||
		ContainsString(errStr, "websocket: close sent")
}

// ContainsString 简单的字符串包含检查（避免导入 strings）
func ContainsString(s, substr string) bool {
	return len(s) >= len(substr) && FindSubstring(s, substr)
}

// FindSubstring 查找子串
func FindSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
