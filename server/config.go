package server

import (
	"errors"
	"time"
)

var (
	// ErrInvalidListenAddr 无效的监听地址
	ErrInvalidListenAddr = errors.New("invalid listen address")
	// ErrEmptyToken Token 为空
	ErrEmptyToken = errors.New("token cannot be empty")
	// ErrServerNotStarted 服务端未启动
	ErrServerNotStarted = errors.New("server not started")
	// ErrServerAlreadyClosed 服务端已关闭
	ErrServerAlreadyClosed = errors.New("server already closed")
)

// Config 服务端配置
type Config struct {
	// 监听配置
	ListenAddr string // HTTPS 监听地址 (如 :8443)
	Token      string // 认证令牌（WebSocket Subprotocol）

	// TLS 配置
	CertFile string // TLS 证书文件路径（可选）
	KeyFile  string // TLS 私钥文件路径（可选）
	AutoCert bool   // 是否自动生成自签名证书

	// WebSocket 配置
	ReadTimeout      time.Duration // WebSocket 读超时
	WriteTimeout     time.Duration // WebSocket 写超时
	PingInterval     time.Duration // Ping 间隔
	HandshakeTimeout time.Duration // WebSocket 握手超时

	// 缓冲区配置
	ReadBufferSize  int // 读缓冲区大小
	WriteBufferSize int // 写缓冲区大小

	// 背压控制
	BackpressureLimitBytes int // 全局队列背压阈值（字节），0 表示使用默认值 32MB

	// 接入限制
	MaxTotalChannels     int // 最大总通道数（0 表示无限制）
	MaxChannelsPerClient int // 每个客户端最大通道数（0 表示无限制）

	// 反向监听限制
	MaxReverseListeners int // 每客户端最大反向监听器数，默认 3
}

// DefaultConfig 返回带有合理默认值的配置
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:             ":8443",
		Token:                  "",
		AutoCert:               true,
		ReadTimeout:            15 * time.Second,
		WriteTimeout:           5 * time.Second,
		PingInterval:           5 * time.Second,
		HandshakeTimeout:       5 * time.Second,
		ReadBufferSize:         64 * 1024,
		WriteBufferSize:        64 * 1024,
		BackpressureLimitBytes: 32 << 20, // 默认 32MB
		MaxTotalChannels:       0,
		MaxChannelsPerClient:   0,
		MaxReverseListeners:    3,
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.ListenAddr == "" {
		return ErrInvalidListenAddr
	}
	if c.Token == "" {
		return ErrEmptyToken
	}
	if c.MaxTotalChannels < 0 || c.MaxChannelsPerClient < 0 {
		return errors.New("channel limits cannot be negative")
	}
	if c.ReadTimeout <= 0 || c.WriteTimeout <= 0 || c.PingInterval <= 0 || c.HandshakeTimeout <= 0 {
		return errors.New("timeout values must be positive")
	}
	return nil
}
