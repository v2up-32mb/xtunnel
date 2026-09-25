package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server 服务端接口
type Server struct {
	config  *Config
	pool    *serverPool
	httpSrv *http.Server
	cert    tls.Certificate
	mu      sync.Mutex
	started bool
}

// ServerStats 服务端统计信息
type ServerStats struct {
	ActiveConnections int
	ActiveChannels    int
	TotalConnections  int64
	BytesSent         uint64
	BytesReceived     uint64
}

// NewServer 创建新的服务端实例
func NewServer(cfg *Config) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	s := &Server{
		config: cfg,
		pool:   newServerPool(cfg.Token, cfg),
	}

	// 准备 TLS 证书
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		srvLog(LevelInfo, "server", "[服务端] 使用指定证书: %s", cfg.CertFile)
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("加载证书失败: %v", err)
		}
		s.cert = cert
	} else if cfg.AutoCert {
		srvLog(LevelInfo, "server", "[服务端] 自动生成自签证书")
		cert, err := GenerateSelfSignedCert()
		if err != nil {
			return nil, fmt.Errorf("生成证书失败: %v", err)
		}
		s.cert = cert
	}

	return s, nil
}

// Start 启动 HTTPS 服务器
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("server already started")
	}

	// 构建 TLS 配置
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{s.cert},
	}

	// 创建 HTTP 服务器
	s.httpSrv = &http.Server{
		Handler:           http.HandlerFunc(s.pool.handleWebSocket),
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: s.config.HandshakeTimeout,
	}

	// 预先占用端口，使端口冲突在 Start 阶段立即暴露而非异步失败
	ln, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", s.config.ListenAddr, err)
	}

	// 启动服务器（在 goroutine 中）
	go func() {
		srvLog(LevelInfo, "server", "[服务端] HTTPS 监听: %s", s.config.ListenAddr)
		srvLog(LevelInfo, "server", "[服务端] Token: %s", s.config.Token)
		if err := s.httpSrv.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
			srvLog(LevelError, "server", "[服务端] 启动失败: %v", err)
		}
	}()

	s.started = true
	srvLog(LevelInfo, "server", "[服务端] 已启动")
	return nil
}

// Shutdown 优雅关闭服务端
func (s *Server) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	if s.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(ctx)
	}

	// 主动关闭所有已建立的 WebSocket 通道，让客户端及时感知断开
	s.pool.Shutdown()

	s.started = false
	srvLog(LevelInfo, "server", "[服务端] 已关闭")
	return nil
}

// Handler 返回 HTTP Handler 用于处理 WebSocket 升级
func (s *Server) Handler() http.HandlerFunc {
	return s.pool.handleWebSocket
}

// Stats 返回服务端统计信息
func (s *Server) Stats() *ServerStats {
	return s.pool.Stats()
}
