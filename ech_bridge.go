// ECH 桥接：基于 xclient 共享 ECH 管理器（DoH 多服务器 fallback 优先，
// 失败回退 UDP DNS，再回退标准 TLS）。此逻辑自 x-client xtunnel/shared.go
// 上移入库，使上游 CLI 与移动端共用同一回退链。
package xtunnel

import (
	"strings"

	"github.com/v2up-32mb/xshared/config"
	"github.com/v2up-32mb/xshared/dns"
	"github.com/v2up-32mb/xshared/ech"
)

// newSharedEchManager 基于 x-tunnel 配置构建共享 ECH 管理器。
func newSharedEchManager(cfg *Config) *ech.EchManager {
	return ech.NewEchManager(newSharedDoHClient(cfg), cfg.ECHDomain, 0, 0)
}

// newSharedConfig 将 x-tunnel 配置映射为共享配置视图（logger/DoH 使用）。
func newSharedConfig(cfg *Config) *config.Config {
	shared := config.DefaultConfig()
	shared.EnableDoH = true
	if dnsServer := strings.TrimSpace(cfg.DNSServer); dnsServer != "" {
		shared.DoHUrl = dnsServer
	}
	return shared
}

// newSharedDoHClient 基于共享配置视图构建 DoH 客户端。
func newSharedDoHClient(cfg *Config) *dns.DoHClient {
	return dns.NewDoHClient(newSharedConfig(cfg))
}
