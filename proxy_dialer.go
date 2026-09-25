// 代理拨号适配：将 x-tunnel 通道池适配为 xshared/dialer 抽象，
// 使 xshared/socks5 与 xshared/httpproxy 的 TCP/UDP 数据面复用
// 通道竞争、Hot Pair、背压与重连全部池能力。
package xtunnel

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/v2up-32mb/xshared/dialer"
	"github.com/v2up-32mb/xshared/pipe"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// websocketBinary gorilla 二进制消息类型（池 broadcastWrite 的 msgType 参数）
const websocketBinary = websocket.BinaryMessage

// streamDialer 返回池的 TCP 流拨号适配器。
func (p *clientPool) streamDialer() dialer.Dialer { return &poolStreamDialer{pool: p} }

// udpDialer 返回池的 UDP 通道拨号适配器。
func (p *clientPool) udpDialer() dialer.UDPDialer { return &poolUDPDialer{pool: p} }

// ---- TCP ----

type poolStreamDialer struct{ pool *clientPool }

// DialStream 完成「注册 → MsgTCPConnect（预热热路径/广播）→ 等待连接建立」，
// 返回的 net.Conn 下行由池写入内存管道，上行经 SendDataDirect/广播。
// 预热热路径：先取 Ready Pair，connID 组合为 Pair 键 + 唯一后缀，
// 服务端按前缀查表直接获得完整收发通道，拨号期零选路消息；
// Pair 共享复用，后缀保证并发连接 connID 互不冲突。
func (d *poolStreamDialer) DialStream(ctx context.Context, target string) (net.Conn, error) {
	p := d.pool
	var pair *HotChannelPair
	if p.config.EnableHotPair && p.pairWarmer != nil {
		pair = p.pairWarmer.AcquirePrimary()
	}
	connID := uuid.NewString()
	if pair != nil {
		connID = protocol.HotPairConnID(pair.PrebindID, connID)
	}
	sink, source := pipe.NewBufferedPipe()
	p.registerAndBroadcastTCPWithPair(connID, target, nil, sink, "SOCKS5", pair)

	p.mu.RLock()
	st := p.conns[connID]
	var connected chan bool
	if st != nil {
		connected = st.connected
	}
	p.mu.RUnlock()

	if connected != nil {
		select {
		case <-connected:
		case <-time.After(p.connectTimeout()):
			p.Unregister(connID)
			return nil, fmt.Errorf("连接 %s 超时", target)
		case <-ctx.Done():
			p.Unregister(connID)
			return nil, ctx.Err()
		}
	}
	return &pipeStream{pool: p, connID: connID, source: source}, nil
}

// pipeStream 通道流的 net.Conn 侧：上行直发/广播，下行由池写入 source。
type pipeStream struct {
	pool   *clientPool
	connID string
	source net.Conn
	once   sync.Once
}

func (s *pipeStream) Read(p []byte) (int, error) { return s.source.Read(p) }
func (s *pipeStream) Write(p []byte) (int, error) {
	if chID, ok := s.pool.GetUplinkChannel(s.connID); ok {
		if err := s.pool.SendDataDirect(chID, s.connID, p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	// uplink 未确定：广播
	s.pool.broadcastWrite(websocketBinary, protocol.EncodeMessage(protocol.MsgTCPData, s.connID, nil, p))
	return len(p), nil
}

// Close 发送 MsgTCPClose（直发/广播）并注销；重复调用安全。
func (s *pipeStream) Close() error {
	s.once.Do(func() {
		if chID, ok := s.pool.GetUplinkChannel(s.connID); ok {
			_ = s.pool.SendCloseDirect(chID, s.connID)
		} else {
			s.pool.broadcastWrite(websocketBinary, protocol.EncodeMessage(protocol.MsgTCPClose, s.connID, nil, nil))
		}
		_ = s.source.Close()
		s.pool.Unregister(s.connID)
	})
	return nil
}
func (s *pipeStream) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (s *pipeStream) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (s *pipeStream) SetDeadline(t time.Time) error      { return nil }
func (s *pipeStream) SetReadDeadline(t time.Time) error  { return nil }
func (s *pipeStream) SetWriteDeadline(t time.Time) error { return nil }

// combinedDialer 同时具备 TCP 流与 UDP 通道能力。
// xshared/socks5 服务器持单一拨号器并以 \`dialer.UDPDialer\` 断言探测 UDP 能力，
// 因此 SOCKS5 场景应使用本类型（UDP ASSOCIATE 可用）；纯 TCP 消费方用 poolStreamDialer 即可。
type combinedDialer struct{ pool *clientPool }

func (d *combinedDialer) DialStream(ctx context.Context, target string) (net.Conn, error) {
	return (&poolStreamDialer{pool: d.pool}).DialStream(ctx, target)
}

func (d *combinedDialer) DialUDP(ctx context.Context, blockedPorts []int) (dialer.UDPChannel, error) {
	return (&poolUDPDialer{pool: d.pool}).DialUDP(ctx, blockedPorts)
}

// ---- UDP ----

type poolUDPDialer struct{ pool *clientPool }

// DialUDP 注册 UDP 下行汇。MsgUDPConnect 由 Send 首包懒启动（对齐上游
// assoc.loop 首包 StartUDPRace 语义）；端口拦截由消费方（xshared/socks5
// 帧层）与 Send 内双重执行。
func (d *poolUDPDialer) DialUDP(ctx context.Context, blockedPorts []int) (dialer.UDPChannel, error) {
	p := d.pool
	connID := uuid.NewString()
	ch := &poolUDPChannel{
		pool:    p,
		connID:  connID,
		blocked: append([]int(nil), blockedPorts...),
		down:    make(chan udpDatagram, 256),
		done:    make(chan struct{}),
	}
	p.RegisterUDP(connID, ch)
	return ch, nil
}

type udpDatagram struct {
	target string
	data   []byte
}

type poolUDPChannel struct {
	pool    *clientPool
	connID  string
	blocked []int
	down    chan udpDatagram
	done    chan struct{}
	once    sync.Once
	raceMu  sync.Mutex
	raceOn  bool
}

// handleUDPResponse 实现 udpDownlinkSink：上游下行数据报入队。
func (c *poolUDPChannel) handleUDPResponse(addrStr string, data []byte) {
	select {
	case c.down <- udpDatagram{target: addrStr, data: append([]byte(nil), data...)}:
	case <-c.done:
	default:
		// 队列满：丢弃（UDP 语义）
	}
}

// Send 上行：首包懒启动 MsgUDPConnect，经通道直发或广播。
func (c *poolUDPChannel) Send(target string, data []byte) error {
	select {
	case <-c.done:
		return net.ErrClosed
	default:
	}

	host, ps, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	// 端口拦截（双保险：消费方帧层已过滤）
	if port, aerr := strconv.Atoi(ps); aerr == nil {
		for _, bp := range c.blocked {
			if bp == port {
				return nil
			}
		}
	}
	// 本地 IP 策略过滤（对齐上游 assoc.loop，仅对已是 IP 的目标有意义）
	if ip := net.ParseIP(host); ip != nil {
		if c.pool.config.IPStrategy == protocol.IPStrategyIPv4Only && ip.To4() == nil {
			return nil
		}
		if c.pool.config.IPStrategy == protocol.IPStrategyIPv6Only && ip.To4() != nil {
			return nil
		}
	}

	// 首包懒启动 UDP 竞争
	c.raceMu.Lock()
	if !c.raceOn {
		c.raceOn = true
		c.raceMu.Unlock()
		c.pool.StartUDPRace(c.connID, target)
	} else {
		c.raceMu.Unlock()
	}

	if chID, ok := c.pool.GetUplinkChannel(c.connID); ok {
		return c.pool.SendUDPDataDirect(chID, c.connID, data)
	}
	// uplink 未确定：广播
	c.pool.broadcastWrite(websocketBinary, protocol.EncodeMessage(protocol.MsgUDPData, c.connID, nil, data))
	return nil
}

// ReadFrom 下行读取；通道关闭后 ok=false。
func (c *poolUDPChannel) ReadFrom() (string, []byte, bool) {
	select {
	case dg := <-c.down:
		return dg.target, dg.data, true
	case <-c.done:
		return "", nil, false
	}
}

// closeSink 实现 udpDownlinkSink：池侧强制停机。
func (c *poolUDPChannel) closeSink() { c.stop() }

// Close 实现 dialer.UDPChannel：消费方主动停机。
func (c *poolUDPChannel) Close() error { c.stop(); return nil }

func (c *poolUDPChannel) stop() {
	c.once.Do(func() {
		close(c.done)
		if chID, ok := c.pool.GetUplinkChannel(c.connID); ok {
			c.pool.SendUDPCloseDirect(chID, c.connID)
		}
		c.pool.Unregister(c.connID)
	})
}

// ---- 内存缓冲管道 ----

// bufferedPipe 带缓冲的内存双向管道（net.Pipe 零缓冲会让池下行阻塞读循环）。
