// 代理拨号适配：将 x-tunnel 通道池适配为 xshared/dialer 抽象，
// 使 xshared/socks5 与 xshared/httpproxy 的 TCP/UDP 数据面复用
// 通道竞争、Hot Pair、背压与重连全部池能力。
package xtunnel

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/v2up-32mb/xshared/dialer"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// websocketBinary gorilla 二进制消息类型（池 broadcastWrite 的 msgType 参数）
const websocketBinary = websocket.BinaryMessage

// ParseSocks5Auth 解析 "socks5://user:pass@host" 形式的本地代理监听地址。
// 凭据非空时消费方应启用 SOCKS5 RFC1929 子协商。
func ParseSocks5Auth(addr string) (host, user, pass string, err error) {
	full := strings.TrimPrefix(addr, "socks5://")
	if strings.Contains(full, "@") {
		parts := strings.SplitN(full, "@", 2)
		if len(parts) != 2 {
			return "", "", "", fmt.Errorf("地址格式错误: %s", addr)
		}
		auth := parts[0]
		host = parts[1]
		if strings.Contains(auth, ":") {
			creds := strings.SplitN(auth, ":", 2)
			user, pass = creds[0], creds[1]
		} else {
			user = auth
		}
		return host, user, pass, nil
	}
	return full, "", "", nil
}

// AuthEqual 常量时间比较（鉴权回调使用，防时序侧信道）。
func AuthEqual(a, b string) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// streamDialer 返回池的 TCP 流拨号适配器。
func (p *clientPool) streamDialer() dialer.Dialer { return &poolStreamDialer{pool: p} }

// udpDialer 返回池的 UDP 通道拨号适配器。
func (p *clientPool) udpDialer() dialer.UDPDialer { return &poolUDPDialer{pool: p} }

// ---- TCP ----

type poolStreamDialer struct{ pool *clientPool }

// DialStream 完成「注册 → MsgTCPConnect（Hot Pair/广播）→ 等待连接建立」，
// 返回的 net.Conn 下行由池写入内存管道，上行经 SendDataDirect/广播。
func (d *poolStreamDialer) DialStream(ctx context.Context, target string) (net.Conn, error) {
	p := d.pool
	requestID := uuid.NewString()
	sink, source := newBufferedPipe()
	// Hot Pair 路径会复用 prebind connID，后续数据面必须使用返回的实际 connID
	connID := p.RegisterAndBroadcastTCP(requestID, target, nil, sink, "SOCKS5")

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
type bufferedPipe struct {
	mu      sync.Mutex
	aCh     chan []byte
	bCh     chan []byte
	aClosed bool
	bClosed bool
	aDone   chan struct{}
	bDone   chan struct{}
}

// NewBufferedPipe 创建双向缓冲内存管道（每方向 512 块缓冲，Close 双向传播）。
// 慢读端不会阻塞写端协程，供反向通道等服务端实现复用，避免拖死协议读循环。
func NewBufferedPipe() (net.Conn, net.Conn) { return newBufferedPipe() }

func newBufferedPipe() (a, b *bufferedConn) {
	p := &bufferedPipe{
		aCh:   make(chan []byte, 512),
		bCh:   make(chan []byte, 512),
		aDone: make(chan struct{}),
		bDone: make(chan struct{}),
	}
	return &bufferedConn{p: p, side: true}, &bufferedConn{p: p, side: false} // true=A 端, false=B 端
}

type bufferedConn struct {
	p       *bufferedPipe
	side    bool // true=A（写入 bCh 由 B 读），false=B
	pending []byte
}

// outCh 本端写入通道：A 写 bCh（由 B 读），B 写 aCh（由 A 读）
func (c *bufferedConn) outCh() chan []byte {
	if c.side {
		return c.p.bCh
	}
	return c.p.aCh
}

// inCh 本端读取通道：A 读 aCh（由 B 写），B 读 bCh（由 A 写）
func (c *bufferedConn) inCh() chan []byte {
	if c.side {
		return c.p.aCh
	}
	return c.p.bCh
}

// peerDone 对端关闭标志：A 等 bDone（B 关闭时置位），B 等 aDone
func (c *bufferedConn) peerDone() chan struct{} {
	if c.side {
		return c.p.bDone
	}
	return c.p.aDone
}

// selfDone 本端关闭标志：本端 Close 时关闭，对端经 peerDone 感知
func (c *bufferedConn) selfDone() chan struct{} {
	if c.side {
		return c.p.aDone
	}
	return c.p.bDone
}
func (c *bufferedConn) selfClosedFlag() *bool {
	if c.side {
		return &c.p.aClosed
	}
	return &c.p.bClosed
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	// 优先消费本端入队数据（对端 Write），随后探测对端关闭
	select {
	case data, ok := <-c.inCh():
		if !ok {
			return 0, io.EOF
		}
		c.pending = data
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	default:
	}
	select {
	case data, ok := <-c.inCh():
		if !ok {
			return 0, io.EOF
		}
		c.pending = data
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	case <-c.peerDone():
		// 对端已关闭：排空残余后 EOF
		select {
		case data, ok := <-c.inCh():
			if !ok {
				return 0, io.EOF
			}
			c.pending = data
			n := copy(p, c.pending)
			c.pending = c.pending[n:]
			return n, nil
		default:
			return 0, io.EOF
		}
	}
}

func (c *bufferedConn) Write(p []byte) (int, error) {
	if *c.selfClosedFlag() {
		return 0, net.ErrClosed
	}
	// 对端已关闭：立即报错（对齐 net.Pipe 语义；竞争窗口内入队的单帧随管道回收）
	select {
	case <-c.peerDone():
		return 0, io.ErrClosedPipe
	default:
	}
	data := append([]byte(nil), p...)
	select {
	case c.outCh() <- data:
		return len(p), nil
	default:
	}
	// 队列满：限时等待（背压），对端关闭则报错
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case c.outCh() <- data:
		return len(p), nil
	case <-c.peerDone():
		return 0, io.ErrClosedPipe
	case <-timer.C:
		return 0, errors.New("buffered pipe 拥塞超时")
	}
}

func (c *bufferedConn) Close() error {
	flag := c.selfClosedFlag()
	done := c.selfDone()
	c.p.mu.Lock()
	if !*flag {
		*flag = true
		close(done)
	}
	c.p.mu.Unlock()
	return nil
}
func (c *bufferedConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *bufferedConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *bufferedConn) SetDeadline(t time.Time) error      { return nil }
func (c *bufferedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *bufferedConn) SetWriteDeadline(t time.Time) error { return nil }
