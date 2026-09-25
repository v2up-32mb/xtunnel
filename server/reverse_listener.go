package server

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xshared/config"
	"github.com/v2up-32mb/xshared/httpproxy"
	"github.com/v2up-32mb/xshared/socks5"
	"github.com/v2up-32mb/xtunnel"
	"github.com/v2up-32mb/xtunnel/protocol"
)

const websocketBinary = websocket.BinaryMessage

type reverseListener struct {
	spec       string
	listenerID string
	server     interface{ Close() error }
}

type ReverseListenerManager struct {
	maxPerClient int
	clients      map[string]map[string]*reverseListener
	mu           sync.RWMutex
	pool         *serverPool
}

func NewReverseListenerManager(maxPerClient int) *ReverseListenerManager {
	if maxPerClient <= 0 {
		maxPerClient = 3
	}
	return &ReverseListenerManager{
		maxPerClient: maxPerClient,
		clients:      make(map[string]map[string]*reverseListener),
	}
}

func (m *ReverseListenerManager) HandleReverseListen(clientID string, chID int, listenerID string, spec []byte) {
	specStr := string(spec)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pool == nil {
		m.reply(clientID, chID, listenerID, protocol.StatusERR, "manager not initialized")
		return
	}

	clientMap, ok := m.clients[clientID]
	if !ok {
		clientMap = make(map[string]*reverseListener)
		m.clients[clientID] = clientMap
	}

	if _, exists := clientMap[specStr]; exists {
		m.reply(clientID, chID, listenerID, protocol.StatusOK, "")
		return
	}

	if len(clientMap) >= m.maxPerClient {
		m.reply(clientID, chID, listenerID, protocol.StatusERR, "listener limit reached")
		return
	}

	host, _, _, scheme, err := parseListenerSpec(specStr)
	if err != nil {
		m.reply(clientID, chID, listenerID, protocol.StatusERR, err.Error())
		return
	}

	dialer := &reverseDialer{pool: m.pool, clientID: clientID}
	_, user, pass, scheme, err := parseListenerSpec(specStr)
	if err != nil {
		m.reply(clientID, chID, listenerID, protocol.StatusERR, err.Error())
		return
	}

	var srv interface{ Close() error }
	switch scheme {
	case "socks5":
		cfg := &config.Config{ListenAddress: host}
		opts := []socks5.Option{}
		if user != "" || pass != "" {
			opts = append(opts, socks5.WithUserPassAuth(func(u, p string) bool {
				return xtunnel.AuthEqual(u, user) && xtunnel.AuthEqual(p, pass)
			}))
		}
		s := socks5.NewServer(cfg, dialer, opts...)
		srv = s
		if err := s.Start(); err != nil {
			m.reply(clientID, chID, listenerID, protocol.StatusERR, err.Error())
			return
		}
	case "http":
		cfg := &config.Config{ListenAddress: host}
		opts := []httpproxy.Option{}
		if user != "" || pass != "" {
			opts = append(opts, httpproxy.WithUserPassAuth(func(u, p string) bool {
				return xtunnel.AuthEqual(u, user) && xtunnel.AuthEqual(p, pass)
			}))
		}
		s := httpproxy.NewServer(cfg, dialer, opts...)
		srv = s
		if err := s.Start(); err != nil {
			m.reply(clientID, chID, listenerID, protocol.StatusERR, err.Error())
			return
		}
	default:
		m.reply(clientID, chID, listenerID, protocol.StatusERR, "unsupported scheme")
		return
	}

	clientMap[specStr] = &reverseListener{
		spec:       specStr,
		listenerID: listenerID,
		server:     srv,
	}
	srvLog(LevelInfo, "reverse_listener", "[服务端] 反向监听已开启: %s (客户端 %s)", specStr, protocol.ShortID(clientID))
	m.reply(clientID, chID, listenerID, protocol.StatusOK, "")
}

func (m *ReverseListenerManager) reply(clientID string, chID int, listenerID string, status protocol.ConnStatus, reason string) {
	if m.pool == nil {
		return
	}
	meta := []byte{byte(status)}
	if reason != "" {
		meta = append(meta, []byte(reason)...)
	}
	data := protocol.EncodeMessage(protocol.MsgReverseListenResult, listenerID, meta, nil)
	_ = m.pool.sendToChannel(clientID, chID, websocketBinary, data)
}

func (m *ReverseListenerManager) ShutdownClient(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if clientMap, ok := m.clients[clientID]; ok {
		for _, rl := range clientMap {
			if rl.server != nil {
				rl.server.Close()
			}
		}
		delete(m.clients, clientID)
		srvLog(LevelInfo, "reverse_listener", "[服务端] 反向监听已关闭: 客户端 %s", protocol.ShortID(clientID))
	}
}

func (m *ReverseListenerManager) Shutdown() {
	m.mu.Lock()
	for cID, cmap := range m.clients {
		for _, rl := range cmap {
			if rl.server != nil {
				rl.server.Close()
			}
		}
		delete(m.clients, cID)
	}
	m.mu.Unlock()
}

func parseListenerSpec(spec string) (host string, user string, pass string, scheme string, err error) {
	if strings.HasPrefix(spec, "socks5://") {
		scheme = "socks5"
		rest := strings.TrimPrefix(spec, "socks5://")
		if strings.Contains(rest, "@") {
			parts := strings.SplitN(rest, "@", 2)
			auth := parts[0]
			host = parts[1]
			if strings.Contains(auth, ":") {
				ub := strings.SplitN(auth, ":", 2)
				user, pass = ub[0], ub[1]
			} else {
				user = auth
			}
		} else {
			host = rest
		}
		return
	}
	if strings.HasPrefix(spec, "http://") {
		scheme = "http"
		rest := strings.TrimPrefix(spec, "http://")
		if strings.Contains(rest, "@") {
			parts := strings.SplitN(rest, "@", 2)
			auth := parts[0]
			host = parts[1]
			if strings.Contains(auth, ":") {
				ub := strings.SplitN(auth, ":", 2)
				user, pass = ub[0], ub[1]
			} else {
				user = auth
			}
		} else {
			host = rest
		}
		return
	}
	return "", "", "", "", fmt.Errorf("unsupported spec")
}
