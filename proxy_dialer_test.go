package xtunnel

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/v2up-32mb/xshared/dialer"
)

// ---- bufferedPipe ----

func TestBufferedPipeEchoRoundTrip(t *testing.T) {
	a, b := newBufferedPipe()
	defer a.Close()
	defer b.Close()

	// B 端回显协程
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := b.Read(buf)
			if err != nil {
				return
			}
			if _, werr := b.Write(buf[:n]); werr != nil {
				return
			}
		}
	}()

	_ = a.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := a.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(a, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("回显不符: %q", buf)
	}
}

func TestBufferedPipeLargeTransferIntact(t *testing.T) {
	// 大于单帧缓冲：验证 pending 拼接与顺序完整性
	a, b := newBufferedPipe()
	defer a.Close()
	defer b.Close()

	payload := make([]byte, 300*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.SetDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(b, got); err != nil {
			t.Errorf("b read: %v", err)
			return
		}
		for i := range got {
			if got[i] != payload[i] {
				t.Errorf("payload 在 #%d 处不一致", i)
				return
			}
		}
	}()
	_ = a.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := a.Write(payload); err != nil {
		t.Fatalf("a write: %v", err)
	}
	wg.Wait()
}

func TestBufferedPipeClosePropagatesEOF(t *testing.T) {
	a, b := newBufferedPipe()
	defer b.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = b.Close()
	}()
	_ = a.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := a.Read(make([]byte, 1)); err == nil {
		t.Fatalf("对端关闭后 Read 应返回错误")
	}
	// 关闭后 Write 报错
	if _, err := a.Write([]byte("x")); err == nil {
		t.Fatalf("本端关闭后 Write 应报错")
	}
	_ = a.Close()
}

func TestBufferedPipeConcurrentStreams(t *testing.T) {
	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a, b := newBufferedPipe()
			defer a.Close()
			defer b.Close()
			go func() {
				buf := make([]byte, 1024)
				for {
					m, err := b.Read(buf)
					if err != nil {
						return
					}
					if _, werr := b.Write(buf[:m]); werr != nil {
						return
					}
				}
			}()
			_ = a.SetDeadline(time.Now().Add(3 * time.Second))
			msg := []byte{byte(i), byte(i + 1)}
			if _, err := a.Write(msg); err != nil {
				t.Errorf("#%d write: %v", i, err)
				return
			}
			got := make([]byte, 2)
			if _, err := io.ReadFull(a, got); err != nil {
				t.Errorf("#%d read: %v", i, err)
				return
			}
			if got[0] != byte(i) {
				t.Errorf("#%d 串扰: %v", i, got)
			}
		}(i)
	}
	wg.Wait()
}

// ---- ParseSocks5Auth ----

func TestParseSocks5Auth(t *testing.T) {
	cases := []struct {
		name, in, host, user, pass string
		wantErr                    bool
	}{
		{name: "无认证", in: "127.0.0.1:1080", host: "127.0.0.1:1080"},
		{name: "socks5 前缀无认证", in: "socks5://127.0.0.1:1080", host: "127.0.0.1:1080"},
		{name: "带用户名密码", in: "socks5://u:p@127.0.0.1:1080", host: "127.0.0.1:1080", user: "u", pass: "p"},
		{name: "仅用户名", in: "socks5://u@127.0.0.1:1080", host: "127.0.0.1:1080", user: "u"},
		{name: "密码含冒号", in: "socks5://u:p:1@127.0.0.1:1080", host: "127.0.0.1:1080", user: "u", pass: "p:1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host, user, pass, err := ParseSocks5Auth(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if host != tc.host || user != tc.user || pass != tc.pass {
				t.Fatalf("got (%q,%q,%q), want (%q,%q,%q)", host, user, pass, tc.host, tc.user, tc.pass)
			}
		})
	}
}

func TestAuthEqual(t *testing.T) {
	if !AuthEqual("token", "token") {
		t.Fatal("相同凭据应相等")
	}
	if AuthEqual("token", "tokem") {
		t.Fatal("不同凭据不应相等")
	}
	if AuthEqual("short", "longer") {
		t.Fatal("不同长度不应相等")
	}
}

// ---- 编译期接口满足断言 ----

var _ dialer.Dialer = (*poolStreamDialer)(nil)
var _ dialer.UDPDialer = (*poolUDPDialer)(nil)
var _ udpDownlinkSink = (*poolUDPChannel)(nil)

func TestCombinedDialerImplementsDialer(t *testing.T) {
	var _ dialer.Dialer = (*combinedDialer)(nil)
	var _ dialer.UDPDialer = (*combinedDialer)(nil)
}
