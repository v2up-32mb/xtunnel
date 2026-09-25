// Package server 提供基于 WebSocket 的隧道代理服务端.
//
// 基本使用:
//
//	cfg := &server.Config{
//	    ListenAddr: ":8443",
//	    Token:      "your_token",
//	    AutoCert:   true,
//	}
//	s, err := server.NewServer(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := s.Start(); err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Shutdown()
//	select {}
package server
