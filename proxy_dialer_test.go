package xtunnel

import (
	"testing"

	"github.com/v2up-32mb/xshared/dialer"
)

// ---- 编译期接口满足断言 ----

var _ dialer.Dialer = (*poolStreamDialer)(nil)
var _ dialer.UDPDialer = (*poolUDPDialer)(nil)
var _ udpDownlinkSink = (*poolUDPChannel)(nil)

func TestCombinedDialerImplementsDialer(t *testing.T) {
	var _ dialer.Dialer = (*combinedDialer)(nil)
	var _ dialer.UDPDialer = (*combinedDialer)(nil)
}
