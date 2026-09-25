// Package common 提供 client 和 server 共享的二进制协议定义
package protocol

import (
	"encoding/binary"
	"errors"
	"strings"
)

// ======================== 二进制协议 ========================

// MessageType 消息类型
type MessageType uint8

const (
	MsgTCPConnect MessageType = iota + 1
	MsgTCPData
	MsgTCPClose
	MsgUDPConnect
	MsgUDPData
	MsgUDPClose
	MsgConnStatus
	MsgSelectUplink
	MsgSelectDownlink
	MsgBackpressure
	MsgPrebindRequest MessageType = 0x10 // 预绑定请求
	MsgChannelReset   MessageType = 0x11 // 通道重置通知

	// 反向通道控制消息（加法式变更：旧端 switch 无此 case 自动忽略）。
	// 语义见 xtunnel-cli 仓 docs/superpowers/specs/2026-09-23-reverse-mode-design.md
	MsgReverseListen       MessageType = 0x20 // 客户端→服务端：请求服务端开启反向监听，meta=监听参数值原文（如 socks5://user:pass@0.0.0.0:30000），connID=监听器 ID
	MsgReverseListenResult MessageType = 0x21 // 服务端→客户端：监听注册结果，meta[0]=状态(ConnStatus)，meta[1:]=失败原因文本
	MsgHotPairNotify       MessageType = 0x22 // 客户端→服务端：批量预热通道对通知（健康维护由客户端负责），payload 为 HotPairInfo 记录序列
	MsgHotPairBegin        MessageType = 0x23 // 客户端→服务端：显式预绑定轮次开始（B 方案 armed）；服务端收到后在 armed 窗口内接受同轮 prebind，旧客户端无 Begin 时回退 5s 窗口
)

// ======================== HotPair 预热通道对 ========================

// HotPairInfo 一条预热通道对记录。通道以绝对方向命名，两种模式共用同一语义：
// ChA = client→server 方向通道（客户端发包/服务端收包）；
// ChB = server→client 方向通道（服务端发包/客户端收包）。
// 各端按「数据流向我来=收包通道，我发出=发包通道」对称推导，正反模式零特判。
type HotPairInfo struct {
	Key string // 预热 Pair 键（预热期双方已知的 prebind connID）
	ChA int
	ChB int
}

// hotPairRecordLen 单条记录定长部分：keyLen(1) + chA(4) + chB(4)
const hotPairRecordFixedLen = 1 + 4 + 4

// EncodeHotPairNotify 编码批量预热通道对通知 payload：
// 记录 × N 连续排列（[1B keyLen][key][4B ChA][4B ChB]），无总数前缀，按长度自然终止。
func EncodeHotPairNotify(entries []HotPairInfo) []byte {
	size := 0
	for _, e := range entries {
		size += hotPairRecordFixedLen + len(e.Key)
	}
	buf := make([]byte, 0, size)
	for _, e := range entries {
		if len(e.Key) == 0 || len(e.Key) > 255 {
			continue
		}
		buf = append(buf, byte(len(e.Key)))
		buf = append(buf, e.Key...)
		var ch [4]byte
		binary.BigEndian.PutUint32(ch[:], uint32(e.ChA))
		buf = append(buf, ch[:]...)
		binary.BigEndian.PutUint32(ch[:], uint32(e.ChB))
		buf = append(buf, ch[:]...)
	}
	return buf
}

// DecodeHotPairNotify 解码批量预热通道对通知 payload，容忍尾部截断（跳过不完整记录）
func DecodeHotPairNotify(payload []byte) []HotPairInfo {
	var out []HotPairInfo
	for off := 0; off < len(payload); {
		if off+hotPairRecordFixedLen > len(payload) {
			break
		}
		keyLen := int(payload[off])
		if keyLen == 0 || off+hotPairRecordFixedLen+keyLen > len(payload) {
			break
		}
		key := string(payload[off+1 : off+1+keyLen])
		cha := int(binary.BigEndian.Uint32(payload[off+1+keyLen : off+5+keyLen]))
		chb := int(binary.BigEndian.Uint32(payload[off+5+keyLen : off+9+keyLen]))
		out = append(out, HotPairInfo{Key: key, ChA: cha, ChB: chb})
		off += hotPairRecordFixedLen + keyLen
	}
	return out
}

// hotPairKeyPrefix 预热 Pair 键前缀（与预热期 prebind connID 一致）
const hotPairKeyPrefix = "prebind-"

// hotPairConnIDSep 拨号 connID 中 Pair 键与连接唯一后缀的分隔符。
// 键（"prebind-<uuid>"）只含字母数字与 '-'，不含 '.'，因此取首个 '.' 分隔即无歧义。
const hotPairConnIDSep = '.'

// HotPairConnID 组合拨号 connID：预热 Pair 键 + 连接唯一后缀。
// 接收方按前缀查预热表即可获得完整收发通道，键相同的多条 Pair 也能精确消歧。
func HotPairConnID(key, uniqueSuffix string) string {
	if key == "" || uniqueSuffix == "" {
		return ""
	}
	return key + string(hotPairConnIDSep) + uniqueSuffix
}

// SplitHotPairConnID 拆分拨号 connID；非预热格式返回 ok=false。
func SplitHotPairConnID(connID string) (key, suffix string, ok bool) {
	idx := strings.IndexByte(connID, hotPairConnIDSep)
	if idx <= 0 || idx == len(connID)-1 {
		return "", "", false
	}
	key = connID[:idx]
	suffix = connID[idx+1:]
	if !strings.HasPrefix(key, hotPairKeyPrefix) {
		return "", "", false
	}
	return key, suffix, true
}

// PrebindTarget 预绑定目标标识
const PrebindTarget = "x-tunnel.prebind"

// BackpressureState 背压状态
type BackpressureState uint8

const (
	BackpressureNormal   BackpressureState = 0 // 恢复正常
	BackpressureSlowDown BackpressureState = 1 // 减速
	BackpressurePause    BackpressureState = 2 // 暂停
)

// ConnStatus 连接状态
type ConnStatus uint8

const (
	StatusOK  ConnStatus = 0
	StatusERR ConnStatus = 1
)

const headerLen = 8

// maxInt 是 int 类型的最大值，用于防止切片分配溢出
const maxInt = int(^uint(0) >> 1)

// EncodeMessage 编码消息
func EncodeMessage(t MessageType, connID string, meta, payload []byte) []byte {
	if len(connID) > 255 {
		connID = connID[:255]
	}
	buf := make([]byte, headerLen+len(connID)+len(meta)+len(payload))
	buf[0] = byte(t)
	buf[1] = byte(len(connID))
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(meta)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	off := headerLen
	copy(buf[off:], connID)
	off += len(connID)
	copy(buf[off:], meta)
	off += len(meta)
	copy(buf[off:], payload)
	return buf
}

// DecodeMessage 解码消息
func DecodeMessage(b []byte) (t MessageType, connID string, meta, payload []byte, err error) {
	if len(b) < headerLen {
		return 0, "", nil, nil, errors.New("帧过短")
	}
	t = MessageType(b[0])
	idLen := int(b[1])
	metaLen := int(binary.BigEndian.Uint16(b[2:4]))
	payloadLen32 := binary.BigEndian.Uint32(b[4:8])

	// 使用 uint64 计算总长度，避免 32 位平台上 int 溢出
	total := uint64(headerLen) + uint64(idLen) + uint64(metaLen) + uint64(payloadLen32)
	if total > uint64(len(b)) || total > uint64(maxInt) {
		return 0, "", nil, nil, errors.New("长度无效")
	}

	payloadLen := int(payloadLen32)
	off := headerLen
	connID = string(b[off : off+idLen])
	off += idLen
	meta = b[off : off+metaLen]
	off += metaLen
	payload = b[off : off+payloadLen]
	return t, connID, meta, payload, nil
}
