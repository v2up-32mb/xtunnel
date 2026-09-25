package protocol

import (
	"testing"
)

func TestHotPairNotifyEncodeDecode(t *testing.T) {
	entries := []HotPairInfo{
		{Key: "prebind-aaa", ChA: 1, ChB: 2},
		{Key: "prebind-bbb", ChA: 3, ChB: 4},
	}
	payload := EncodeHotPairNotify(entries)
	got := DecodeHotPairNotify(payload)
	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	for i := range entries {
		if got[i] != entries[i] {
			t.Fatalf("entry %d mismatch: got %+v want %+v", i, got[i], entries[i])
		}
	}
}

func TestHotPairNotifyEncodeSkipsInvalid(t *testing.T) {
	payload := EncodeHotPairNotify([]HotPairInfo{
		{Key: "", ChA: 1, ChB: 2}, // 空 key：跳过
		{Key: "prebind-ok", ChA: 5, ChB: 6},
	})
	got := DecodeHotPairNotify(payload)
	if len(got) != 1 || got[0].Key != "prebind-ok" || got[0].ChA != 5 || got[0].ChB != 6 {
		t.Fatalf("unexpected decode result: %+v", got)
	}
}

func TestHotPairNotifyDecodeTruncated(t *testing.T) {
	full := EncodeHotPairNotify([]HotPairInfo{{Key: "prebind-ccc", ChA: 7, ChB: 8}})
	// 尾部截断：跳过不完整记录，不 panic
	got := DecodeHotPairNotify(full[:len(full)-2])
	if len(got) != 0 {
		t.Fatalf("expected no entries from truncated payload, got %+v", got)
	}
	// 完整记录 + 垃圾尾巴
	got = DecodeHotPairNotify(append(append([]byte{}, full...), 0x01, 0x02))
	if len(got) != 1 || got[0].Key != "prebind-ccc" {
		t.Fatalf("unexpected decode with trailing garbage: %+v", got)
	}
}

func TestHotPairConnIDRoundTrip(t *testing.T) {
	key := "prebind-3f2a9c1e-1111-2222-3333-444455556666"
	suffix := "778899aa-bbbb-cccc-dddd-eeeeffff0000"
	connID := HotPairConnID(key, suffix)
	if connID == "" {
		t.Fatal("HotPairConnID returned empty")
	}
	gotKey, gotSuffix, ok := SplitHotPairConnID(connID)
	if !ok || gotKey != key || gotSuffix != suffix {
		t.Fatalf("round trip mismatch: key=%q suffix=%q ok=%v", gotKey, gotSuffix, ok)
	}
}

func TestSplitHotPairConnIDRejects(t *testing.T) {
	cases := []string{
		"",
		"plain-uuid",
		"prebind-only-no-sep",
		"prebind-x.",         // 空后缀
		".suffix",            // 空 key
		"wrongprefix.suffix", // 键无 prebind 前缀
	}
	for _, c := range cases {
		if _, _, ok := SplitHotPairConnID(c); ok {
			t.Fatalf("expected reject %q", c)
		}
	}
}

func TestHotPairConnIDEmptyArgs(t *testing.T) {
	if HotPairConnID("", "x") != "" || HotPairConnID("prebind-x", "") != "" {
		t.Fatal("expected empty result for empty args")
	}
}

// TestEncodeDecodeHotPairBegin P2-5: 0x23 Begin 帧(空 connID/空 payload)编解码回环,不报错
func TestEncodeDecodeHotPairBegin(t *testing.T) {
	raw := EncodeMessage(MsgHotPairBegin, "", nil, nil)
	mtype, connID, meta, payload, err := DecodeMessage(raw)
	if err != nil {
		t.Fatalf("DecodeMessage(Begin) 错误: %v", err)
	}
	if mtype != MsgHotPairBegin {
		t.Errorf("msgType = %v, want MsgHotPairBegin", mtype)
	}
	if connID != "" || len(meta) != 0 || len(payload) != 0 {
		t.Errorf("空 ID/payload 回环失真: id=%q meta=%v payload=%v", connID, meta, payload)
	}
}
