package protocol

import "testing"

func TestShortID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"prebind 前缀+完整UUID: 保留前缀+后续8位", "prebind-5f3a9c21d8e7b4a09c1f2e3d4a5b6c7d8e9f0", "prebind-5f3a9c21"},
		{"prebind 前缀+恰好8位UUID", "prebind-a1b2c3d4", "prebind-a1b2c3d4"},
		{"prebind 前缀+不足8位(异常短): 走默认截8位", "prebind-12", "prebind-"},
		{"裸 UUID: 截前8位", "5f3a9c21-d8e7-4b09-a1d2-e8f9799a0b1c", "5f3a9c21"},
		{"短 ID 原样", "abc", "abc"},
		{"恰好8字符", "12345678", "12345678"},
		{"空串", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShortID(tc.in); got != tc.want {
				t.Errorf("ShortID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
