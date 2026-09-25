package user_service

import "testing"

func TestParseRawJIDStripsPlusForRawIQ(t *testing.T) {
	cases := map[string]string{
		"558896841352":                 "558896841352@s.whatsapp.net",
		"+558896841352":                "558896841352@s.whatsapp.net",
		"558896841352@s.whatsapp.net":  "558896841352@s.whatsapp.net",
		"+558896841352@s.whatsapp.net": "558896841352@s.whatsapp.net",
		"123456789@lid":                "123456789@lid",
	}
	for in, want := range cases {
		jid, ok := parseRawJID(in)
		if !ok {
			t.Fatalf("parseRawJID(%q) not ok", in)
		}
		if got := jid.String(); got != want {
			t.Errorf("parseRawJID(%q) = %q, want %q", in, got, want)
		}
	}
	if _, ok := parseRawJID(""); ok {
		t.Error("parseRawJID(\"\") should fail")
	}
}
