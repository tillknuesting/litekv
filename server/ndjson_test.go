package server

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// The encoding is one rule used by two routes in two directions, so it is
// tested as a rule and not only through the routes that use it. What the routes
// then check is that they use it.

// TestTextOrBase64 is the choice itself: valid UTF-8 goes in the plain field,
// anything else goes in the base64 one, and nothing is ever in both.
func TestTextOrBase64(t *testing.T) {
	for _, test := range []struct {
		name  string
		bytes []byte
		plain bool
	}{
		{"ascii", []byte("plain"), true},
		{"empty", []byte(""), true},
		{"nil", nil, true},
		{"umlaut", []byte("ümlaut"), true},
		{"han", []byte("键"), true},
		{"a nul is text", []byte("\x00zero"), true},
		{"a newline is text", []byte("two\nlines"), true},
		{"quotes and a backslash", []byte(`he said "\"`), true},
		{"a lone 0xff", []byte{0xff}, false},
		{"a truncated rune", []byte("键")[:2], false},
		{"a lone surrogate half", []byte{0xed, 0xa0, 0x80}, false},
		{"binary", []byte{0x00, 0x01, 0xfe, 0xff}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			plain, coded := onTheWire(test.bytes)

			if (plain != nil) != test.plain {
				t.Errorf("plain is %v, want %v", plain != nil, test.plain)
			}
			if (coded != nil) == test.plain {
				t.Errorf("both fields or neither: plain %v, coded %v", plain != nil, coded != nil)
			}

			back, err := fromTheWire(plain, coded, "key")
			if err != nil {
				t.Fatalf("reading it back: %v", err)
			}
			if !bytes.Equal(back, test.bytes) {
				t.Errorf("round trip gave %q, want %q", back, test.bytes)
			}
		})
	}
}

// TestTheReplacementCharacterIsNotAnEncoding is why this file exists at all.
// encoding/json turns a byte that is not UTF-8 into U+FFFD rather than refusing
// it, so a rule that let one through would answer 200 having lost the caller's
// bytes. The check is utf8.Valid and nothing else, and this is what says so.
func TestTheReplacementCharacterIsNotAnEncoding(t *testing.T) {
	original := []byte{'a', 0xff, 'b'}

	// What the naive encoding would do, and it does not report anything.
	naive, err := json.Marshal(string(original))
	if err != nil {
		t.Fatal(err)
	}
	var lost string
	if err := json.Unmarshal(naive, &lost); err != nil {
		t.Fatal(err)
	}
	if lost == string(original) {
		t.Skip("encoding/json no longer replaces invalid UTF-8, so this rule can be simpler")
	}

	// And what this one does.
	var kept bytes.Buffer
	if err := encodePair(json.NewEncoder(&kept), original, original); err != nil {
		t.Fatal(err)
	}
	if bytes.ContainsRune(kept.Bytes(), '�') {
		t.Fatalf("the replacement character reached the wire: %s", kept.Bytes())
	}

	line, err := decodePair(bytes.TrimSpace(kept.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	key, err := fromTheWire(line.Key, line.KeyB64, "key")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, original) {
		t.Errorf("read back %q, want %q", key, original)
	}
}

// TestOneSpellingOfAField holds the two rules that make a request unambiguous:
// both forms at once is a client that does not know what it is sending, and the
// base64 is base64url without padding and not whatever the client had to hand.
func TestOneSpellingOfAField(t *testing.T) {
	plain, coded := "a", "YQ"
	padded, wrong := "YQ==", "!!!!"

	if _, err := fromTheWire(&plain, &coded, "key"); err == nil {
		t.Error("key and key_b64 together were accepted")
	}
	if _, err := fromTheWire(nil, &padded, "key"); err == nil {
		t.Error("padded base64 was accepted")
	}
	if _, err := fromTheWire(nil, &wrong, "value"); err == nil {
		t.Error("something that is not base64 at all was accepted")
	}

	// The empty string in one of the two fields is still that field being sent,
	// which is why they are pointers.
	empty := ""
	if _, err := fromTheWire(&empty, &coded, "key"); err == nil {
		t.Error(`{"key":"","key_b64":"YQ"} was accepted`)
	}
}

// TestALineIsOneObject is the framing. Everything here is a client that got
// something slightly wrong, and the point of each is that it is told rather
// than quietly obeyed.
func TestALineIsOneObject(t *testing.T) {
	for _, test := range []struct {
		name string
		line string
	}{
		{"not JSON", `{`},
		{"not an object", `["op","write"]`},
		{"a number", `12`},
		{"a field nobody has", `{"op":"write","vaule":"x"}`},
		{"two objects on one line", `{"op":"write"} {"op":"delete"}`},
		{"trailing rubbish", `{"op":"write"} x`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodePair([]byte(test.line)); err == nil {
				t.Errorf("%s was accepted", test.line)
			}
		})
	}

	// And the ordinary case, so that the above is about these lines rather than
	// about decodePair refusing everything.
	line, err := decodePair([]byte(`{"op":"write","key":"a","value":"b"}`))
	if err != nil {
		t.Fatalf("an ordinary line: %v", err)
	}
	if line.Op != opWrite || line.Key == nil || *line.Key != "a" {
		t.Errorf("decoded %+v", line)
	}
}

// TestALineMustBeText is the other half of the encoding rule, on the way in. A
// raw byte that is not UTF-8 inside a JSON string is not JSON, and encoding/json
// would replace it and carry on; the line is refused before it gets the chance.
func TestALineMustBeText(t *testing.T) {
	if _, err := decodePair([]byte("{\"op\":\"write\",\"key\":\"\xff\"}")); err == nil {
		t.Error("a line that is not UTF-8 was accepted")
	}

	// Spelled the way it is meant to be spelled, the same key is fine.
	line, err := decodePair([]byte(`{"op":"write","key_b64":"_w"}`))
	if err != nil {
		t.Fatal(err)
	}
	key, err := fromTheWire(line.Key, line.KeyB64, "key")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, []byte{0xff}) {
		t.Errorf("key_b64 gave %q", key)
	}
}

// TestAnAnswerCarriesOnlyWhatItMeans keeps op and expires out of a pair on the
// way back. They are what a client sends; a scan that echoed them empty would
// be inviting somebody to read something into them.
func TestAnAnswerCarriesOnlyWhatItMeans(t *testing.T) {
	var out bytes.Buffer
	if err := encodePair(json.NewEncoder(&out), []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	line := strings.TrimSpace(out.String())
	if line != `{"key":"k","value":"v"}` {
		t.Errorf("a pair is %s", line)
	}
	if !strings.HasSuffix(out.String(), "\n") {
		t.Error("a pair is not newline-terminated, so the framing is gone")
	}
}
