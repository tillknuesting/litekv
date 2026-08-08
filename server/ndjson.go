package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// The routes that carry more than one record at a time carry them as
// newline-delimited JSON: one object to a line, no array around them, so that a
// body can be produced and consumed a line at a time and neither end has to
// hold a large answer as a single JSON value before it can look at any of it.
//
// # A key is bytes and a JSON string is not
//
// A key or a value is arbitrary bytes, and the store means that literally —
// TestKeyOfAnyBytes puts a 0xff through it. A JSON string cannot hold arbitrary
// bytes: it holds text, and encoding/json quietly replaces a byte sequence that
// is not UTF-8 with U+FFFD rather than refusing it. In both directions. A store
// that hands back a replacement character where its caller wrote 0xff has lost
// that caller's data while answering 200, which is the worst shape a bug of this
// kind can have.
//
// So the rule, and it is the same rule in both directions and on every route
// that uses this file:
//
//   - A key or a value is a plain string field — "key", "value" — when it is
//     valid UTF-8.
//   - It is a separate base64url field — "key_b64", "value_b64" — when it is
//     not.
//   - Which one is decided by utf8.Valid and by nothing else. Not by whether
//     the bytes look like text, not by what encoding/json does with them.
//   - A field sent both ways at once is an error, not something to resolve. The
//     two forms mean the same thing, so a request holding both is a client that
//     does not know which one it is sending, and picking one for it would store
//     whichever this file happened to prefer.
//
// base64url here is base64.RawURLEncoding: the alphabet of RFC 4648 §5, and no
// padding. Unpadded because the padding carries nothing and one spelling of a
// field is easier to be right about than two — a padded field is refused with a
// sentence saying so, which costs a client one round trip and never a wrong
// answer.
//
// The plain form is the ordinary one and is meant to be. Keys people actually
// have are text, and a body of them should be readable in a terminal without
// anything being decoded first.

// pair is one record on the wire.
//
// It is one struct rather than one per direction because the encoding is one
// rule: a route that writes a pair and a route that reads one have to agree
// about it exactly, and two structs would be two places to disagree. The fields
// a direction does not use are absent from it — Op and Expires are what a
// client sends and never what it is told, and omitempty is what keeps them out
// of an answer.
//
// The four bytes-bearing fields are pointers so that absent and present-and-
// empty are different things. They have to be: "both forms of one field is an
// error" is a claim about which fields are there, and {"key":"a","key_b64":""}
// is a client sending both. With plain strings that request would be
// indistinguishable from one sending only "key".
type pair struct {
	// Op is "write" or "delete", on the way in only.
	Op string `json:"op,omitempty"`

	Key    *string `json:"key,omitempty"`
	KeyB64 *string `json:"key_b64,omitempty"`

	Value    *string `json:"value,omitempty"`
	ValueB64 *string `json:"value_b64,omitempty"`

	// Expires is an RFC 3339 time, meaning exactly what the Litekv-Expires
	// header means on a PUT: the instant after which the record stops
	// answering. On the way in only.
	Expires string `json:"expires,omitempty"`
}

const (
	opWrite  = "write"
	opDelete = "delete"
)

// encodePair writes one key and value as a line of NDJSON.
//
// json.Encoder.Encode ends each value with a newline, which is the whole of the
// framing.
func encodePair(to *json.Encoder, key, value []byte) error {
	var out pair
	out.Key, out.KeyB64 = onTheWire(key)
	out.Value, out.ValueB64 = onTheWire(value)
	return to.Encode(&out)
}

// onTheWire picks which of a field's two forms some bytes go in, and fills that
// one in. Exactly one of the two is ever non-nil, including for no bytes at all:
// an empty value is a value, and the field being there and empty is what says so.
func onTheWire(b []byte) (plain, coded *string) {
	if utf8.Valid(b) {
		text := string(b)
		return &text, nil
	}

	encoded := base64.RawURLEncoding.EncodeToString(b)
	return nil, &encoded
}

// fromTheWire is the other direction: the bytes a field's two forms name,
// whichever of them was sent. field is the plain form's name and is only used
// to say what went wrong.
//
// The bytes come back as this call's own allocation every time — a string
// conversion or a base64 decode, never a window onto the buffer the line was
// read out of. That is not an accident and it is not free to change: a
// litekv.Batch does not copy the keys and values handed to it, it reads them
// when the batch is written, so everything a batch is built from has to stay
// alive and unchanged until WriteBatch returns. Handing it bytes that a decoder
// reuses for the next line would store the last line's key under every record
// in the batch, and it would do it silently.
func fromTheWire(plain, coded *string, field string) ([]byte, error) {
	switch {
	case plain != nil && coded != nil:
		return nil, fmt.Errorf("%s and %s_b64 are two spellings of one field; send one of them", field, field)

	case coded != nil:
		b, err := base64.RawURLEncoding.DecodeString(*coded)
		if err != nil {
			return nil, fmt.Errorf("%s_b64 is not base64url without padding", field)
		}
		return b, nil

	case plain != nil:
		return []byte(*plain), nil
	}

	// Neither, which is the empty key or the empty value. The store holds both,
	// and an absent field meaning an empty one is the same rule for keys as for
	// values rather than two.
	return nil, nil
}

// decodePair reads one line of a request body.
//
// Strictly, in three ways that all exist for the same reason — a client that
// got something slightly wrong should be told, not quietly obeyed:
//
//   - The line has to be valid UTF-8 before encoding/json sees it. This is the
//     other half of the rule above. A raw 0xff inside a JSON string is not JSON
//     at all, and the decoder would turn it into U+FFFD and carry on, which is
//     the silent data loss this whole file is arranged to prevent. Bytes that
//     are not text go in key_b64 and value_b64; that is what they are for.
//   - A field the struct does not have is refused. "vaule" would otherwise be a
//     write of an empty value, stored and acknowledged.
//   - A second JSON value on the line is refused. One object to a line is the
//     framing, and a line holding two of them means the sender is framing it
//     differently from the reader.
func decodePair(line []byte) (pair, error) {
	if !utf8.Valid(line) {
		return pair{}, errors.New("not valid UTF-8; bytes that are not text go in key_b64 and value_b64")
	}

	var out pair
	one := json.NewDecoder(bytes.NewReader(line))
	one.DisallowUnknownFields()

	if err := one.Decode(&out); err != nil {
		return pair{}, fmt.Errorf("not a JSON object: %w", err)
	}
	if one.More() {
		return pair{}, errors.New("more than one JSON value on the line")
	}
	return out, nil
}
