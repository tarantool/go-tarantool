package tarantool_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	. "github.com/tarantool/go-tarantool"
)

var samples = map[string]BoxError{
	"SimpleError": {
		Type:  "ClientError",
		File:  "config.lua",
		Line:  uint64(202),
		Msg:   "Unknown error",
		Errno: uint64(0),
		Code:  uint64(0),
	},
	"AccessDeniedError": {
		Type:  "AccessDeniedError",
		File:  "/__w/sdk/sdk/tarantool-2.10/tarantool/src/box/func.c",
		Line:  uint64(535),
		Msg:   "Execute access to function 'forbidden_function' is denied for user 'no_grants'",
		Errno: uint64(0),
		Code:  uint64(42),
		Fields: map[string]interface{}{
			"object_type": "function",
			"object_name": "forbidden_function",
			"access_type": "Execute",
		},
	},
	"ChainedError": {
		Type:  "ClientError",
		File:  "config.lua",
		Line:  uint64(205),
		Msg:   "Timeout exceeded",
		Errno: uint64(0),
		Code:  uint64(78),
		Prev: &BoxError{
			Type:  "ClientError",
			File:  "config.lua",
			Line:  uint64(202),
			Msg:   "Unknown error",
			Errno: uint64(0),
			Code:  uint64(0),
		},
	},
}

var stringCases = map[string]struct {
	e BoxError
	s string
}{
	"SimpleError": {
		samples["SimpleError"],
		"Unknown error (ClientError, code 0x0), see config.lua line 202",
	},
	"AccessDeniedError": {
		samples["AccessDeniedError"],
		"Execute access to function 'forbidden_function' is denied for user " +
			"'no_grants' (AccessDeniedError, code 0x2a), see " +
			"/__w/sdk/sdk/tarantool-2.10/tarantool/src/box/func.c line 535",
	},
	"ChainedError": {
		samples["ChainedError"],
		"Timeout exceeded (ClientError, code 0x4e), see config.lua line 205: " +
			"Unknown error (ClientError, code 0x0), see config.lua line 202",
	},
}

func TestBoxErrorStringRepr(t *testing.T) {
	for name, testcase := range stringCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testcase.s, testcase.e.Error())
		})
	}
}

var mpDecodeSamples = map[string]struct {
	b   []byte
	ok  bool
	err *regexp.Regexp
}{
	"OuterMapInvalidLen": {
		[]byte{0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding map length`),
	},
	"OuterMapInvalidKey": {
		[]byte{0x81, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding int64`),
	},
	"OuterMapExtraKey": {
		[]byte{0x82, 0x00, 0x91, 0x81, 0x02, 0x01, 0x11, 0x00},
		true,
		regexp.MustCompile(``),
	},
	"OuterMapExtraInvalidKey": {
		[]byte{0x81, 0x11, 0x81},
		false,
		regexp.MustCompile(`EOF`),
	},
	"ArrayInvalidLen": {
		[]byte{0x81, 0x00, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding array length`),
	},
	"ArrayZeroLen": {
		[]byte{0x81, 0x00, 0x90},
		false,
		regexp.MustCompile(`msgpack: unexpected empty BoxError stack on decode`),
	},
	"InnerMapInvalidLen": {
		[]byte{0x81, 0x00, 0x91, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding map length`),
	},
	"InnerMapInvalidKey": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding int64`),
	},
	"InnerMapInvalidErrorType": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x00, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding (?:string\/bytes|bytes) length`),
	},
	"InnerMapInvalidErrorFile": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x01, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding (?:string\/bytes|bytes) length`),
	},
	"InnerMapInvalidErrorLine": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x02, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding uint64`),
	},
	"InnerMapInvalidErrorMessage": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x03, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding (?:string\/bytes|bytes) length`),
	},
	"InnerMapInvalidErrorErrno": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x04, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding uint64`),
	},
	"InnerMapInvalidErrorErrcode": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x05, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding uint64`),
	},
	"InnerMapInvalidErrorFields": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x06, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding map length`),
	},
	"InnerMapInvalidErrorFieldsKey": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x06, 0x81, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding (?:string\/bytes|bytes) length`),
	},
	"InnerMapInvalidErrorFieldsValue": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x06, 0x81, 0xa3, 0x6b, 0x65, 0x79, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding interface{}`),
	},
	"InnerMapExtraKey": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x21, 0x00},
		true,
		regexp.MustCompile(``),
	},
	"InnerMapExtraInvalidKey": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x21, 0x81},
		false,
		regexp.MustCompile(`EOF`),
	},
}

func TestMessagePackDecode(t *testing.T) {
	for name, testcase := range mpDecodeSamples {
		t.Run(name, func(t *testing.T) {
			var val *BoxError = &BoxError{}
			err := val.UnmarshalMsgpack(testcase.b)
			if testcase.ok {
				require.Nilf(t, err, "No errors on decode")
			} else {
				require.Regexp(t, testcase.err, err.Error())
			}
		})
	}
}

func TestMessagePackUnmarshalToNil(t *testing.T) {
	var val *BoxError = nil
	require.PanicsWithValue(t, "cannot unmarshal to a nil pointer",
		func() { val.UnmarshalMsgpack(mpDecodeSamples["InnerMapExtraKey"].b) })
}
