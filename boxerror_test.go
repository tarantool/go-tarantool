package tarantool_test

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	. "github.com/tarantool/go-tarantool/v3"
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
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1` +
			` decoding (?:string\/bytes|bytes) length`),
	},
	"InnerMapInvalidErrorFile": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x01, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1` +
			` decoding (?:string\/bytes|bytes) length`),
	},
	"InnerMapInvalidErrorLine": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x02, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding uint64`),
	},
	"InnerMapInvalidErrorMessage": {
		[]byte{0x81, 0x00, 0x91, 0x81, 0x03, 0xc1},
		false,
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1 decoding` +
			` (?:string\/bytes|bytes) length`),
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
		regexp.MustCompile(`msgpack: (?:unexpected|invalid|unknown) code.c1` +
			` decoding (?:string\/bytes|bytes) length`),
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
			var val = &BoxError{}
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

func TestMessagePackEncodeNil(t *testing.T) {
	var val *BoxError

	_, err := val.MarshalMsgpack()
	require.NotNil(t, err)
	require.Equal(t, "msgpack: unexpected nil BoxError on encode", err.Error())
}

type TupleBoxError struct {
	pk  string // BoxError cannot be used as a primary key.
	val BoxError
}

func (t *TupleBoxError) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}

	if err := e.EncodeString(t.pk); err != nil {
		return err
	}

	return e.Encode(&t.val)
}

func (t *TupleBoxError) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 2 {
		return fmt.Errorf("Array length doesn't match: %d", l)
	}

	if t.pk, err = d.DecodeString(); err != nil {
		return err
	}

	return d.Decode(&t.val)
}

// Raw bytes encoding test is impossible for
// object with Fields since map iterating is random.
var tupleCases = map[string]struct {
	tuple TupleBoxError
	ttObj string
}{
	"SimpleError": {
		TupleBoxError{
			"simple_error_pk",
			samples["SimpleError"],
		},
		"simple_error",
	},
	"AccessDeniedError": {
		TupleBoxError{
			"access_denied_error_pk",
			samples["AccessDeniedError"],
		},
		"access_denied_error",
	},
	"ChainedError": {
		TupleBoxError{
			"chained_error_pk",
			samples["ChainedError"],
		},
		"chained_error",
	},
}

func TestErrorTypeMPEncodeDecode(t *testing.T) {
	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			buf, err := msgpack.Marshal(&testcase.tuple)
			require.Nil(t, err)

			var res TupleBoxError
			err = msgpack.Unmarshal(buf, &res)
			require.Nil(t, err)

			require.Equal(t, testcase.tuple, res)
		})
	}
}
