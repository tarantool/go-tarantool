package tarantool_test

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
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

func TestMessagePackEncodeNil(t *testing.T) {
	var val *BoxError

	_, err := val.MarshalMsgpack()
	require.NotNil(t, err)
	require.Equal(t, "msgpack: unexpected nil BoxError on encode", err.Error())
}

var space = "test_error_type"
var index = "primary"

type TupleBoxError struct {
	pk  string // BoxError cannot be used as a primary key.
	val BoxError
}

func (t *TupleBoxError) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}

	if err := e.EncodeString(t.pk); err != nil {
		return err
	}

	return e.Encode(&t.val)
}

func (t *TupleBoxError) DecodeMsgpack(d *decoder) error {
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
			buf, err := marshal(&testcase.tuple)
			require.Nil(t, err)

			var res TupleBoxError
			err = unmarshal(buf, &res)
			require.Nil(t, err)

			require.Equal(t, testcase.tuple, res)
		})
	}
}

func TestErrorTypeEval(t *testing.T) {
	test_helpers.SkipIfErrorMessagePackTypeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			resp, err := conn.Eval("return ...", []interface{}{&testcase.tuple.val})
			require.Nil(t, err)
			require.NotNil(t, resp.Data)
			require.Equal(t, len(resp.Data), 1)
			actual, ok := toBoxError(resp.Data[0])
			require.Truef(t, ok, "Response data has valid type")
			require.Equal(t, testcase.tuple.val, actual)
		})
	}
}

func TestErrorTypeEvalTyped(t *testing.T) {
	test_helpers.SkipIfErrorMessagePackTypeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			var res []BoxError
			err := conn.EvalTyped("return ...", []interface{}{&testcase.tuple.val}, &res)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, len(res), 1)
			require.Equal(t, testcase.tuple.val, res[0])
		})
	}
}

func TestErrorTypeInsert(t *testing.T) {
	test_helpers.SkipIfErrorMessagePackTypeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	truncateEval := fmt.Sprintf("box.space[%q]:truncate()", space)
	_, err := conn.Eval(truncateEval, []interface{}{})
	require.Nil(t, err)

	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			_, err = conn.Insert(space, &testcase.tuple)
			require.Nil(t, err)

			checkEval := fmt.Sprintf(`
				local err = rawget(_G, %q)
				assert(err ~= nil)

				local tuple = box.space[%q]:get(%q)
				assert(tuple ~= nil)

				local tuple_err = tuple[2]
				assert(tuple_err ~= nil)

				return compare_box_errors(err, tuple_err)
			`, testcase.ttObj, space, testcase.tuple.pk)

			// In fact, compare_box_errors does not check than File and Line
			// of connector BoxError are equal to the Tarantool ones
			// since they may differ between different Tarantool versions
			// and editions.
			_, err := conn.Eval(checkEval, []interface{}{})
			require.Nilf(t, err, "Tuple has been successfully inserted")
		})
	}
}

func TestErrorTypeInsertTyped(t *testing.T) {
	test_helpers.SkipIfErrorMessagePackTypeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	truncateEval := fmt.Sprintf("box.space[%q]:truncate()", space)
	_, err := conn.Eval(truncateEval, []interface{}{})
	require.Nil(t, err)

	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			var res []TupleBoxError
			err = conn.InsertTyped(space, &testcase.tuple, &res)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, len(res), 1)
			require.Equal(t, testcase.tuple, res[0])

			checkEval := fmt.Sprintf(`
				local err = rawget(_G, %q)
				assert(err ~= nil)

				local tuple = box.space[%q]:get(%q)
				assert(tuple ~= nil)

				local tuple_err = tuple[2]
				assert(tuple_err ~= nil)

				return compare_box_errors(err, tuple_err)
			`, testcase.ttObj, space, testcase.tuple.pk)

			// In fact, compare_box_errors does not check than File and Line
			// of connector BoxError are equal to the Tarantool ones
			// since they may differ between different Tarantool versions
			// and editions.
			_, err := conn.Eval(checkEval, []interface{}{})
			require.Nilf(t, err, "Tuple has been successfully inserted")
		})
	}
}

func TestErrorTypeSelect(t *testing.T) {
	test_helpers.SkipIfErrorMessagePackTypeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	truncateEval := fmt.Sprintf("box.space[%q]:truncate()", space)
	_, err := conn.Eval(truncateEval, []interface{}{})
	require.Nil(t, err)

	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			insertEval := fmt.Sprintf(`
				local err = rawget(_G, %q)
				assert(err ~= nil)

				local tuple = box.space[%q]:insert{%q, err}
				assert(tuple ~= nil)
			`, testcase.ttObj, space, testcase.tuple.pk)

			_, err := conn.Eval(insertEval, []interface{}{})
			require.Nilf(t, err, "Tuple has been successfully inserted")

			var resp *Response
			var offset uint32 = 0
			var limit uint32 = 1
			resp, err = conn.Select(space, index, offset, limit, IterEq, []interface{}{testcase.tuple.pk})
			require.Nil(t, err)
			require.NotNil(t, resp.Data)
			require.Equalf(t, len(resp.Data), 1, "Exactly one tuple had been found")
			tpl, ok := resp.Data[0].([]interface{})
			require.Truef(t, ok, "Tuple has valid type")
			require.Equal(t, testcase.tuple.pk, tpl[0])
			var actual BoxError
			actual, ok = toBoxError(tpl[1])
			require.Truef(t, ok, "BoxError tuple field has valid type")
			// In fact, CheckEqualBoxErrors does not check than File and Line
			// of connector BoxError are equal to the Tarantool ones
			// since they may differ between different Tarantool versions
			// and editions.
			test_helpers.CheckEqualBoxErrors(t, testcase.tuple.val, actual)
		})
	}
}

func TestErrorTypeSelectTyped(t *testing.T) {
	test_helpers.SkipIfErrorMessagePackTypeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	truncateEval := fmt.Sprintf("box.space[%q]:truncate()", space)
	_, err := conn.Eval(truncateEval, []interface{}{})
	require.Nil(t, err)

	for name, testcase := range tupleCases {
		t.Run(name, func(t *testing.T) {
			insertEval := fmt.Sprintf(`
				local err = rawget(_G, %q)
				assert(err ~= nil)

				local tuple = box.space[%q]:insert{%q, err}
				assert(tuple ~= nil)
			`, testcase.ttObj, space, testcase.tuple.pk)

			_, err := conn.Eval(insertEval, []interface{}{})
			require.Nilf(t, err, "Tuple has been successfully inserted")

			var offset uint32 = 0
			var limit uint32 = 1
			var resp []TupleBoxError
			err = conn.SelectTyped(space, index, offset, limit, IterEq, []interface{}{testcase.tuple.pk}, &resp)
			require.Nil(t, err)
			require.NotNil(t, resp)
			require.Equalf(t, len(resp), 1, "Exactly one tuple had been found")
			require.Equal(t, testcase.tuple.pk, resp[0].pk)
			// In fact, CheckEqualBoxErrors does not check than File and Line
			// of connector BoxError are equal to the Tarantool ones
			// since they may differ between different Tarantool versions
			// and editions.
			test_helpers.CheckEqualBoxErrors(t, testcase.tuple.val, resp[0].val)
		})
	}
}
