package decimal_test

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	. "github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/decimal"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

var isDecimalSupported = false

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

func skipIfDecimalUnsupported(t *testing.T) {
	t.Helper()

	if isDecimalSupported == false {
		t.Skip("Skipping test for Tarantool without decimal support in msgpack")
	}
}

var space = "testDecimal"
var index = "primary"

type TupleDecimal struct {
	number Decimal
}

func (t *TupleDecimal) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(1); err != nil {
		return err
	}
	return e.EncodeValue(reflect.ValueOf(&t.number))
}

func (t *TupleDecimal) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("Array length doesn't match: %d", l)
	}

	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	var ok bool
	if t.number, ok = toDecimal(res); !ok {
		return fmt.Errorf("decimal doesn't match")
	}
	return nil
}

var benchmarkSamples = []struct {
	numString string
	mpBuf     string
	fixExt    bool
}{
	{"0.7", "d501017c", true},
	{"0.3", "d501013c", true},
	{"0.00000000000000000000000000000000000001", "d501261c", true},
	{"0.00000000000000000000000000000000000009", "d501269c", true},
	{"-18.34", "d6010201834d", true},
	{"-108.123456789", "d701090108123456789d", true},
	{"-11111111111111111111111111111111111111", "c7150100011111111111111111111111111111111111111d", false},
}

var correctnessSamples = []struct {
	numString string
	mpBuf     string
	fixExt    bool
}{
	{"100", "c7030100100c", false},
	{"0.1", "d501011c", true},
	{"-0.1", "d501011d", true},
	{"0.0000000000000000000000000000000000001", "d501251c", true},
	{"-0.0000000000000000000000000000000000001", "d501251d", true},
	{"0.00000000000000000000000000000000000001", "d501261c", true},
	{"-0.00000000000000000000000000000000000001", "d501261d", true},
	{"1", "d501001c", true},
	{"-1", "d501001d", true},
	{"0", "d501000c", true},
	{"-0", "d501000c", true},
	{"0.01", "d501021c", true},
	{"0.001", "d501031c", true},
	{"99999999999999999999999999999999999999", "c7150100099999999999999999999999999999999999999c", false},
	{"-99999999999999999999999999999999999999", "c7150100099999999999999999999999999999999999999d", false},
	{"-12.34", "d6010201234d", true},
	{"12.34", "d6010201234c", true},
	{"1.4", "c7030101014c", false},
	{"2.718281828459045", "c70a010f02718281828459045c", false},
	{"-2.718281828459045", "c70a010f02718281828459045d", false},
	{"3.141592653589793", "c70a010f03141592653589793c", false},
	{"-3.141592653589793", "c70a010f03141592653589793d", false},
	{"1234567891234567890.0987654321987654321", "c7150113012345678912345678900987654321987654321c", false},
	{"-1234567891234567890.0987654321987654321", "c7150113012345678912345678900987654321987654321d", false},
}

// There is a difference between encoding result from a raw string and from
// decimal.Decimal. It's expected because decimal.Decimal simplifies decimals:
// 0.00010000 -> 0.0001

var rawSamples = []struct {
	numString string
	mpBuf     string
	fixExt    bool
}{
	{"0.000000000000000000000000000000000010", "c7030124010c", false},
	{"0.010", "c7030103010c", false},
	{"123.456789000000000", "c70b010f0123456789000000000c", false},
}

var decimalSamples = []struct {
	numString string
	mpBuf     string
	fixExt    bool
}{
	{"0.000000000000000000000000000000000010", "d501231c", true},
	{"0.010", "d501021c", true},
	{"123.456789000000000", "c7060106123456789c", false},
}

func TestMPEncodeDecode(t *testing.T) {
	for _, testcase := range benchmarkSamples {
		t.Run(testcase.numString, func(t *testing.T) {
			decNum, err := NewDecimalFromString(testcase.numString)
			if err != nil {
				t.Fatal(err)
			}
			var buf []byte
			tuple := TupleDecimal{number: *decNum}
			if buf, err = marshal(&tuple); err != nil {
				t.Fatalf("Failed to encode decimal number '%s' to a MessagePack buffer: %s", testcase.numString, err)
			}
			var v TupleDecimal
			if err = unmarshal(buf, &v); err != nil {
				t.Fatalf("Failed to decode MessagePack buffer '%x' to a decimal number: %s", buf, err)
			}
			if !decNum.Equal(v.number.Decimal) {
				fmt.Println(decNum)
				fmt.Println(v.number)
				t.Fatal("Decimal numbers are not equal")
			}
		})
	}
}

var lengthSamples = []struct {
	numString string
	length    int
}{
	{"0.010", 2},
	{"0.01", 1},
	{"-0.1", 1},
	{"0.1", 1},
	{"0", 1},
	{"00.1", 1},
	{"100", 3},
	{"0100", 3},
	{"+1", 1},
	{"-1", 1},
	{"1", 1},
	{"-12.34", 4},
	{"123.456789000000000", 18},
}

func TestGetNumberLength(t *testing.T) {
	for _, testcase := range lengthSamples {
		t.Run(testcase.numString, func(t *testing.T) {
			l := GetNumberLength(testcase.numString)
			if l != testcase.length {
				t.Fatalf("Length is wrong: correct %d, incorrect %d", testcase.length, l)
			}
		})
	}

	if l := GetNumberLength(""); l != 0 {
		t.Fatalf("Length is wrong: correct 0, incorrect %d", l)
	}

	if l := GetNumberLength("0"); l != 1 {
		t.Fatalf("Length is wrong: correct 0, incorrect %d", l)
	}

	if l := GetNumberLength("10"); l != 2 {
		t.Fatalf("Length is wrong: correct 0, incorrect %d", l)
	}
}

func TestEncodeStringToBCDIncorrectNumber(t *testing.T) {
	referenceErrMsg := "Number contains more than one point"
	var numString = "0.1.0"
	buf, err := EncodeStringToBCD(numString)
	if err == nil {
		t.Fatalf("no error on encoding a string with incorrect number")
	}
	if buf != nil {
		t.Fatalf("buf is not nil on encoding of a string with double points")
	}
	if err.Error() != referenceErrMsg {
		t.Fatalf("wrong error message on encoding of a string double points")
	}

	referenceErrMsg = "Length of number is zero"
	numString = ""
	buf, err = EncodeStringToBCD(numString)
	if err == nil {
		t.Fatalf("no error on encoding of an empty string")
	}
	if buf != nil {
		t.Fatalf("buf is not nil on encoding of an empty string")
	}
	if err.Error() != referenceErrMsg {
		t.Fatalf("wrong error message on encoding of an empty string")
	}

	referenceErrMsg = "Failed to convert symbol 'a' to a digit"
	numString = "0.1a"
	buf, err = EncodeStringToBCD(numString)
	if err == nil {
		t.Fatalf("no error on encoding of a string number with non-digit symbol")
	}
	if buf != nil {
		t.Fatalf("buf is not nil on encoding of a string number with non-digit symbol")
	}
	if err.Error() != referenceErrMsg {
		t.Fatalf("wrong error message on encoding of a string number with non-digit symbol")
	}
}

func TestEncodeMaxNumber(t *testing.T) {
	referenceErrMsg := "msgpack: decimal number is bigger than maximum supported number (10^38 - 1)"
	decNum := decimal.New(1, DecimalPrecision) // // 10^DecimalPrecision
	tuple := TupleDecimal{number: *NewDecimal(decNum)}
	_, err := marshal(&tuple)
	if err == nil {
		t.Fatalf("It is possible to encode a number unsupported by Tarantool")
	}
	if err.Error() != referenceErrMsg {
		t.Fatalf("Incorrect error message on attempt to encode number unsupported by Tarantool")
	}
}

func TestEncodeMinNumber(t *testing.T) {
	referenceErrMsg := "msgpack: decimal number is lesser than minimum supported number (-10^38 - 1)"
	two := decimal.NewFromInt(2)
	decNum := decimal.New(1, DecimalPrecision).Neg().Sub(two) // -10^DecimalPrecision - 2
	tuple := TupleDecimal{number: *NewDecimal(decNum)}
	_, err := marshal(&tuple)
	if err == nil {
		t.Fatalf("It is possible to encode a number unsupported by Tarantool")
	}
	if err.Error() != referenceErrMsg {
		fmt.Println("Actual message:   ", err.Error())
		fmt.Println("Expected message: ", referenceErrMsg)
		t.Fatalf("Incorrect error message on attempt to encode number unsupported by Tarantool")
	}
}

func benchmarkMPEncodeDecode(b *testing.B, src decimal.Decimal, dst interface{}) {
	b.ResetTimer()

	var v TupleDecimal
	var buf []byte
	var err error
	for i := 0; i < b.N; i++ {
		tuple := TupleDecimal{number: *NewDecimal(src)}
		if buf, err = marshal(&tuple); err != nil {
			b.Fatal(err)
		}
		if err = unmarshal(buf, &v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMPEncodeDecodeDecimal(b *testing.B) {
	for _, testcase := range benchmarkSamples {
		b.Run(testcase.numString, func(b *testing.B) {
			dec, err := decimal.NewFromString(testcase.numString)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkMPEncodeDecode(b, dec, &dec)
		})
	}
}

func BenchmarkMPEncodeDecimal(b *testing.B) {
	for _, testcase := range benchmarkSamples {
		b.Run(testcase.numString, func(b *testing.B) {
			decNum, err := NewDecimalFromString(testcase.numString)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				marshal(decNum)
			}
		})
	}
}

func BenchmarkMPDecodeDecimal(b *testing.B) {
	for _, testcase := range benchmarkSamples {
		b.Run(testcase.numString, func(b *testing.B) {
			decNum, err := NewDecimalFromString(testcase.numString)
			if err != nil {
				b.Fatal(err)
			}
			var buf []byte
			if buf, err = marshal(decNum); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			var v TupleDecimal
			for i := 0; i < b.N; i++ {
				unmarshal(buf, &v)
			}

		})
	}
}

func tupleValueIsDecimal(t *testing.T, tuples []interface{}, number decimal.Decimal) {
	if len(tuples) != 1 {
		t.Fatalf("Response Data len (%d) != 1", len(tuples))
	}

	if tpl, ok := tuples[0].([]interface{}); !ok {
		t.Fatalf("Unexpected return value body")
	} else {
		if len(tpl) != 1 {
			t.Fatalf("Unexpected return value body (tuple len)")
		}
		if val, ok := toDecimal(tpl[0]); !ok || !val.Equal(number) {
			t.Fatalf("Unexpected return value body (tuple 0 field)")
		}
	}
}

func trimMPHeader(mpBuf []byte, fixExt bool) []byte {
	mpHeaderLen := 2
	if fixExt == false {
		mpHeaderLen = 3
	}
	return mpBuf[mpHeaderLen:]
}

func TestEncodeStringToBCD(t *testing.T) {
	samples := append(correctnessSamples, rawSamples...)
	samples = append(samples, benchmarkSamples...)
	for _, testcase := range samples {
		t.Run(testcase.numString, func(t *testing.T) {
			buf, err := EncodeStringToBCD(testcase.numString)
			if err != nil {
				t.Fatalf("Failed to encode decimal '%s' to BCD: %s", testcase.numString, err)

			}
			b, _ := hex.DecodeString(testcase.mpBuf)
			bcdBuf := trimMPHeader(b, testcase.fixExt)
			if reflect.DeepEqual(buf, bcdBuf) != true {
				t.Fatalf("Failed to encode decimal '%s' to BCD: expected '%x', actual '%x'", testcase.numString, bcdBuf, buf)
			}
		})
	}
}

func TestDecodeStringFromBCD(t *testing.T) {
	samples := append(correctnessSamples, rawSamples...)
	samples = append(samples, benchmarkSamples...)
	for _, testcase := range samples {
		t.Run(testcase.numString, func(t *testing.T) {
			b, _ := hex.DecodeString(testcase.mpBuf)
			bcdBuf := trimMPHeader(b, testcase.fixExt)
			s, err := DecodeStringFromBCD(bcdBuf)
			if err != nil {
				t.Fatalf("Failed to decode BCD '%x' to decimal: %s", bcdBuf, err)
			}

			decActual, err := decimal.NewFromString(s)
			if err != nil {
				t.Fatalf("Failed to encode string ('%s') to decimal", s)
			}
			decExpected, err := decimal.NewFromString(testcase.numString)
			if err != nil {
				t.Fatalf("Failed to encode string ('%s') to decimal", testcase.numString)
			}
			if !decExpected.Equal(decActual) {
				t.Fatalf("Decoded decimal from BCD ('%x') is incorrect: expected '%s', actual '%s'", bcdBuf, testcase.numString, s)
			}
		})
	}
}

func TestMPEncode(t *testing.T) {
	samples := append(correctnessSamples, decimalSamples...)
	samples = append(samples, benchmarkSamples...)
	for _, testcase := range samples {
		t.Run(testcase.numString, func(t *testing.T) {
			dec, err := NewDecimalFromString(testcase.numString)
			if err != nil {
				t.Fatalf("NewDecimalFromString() failed: %s", err.Error())
			}
			buf, err := marshal(dec)
			if err != nil {
				t.Fatalf("Marshalling failed: %s", err.Error())
			}
			refBuf, _ := hex.DecodeString(testcase.mpBuf)
			if reflect.DeepEqual(buf, refBuf) != true {
				t.Fatalf("Failed to encode decimal '%s', actual %x, expected %x",
					testcase.numString,
					buf,
					refBuf)
			}
		})
	}
}

func TestMPDecode(t *testing.T) {
	samples := append(correctnessSamples, decimalSamples...)
	samples = append(samples, benchmarkSamples...)
	for _, testcase := range samples {
		t.Run(testcase.numString, func(t *testing.T) {
			mpBuf, err := hex.DecodeString(testcase.mpBuf)
			if err != nil {
				t.Fatalf("hex.DecodeString() failed: %s", err)
			}
			var v interface{}
			err = unmarshal(mpBuf, &v)
			if err != nil {
				t.Fatalf("Unmarshalling failed: %s", err.Error())
			}
			decActual, ok := toDecimal(v)
			if !ok {
				t.Fatalf("Unable to convert to Decimal")
			}

			decExpected, err := decimal.NewFromString(testcase.numString)
			if err != nil {
				t.Fatalf("decimal.NewFromString() failed: %s", err.Error())
			}
			if !decExpected.Equal(decActual.Decimal) {
				t.Fatalf("Decoded decimal ('%s') is incorrect", testcase.mpBuf)
			}
		})
	}
}

func BenchmarkEncodeStringToBCD(b *testing.B) {
	for _, testcase := range benchmarkSamples {
		b.Run(testcase.numString, func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				EncodeStringToBCD(testcase.numString)
			}
		})
	}
}

func BenchmarkDecodeStringFromBCD(b *testing.B) {
	for _, testcase := range benchmarkSamples {
		b.Run(testcase.numString, func(b *testing.B) {
			buf, _ := hex.DecodeString(testcase.mpBuf)
			bcdBuf := trimMPHeader(buf, testcase.fixExt)
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				DecodeStringFromBCD(bcdBuf)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	skipIfDecimalUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	number, err := decimal.NewFromString("-12.34")
	if err != nil {
		t.Fatalf("Failed to prepare test decimal: %s", err)
	}

	resp, err := conn.Insert(space, []interface{}{NewDecimal(number)})
	if err != nil {
		t.Fatalf("Decimal insert failed: %s", err)
	}
	if resp == nil {
		t.Fatalf("Response is nil after Replace")
	}
	tupleValueIsDecimal(t, resp.Data, number)

	var offset uint32 = 0
	var limit uint32 = 1
	resp, err = conn.Select(space, index, offset, limit, IterEq, []interface{}{NewDecimal(number)})
	if err != nil {
		t.Fatalf("Decimal select failed: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	tupleValueIsDecimal(t, resp.Data, number)

	resp, err = conn.Delete(space, index, []interface{}{NewDecimal(number)})
	if err != nil {
		t.Fatalf("Decimal delete failed: %s", err)
	}
	tupleValueIsDecimal(t, resp.Data, number)
}

func assertInsert(t *testing.T, conn *Connection, numString string) {
	number, err := decimal.NewFromString(numString)
	if err != nil {
		t.Fatalf("Failed to prepare test decimal: %s", err)
	}

	resp, err := conn.Insert(space, []interface{}{NewDecimal(number)})
	if err != nil {
		t.Fatalf("Decimal insert failed: %s", err)
	}
	if resp == nil {
		t.Fatalf("Response is nil after Replace")
	}
	tupleValueIsDecimal(t, resp.Data, number)

	resp, err = conn.Delete(space, index, []interface{}{NewDecimal(number)})
	if err != nil {
		t.Fatalf("Decimal delete failed: %s", err)
	}
	tupleValueIsDecimal(t, resp.Data, number)
}

func TestInsert(t *testing.T) {
	skipIfDecimalUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	samples := append(correctnessSamples, benchmarkSamples...)
	for _, testcase := range samples {
		t.Run(testcase.numString, func(t *testing.T) {
			assertInsert(t, conn, testcase.numString)
		})
	}
}

func TestReplace(t *testing.T) {
	skipIfDecimalUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	number, err := decimal.NewFromString("-12.34")
	if err != nil {
		t.Fatalf("Failed to prepare test decimal: %s", err)
	}

	respRep, errRep := conn.Replace(space, []interface{}{NewDecimal(number)})
	if errRep != nil {
		t.Fatalf("Decimal replace failed: %s", errRep)
	}
	if respRep == nil {
		t.Fatalf("Response is nil after Replace")
	}
	tupleValueIsDecimal(t, respRep.Data, number)

	respSel, errSel := conn.Select(space, index, 0, 1, IterEq, []interface{}{NewDecimal(number)})
	if errSel != nil {
		t.Fatalf("Decimal select failed: %s", errSel)
	}
	if respSel == nil {
		t.Fatalf("Response is nil after Select")
	}
	tupleValueIsDecimal(t, respSel.Data, number)
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 2, 0)
	if err != nil {
		log.Fatalf("Failed to extract Tarantool version: %s", err)
	}

	if isLess {
		log.Println("Skipping decimal tests...")
		isDecimalSupported = false
		return m.Run()
	} else {
		isDecimalSupported = true
	}

	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   "config.lua",
		Listen:       server,
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Fatalf("Failed to prepare test Tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
