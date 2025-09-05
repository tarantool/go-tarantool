package uuid_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"

	. "github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
	_ "github.com/tarantool/go-tarantool/v3/uuid"
)

// There is no way to skip tests in testing.M,
// so we use this variable to pass info
// to each testing.T that it should skip.
var isUUIDSupported = false

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 5 * time.Second,
}
var dialer = NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

var space = "testUUID"
var index = "primary"

type TupleUUID struct {
	id uuid.UUID
}

func (t *TupleUUID) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	t.id = res.(uuid.UUID)
	return nil
}

func tupleValueIsId(t *testing.T, tuples []interface{}, id uuid.UUID) {
	if len(tuples) != 1 {
		t.Fatalf("Response Data len != 1")
	}

	if tpl, ok := tuples[0].([]interface{}); !ok {
		t.Errorf("Unexpected return value body")
	} else {
		if len(tpl) != 1 {
			t.Errorf("Unexpected return value body (tuple len)")
		}
		if val, ok := tpl[0].(uuid.UUID); !ok || val != id {
			t.Errorf("Unexpected return value body (tuple 0 field)")
		}
	}
}

func TestSelect(t *testing.T) {
	if isUUIDSupported == false {
		t.Skip("Skipping test for Tarantool without UUID support in msgpack")
	}

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	id, uuidErr := uuid.Parse("c8f0fa1f-da29-438c-a040-393f1126ad39")
	if uuidErr != nil {
		t.Fatalf("Failed to prepare test uuid: %s", uuidErr)
	}

	sel := NewSelectRequest(space).
		Index(index).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{id})
	data, errSel := conn.Do(sel).Get()
	if errSel != nil {
		t.Fatalf("UUID select failed: %s", errSel.Error())
	}
	tupleValueIsId(t, data, id)

	var tuples []TupleUUID
	errTyp := conn.Do(sel).GetTyped(&tuples)
	if errTyp != nil {
		t.Fatalf("Failed to SelectTyped: %s", errTyp.Error())
	}
	if len(tuples) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	}
	if tuples[0].id != id {
		t.Errorf("Bad value loaded from SelectTyped: %s", tuples[0].id)
	}
}

func TestReplace(t *testing.T) {
	if isUUIDSupported == false {
		t.Skip("Skipping test for Tarantool without UUID support in msgpack")
	}

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	id, uuidErr := uuid.Parse("64d22e4d-ac92-4a23-899a-e59f34af5479")
	if uuidErr != nil {
		t.Errorf("Failed to prepare test uuid: %s", uuidErr)
	}

	rep := NewReplaceRequest(space).Tuple([]interface{}{id})
	dataRep, errRep := conn.Do(rep).Get()
	if errRep != nil {
		t.Errorf("UUID replace failed: %s", errRep)
	}
	tupleValueIsId(t, dataRep, id)

	sel := NewSelectRequest(space).
		Index(index).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{id})
	dataSel, errSel := conn.Do(sel).Get()
	if errSel != nil {
		t.Errorf("UUID select failed: %s", errSel)
	}
	tupleValueIsId(t, dataSel, id)
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 4, 1)
	if err != nil {
		log.Fatalf("Failed to extract tarantool version: %s", err)
	}

	if isLess {
		log.Println("Skipping UUID tests...")
		isUUIDSupported = false
		return m.Run()
	} else {
		isUUIDSupported = true
	}

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Printf("Failed to prepare test tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
