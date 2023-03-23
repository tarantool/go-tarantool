package tarantool_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

const invalidSpaceMsg = "invalid space"
const invalidIndexMsg = "invalid index"

const invalidSpace = 2
const invalidIndex = 2
const validSpace = 1           // Any valid value != default.
const validIndex = 3           // Any valid value != default.
const validExpr = "any string" // We don't check the value here.
const validKey = "foo"         // Any string.
const defaultSpace = 0         // And valid too.
const defaultIndex = 0         // And valid too.

const defaultIsolationLevel = DefaultIsolationLevel
const defaultTimeout = 0

const validTimeout = 500 * time.Millisecond

var validStmt *Prepared = &Prepared{StatementID: 1, Conn: &Connection{}}

var validProtocolInfo ProtocolInfo = ProtocolInfo{
	Version:  ProtocolVersion(3),
	Features: []ProtocolFeature{StreamsFeature},
}

type ValidSchemeResolver struct {
}

func (*ValidSchemeResolver) ResolveSpaceIndex(s, i interface{}) (spaceNo, indexNo uint32, err error) {
	if s != nil {
		spaceNo = uint32(s.(int))
	} else {
		spaceNo = defaultSpace
	}
	if i != nil {
		indexNo = uint32(i.(int))
	} else {
		indexNo = defaultIndex
	}
	if spaceNo == invalidSpace {
		return 0, 0, errors.New(invalidSpaceMsg)
	}
	if indexNo == invalidIndex {
		return 0, 0, errors.New(invalidIndexMsg)
	}
	return spaceNo, indexNo, nil
}

var resolver ValidSchemeResolver

func assertBodyCall(t testing.TB, requests []Request, errorMsg string) {
	t.Helper()

	const errBegin = "An unexpected Request.Body() "
	for _, req := range requests {
		var reqBuf bytes.Buffer
		enc := NewEncoder(&reqBuf)

		err := req.Body(&resolver, enc)
		if err != nil && errorMsg != "" && err.Error() != errorMsg {
			t.Errorf(errBegin+"error %q expected %q", err.Error(), errorMsg)
		}
		if err != nil && errorMsg == "" {
			t.Errorf(errBegin+"error %q", err.Error())
		}
		if err == nil && errorMsg != "" {
			t.Errorf(errBegin+"result, expected error %q", errorMsg)
		}
	}
}

func assertBodyEqual(t testing.TB, reference []byte, req Request) {
	t.Helper()

	reqBody, err := test_helpers.ExtractRequestBody(req, &resolver, NewEncoder)
	if err != nil {
		t.Fatalf("An unexpected Response.Body() error: %q", err.Error())
	}

	if !bytes.Equal(reqBody, reference) {
		t.Errorf("Encoded request %v != reference %v", reqBody, reference)
	}
}

func getTestOps() ([]Op, *Operations) {
	ops := []Op{
		{"+", 1, 2},
		{"-", 3, 4},
		{"&", 5, 6},
		{"|", 7, 8},
		{"^", 9, 1},
		{"^", 9, 1}, // The duplication is for test purposes.
		{":", 2, 3},
		{"!", 4, 5},
		{"#", 6, 7},
		{"=", 8, 9},
	}
	operations := NewOperations().
		Add(1, 2).
		Subtract(3, 4).
		BitwiseAnd(5, 6).
		BitwiseOr(7, 8).
		BitwiseXor(9, 1).
		BitwiseXor(9, 1). // The duplication is for test purposes.
		Splice(2, 3).
		Insert(4, 5).
		Delete(6, 7).
		Assign(8, 9)
	return ops, operations
}

func TestRequestsValidSpaceAndIndex(t *testing.T) {
	requests := []Request{
		NewSelectRequest(validSpace),
		NewSelectRequest(validSpace).Index(validIndex),
		NewUpdateRequest(validSpace),
		NewUpdateRequest(validSpace).Index(validIndex),
		NewUpsertRequest(validSpace),
		NewInsertRequest(validSpace),
		NewReplaceRequest(validSpace),
		NewDeleteRequest(validSpace),
		NewDeleteRequest(validSpace).Index(validIndex),
	}

	assertBodyCall(t, requests, "")
}

func TestRequestsInvalidSpace(t *testing.T) {
	requests := []Request{
		NewSelectRequest(invalidSpace).Index(validIndex),
		NewSelectRequest(invalidSpace),
		NewUpdateRequest(invalidSpace).Index(validIndex),
		NewUpdateRequest(invalidSpace),
		NewUpsertRequest(invalidSpace),
		NewInsertRequest(invalidSpace),
		NewReplaceRequest(invalidSpace),
		NewDeleteRequest(invalidSpace).Index(validIndex),
		NewDeleteRequest(invalidSpace),
	}

	assertBodyCall(t, requests, invalidSpaceMsg)
}

func TestRequestsInvalidIndex(t *testing.T) {
	requests := []Request{
		NewSelectRequest(validSpace).Index(invalidIndex),
		NewUpdateRequest(validSpace).Index(invalidIndex),
		NewDeleteRequest(validSpace).Index(invalidIndex),
	}

	assertBodyCall(t, requests, invalidIndexMsg)
}

func TestRequestsCodes(t *testing.T) {
	tests := []struct {
		req  Request
		code int32
	}{
		{req: NewSelectRequest(validSpace), code: SelectRequestCode},
		{req: NewUpdateRequest(validSpace), code: UpdateRequestCode},
		{req: NewUpsertRequest(validSpace), code: UpsertRequestCode},
		{req: NewInsertRequest(validSpace), code: InsertRequestCode},
		{req: NewReplaceRequest(validSpace), code: ReplaceRequestCode},
		{req: NewDeleteRequest(validSpace), code: DeleteRequestCode},
		{req: NewCall16Request(validExpr), code: Call16RequestCode},
		{req: NewCall17Request(validExpr), code: Call17RequestCode},
		{req: NewEvalRequest(validExpr), code: EvalRequestCode},
		{req: NewExecuteRequest(validExpr), code: ExecuteRequestCode},
		{req: NewPingRequest(), code: PingRequestCode},
		{req: NewPrepareRequest(validExpr), code: PrepareRequestCode},
		{req: NewUnprepareRequest(validStmt), code: PrepareRequestCode},
		{req: NewExecutePreparedRequest(validStmt), code: ExecuteRequestCode},
		{req: NewBeginRequest(), code: BeginRequestCode},
		{req: NewCommitRequest(), code: CommitRequestCode},
		{req: NewRollbackRequest(), code: RollbackRequestCode},
		{req: NewIdRequest(validProtocolInfo), code: IdRequestCode},
		{req: NewBroadcastRequest(validKey), code: CallRequestCode},
	}

	for _, test := range tests {
		if code := test.req.Code(); code != test.code {
			t.Errorf("An invalid request code 0x%x, expected 0x%x", code, test.code)
		}
	}
}

func TestRequestsAsync(t *testing.T) {
	tests := []struct {
		req   Request
		async bool
	}{
		{req: NewSelectRequest(validSpace), async: false},
		{req: NewUpdateRequest(validSpace), async: false},
		{req: NewUpsertRequest(validSpace), async: false},
		{req: NewInsertRequest(validSpace), async: false},
		{req: NewReplaceRequest(validSpace), async: false},
		{req: NewDeleteRequest(validSpace), async: false},
		{req: NewCall16Request(validExpr), async: false},
		{req: NewCall17Request(validExpr), async: false},
		{req: NewEvalRequest(validExpr), async: false},
		{req: NewExecuteRequest(validExpr), async: false},
		{req: NewPingRequest(), async: false},
		{req: NewPrepareRequest(validExpr), async: false},
		{req: NewUnprepareRequest(validStmt), async: false},
		{req: NewExecutePreparedRequest(validStmt), async: false},
		{req: NewBeginRequest(), async: false},
		{req: NewCommitRequest(), async: false},
		{req: NewRollbackRequest(), async: false},
		{req: NewIdRequest(validProtocolInfo), async: false},
		{req: NewBroadcastRequest(validKey), async: false},
	}

	for _, test := range tests {
		if async := test.req.Async(); async != test.async {
			t.Errorf("An invalid async %t, expected %t", async, test.async)
		}
	}
}

func TestRequestsCtx_default(t *testing.T) {
	tests := []struct {
		req      Request
		expected context.Context
	}{
		{req: NewSelectRequest(validSpace), expected: nil},
		{req: NewUpdateRequest(validSpace), expected: nil},
		{req: NewUpsertRequest(validSpace), expected: nil},
		{req: NewInsertRequest(validSpace), expected: nil},
		{req: NewReplaceRequest(validSpace), expected: nil},
		{req: NewDeleteRequest(validSpace), expected: nil},
		{req: NewCall16Request(validExpr), expected: nil},
		{req: NewCall17Request(validExpr), expected: nil},
		{req: NewEvalRequest(validExpr), expected: nil},
		{req: NewExecuteRequest(validExpr), expected: nil},
		{req: NewPingRequest(), expected: nil},
		{req: NewPrepareRequest(validExpr), expected: nil},
		{req: NewUnprepareRequest(validStmt), expected: nil},
		{req: NewExecutePreparedRequest(validStmt), expected: nil},
		{req: NewBeginRequest(), expected: nil},
		{req: NewCommitRequest(), expected: nil},
		{req: NewRollbackRequest(), expected: nil},
		{req: NewIdRequest(validProtocolInfo), expected: nil},
		{req: NewBroadcastRequest(validKey), expected: nil},
	}

	for _, test := range tests {
		if ctx := test.req.Ctx(); ctx != test.expected {
			t.Errorf("An invalid ctx %t, expected %t", ctx, test.expected)
		}
	}
}

func TestRequestsCtx_setter(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		req      Request
		expected context.Context
	}{
		{req: NewSelectRequest(validSpace).Context(ctx), expected: ctx},
		{req: NewUpdateRequest(validSpace).Context(ctx), expected: ctx},
		{req: NewUpsertRequest(validSpace).Context(ctx), expected: ctx},
		{req: NewInsertRequest(validSpace).Context(ctx), expected: ctx},
		{req: NewReplaceRequest(validSpace).Context(ctx), expected: ctx},
		{req: NewDeleteRequest(validSpace).Context(ctx), expected: ctx},
		{req: NewCall16Request(validExpr).Context(ctx), expected: ctx},
		{req: NewCall17Request(validExpr).Context(ctx), expected: ctx},
		{req: NewEvalRequest(validExpr).Context(ctx), expected: ctx},
		{req: NewExecuteRequest(validExpr).Context(ctx), expected: ctx},
		{req: NewPingRequest().Context(ctx), expected: ctx},
		{req: NewPrepareRequest(validExpr).Context(ctx), expected: ctx},
		{req: NewUnprepareRequest(validStmt).Context(ctx), expected: ctx},
		{req: NewExecutePreparedRequest(validStmt).Context(ctx), expected: ctx},
		{req: NewBeginRequest().Context(ctx), expected: ctx},
		{req: NewCommitRequest().Context(ctx), expected: ctx},
		{req: NewRollbackRequest().Context(ctx), expected: ctx},
		{req: NewIdRequest(validProtocolInfo).Context(ctx), expected: ctx},
		{req: NewBroadcastRequest(validKey).Context(ctx), expected: ctx},
	}

	for _, test := range tests {
		if ctx := test.req.Ctx(); ctx != test.expected {
			t.Errorf("An invalid ctx %t, expected %t", ctx, test.expected)
		}
	}
}

func TestPingRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplPingBody(refEnc)
	if err != nil {
		t.Errorf("An unexpected RefImplPingBody() error: %q", err.Error())
		return
	}

	req := NewPingRequest()
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestSelectRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplSelectBody(refEnc, validSpace, defaultIndex, 0, 0xFFFFFFFF,
		IterAll, []interface{}{}, nil, false)
	if err != nil {
		t.Errorf("An unexpected RefImplSelectBody() error %q", err.Error())
		return
	}

	req := NewSelectRequest(validSpace)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestSelectRequestDefaultIteratorEqIfKey(t *testing.T) {
	var refBuf bytes.Buffer
	key := []interface{}{uint(18)}

	refEnc := NewEncoder(&refBuf)
	err := RefImplSelectBody(refEnc, validSpace, defaultIndex, 0, 0xFFFFFFFF,
		IterEq, key, nil, false)
	if err != nil {
		t.Errorf("An unexpected RefImplSelectBody() error %q", err.Error())
		return
	}

	req := NewSelectRequest(validSpace).
		Key(key)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestSelectRequestIteratorNotChangedIfKey(t *testing.T) {
	var refBuf bytes.Buffer
	key := []interface{}{uint(678)}
	const iter = IterGe

	refEnc := NewEncoder(&refBuf)
	err := RefImplSelectBody(refEnc, validSpace, defaultIndex, 0, 0xFFFFFFFF,
		iter, key, nil, false)
	if err != nil {
		t.Errorf("An unexpected RefImplSelectBody() error %q", err.Error())
		return
	}

	req := NewSelectRequest(validSpace).
		Iterator(iter).
		Key(key)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestSelectRequestSetters(t *testing.T) {
	const offset = 4
	const limit = 5
	const iter = IterLt
	key := []interface{}{uint(36)}
	afterBytes := []byte{0x1, 0x2, 0x3}
	afterKey := []interface{}{uint(13)}
	var refBufAfterBytes, refBufAfterKey bytes.Buffer

	refEncAfterBytes := NewEncoder(&refBufAfterBytes)
	err := RefImplSelectBody(refEncAfterBytes, validSpace, validIndex, offset,
		limit, iter, key, afterBytes, true)
	if err != nil {
		t.Errorf("An unexpected RefImplSelectBody() error %s", err)
		return
	}

	refEncAfterKey := NewEncoder(&refBufAfterKey)
	err = RefImplSelectBody(refEncAfterKey, validSpace, validIndex, offset,
		limit, iter, key, afterKey, true)
	if err != nil {
		t.Errorf("An unexpected RefImplSelectBody() error %s", err)
		return
	}

	reqAfterBytes := NewSelectRequest(validSpace).
		Index(validIndex).
		Offset(offset).
		Limit(limit).
		Iterator(iter).
		Key(key).
		After(afterBytes).
		FetchPos(true)
	reqAfterKey := NewSelectRequest(validSpace).
		Index(validIndex).
		Offset(offset).
		Limit(limit).
		Iterator(iter).
		Key(key).
		After(afterKey).
		FetchPos(true)

	assertBodyEqual(t, refBufAfterBytes.Bytes(), reqAfterBytes)
	assertBodyEqual(t, refBufAfterKey.Bytes(), reqAfterKey)
}

func TestInsertRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplInsertBody(refEnc, validSpace, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplInsertBody() error: %q", err.Error())
		return
	}

	req := NewInsertRequest(validSpace)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestInsertRequestSetters(t *testing.T) {
	tuple := []interface{}{uint(24)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplInsertBody(refEnc, validSpace, tuple)
	if err != nil {
		t.Errorf("An unexpected RefImplInsertBody() error: %q", err.Error())
		return
	}

	req := NewInsertRequest(validSpace).
		Tuple(tuple)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestReplaceRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplReplaceBody(refEnc, validSpace, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplReplaceBody() error: %q", err.Error())
		return
	}

	req := NewReplaceRequest(validSpace)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestReplaceRequestSetters(t *testing.T) {
	tuple := []interface{}{uint(99)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplReplaceBody(refEnc, validSpace, tuple)
	if err != nil {
		t.Errorf("An unexpected RefImplReplaceBody() error: %q", err.Error())
		return
	}

	req := NewReplaceRequest(validSpace).
		Tuple(tuple)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestDeleteRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplDeleteBody(refEnc, validSpace, defaultIndex, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplDeleteBody() error: %q", err.Error())
		return
	}

	req := NewDeleteRequest(validSpace)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestDeleteRequestSetters(t *testing.T) {
	key := []interface{}{uint(923)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplDeleteBody(refEnc, validSpace, validIndex, key)
	if err != nil {
		t.Errorf("An unexpected RefImplDeleteBody() error: %q", err.Error())
		return
	}

	req := NewDeleteRequest(validSpace).
		Index(validIndex).
		Key(key)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestUpdateRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplUpdateBody(refEnc, validSpace, defaultIndex, []interface{}{}, []Op{})
	if err != nil {
		t.Errorf("An unexpected RefImplUpdateBody() error: %q", err.Error())
		return
	}

	req := NewUpdateRequest(validSpace)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestUpdateRequestSetters(t *testing.T) {
	key := []interface{}{uint(44)}
	refOps, reqOps := getTestOps()
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplUpdateBody(refEnc, validSpace, validIndex, key, refOps)
	if err != nil {
		t.Errorf("An unexpected RefImplUpdateBody() error: %q", err.Error())
		return
	}

	req := NewUpdateRequest(validSpace).
		Index(validIndex).
		Key(key).
		Operations(reqOps)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestUpsertRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplUpsertBody(refEnc, validSpace, []interface{}{}, []Op{})
	if err != nil {
		t.Errorf("An unexpected RefImplUpsertBody() error: %q", err.Error())
		return
	}

	req := NewUpsertRequest(validSpace)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestUpsertRequestSetters(t *testing.T) {
	tuple := []interface{}{uint(64)}
	refOps, reqOps := getTestOps()
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplUpsertBody(refEnc, validSpace, tuple, refOps)
	if err != nil {
		t.Errorf("An unexpected RefImplUpsertBody() error: %q", err.Error())
		return
	}

	req := NewUpsertRequest(validSpace).
		Tuple(tuple).
		Operations(reqOps)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestCallRequestsDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplCallBody(refEnc, validExpr, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplCallBody() error: %q", err.Error())
		return
	}

	req := NewCallRequest(validExpr)
	req16 := NewCall16Request(validExpr)
	req17 := NewCall17Request(validExpr)
	assertBodyEqual(t, refBuf.Bytes(), req)
	assertBodyEqual(t, refBuf.Bytes(), req16)
	assertBodyEqual(t, refBuf.Bytes(), req17)
}

func TestCallRequestsSetters(t *testing.T) {
	args := []interface{}{uint(34)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplCallBody(refEnc, validExpr, args)
	if err != nil {
		t.Errorf("An unexpected RefImplCallBody() error: %q", err.Error())
		return
	}

	req := NewCall16Request(validExpr).
		Args(args)
	req16 := NewCall16Request(validExpr).
		Args(args)
	req17 := NewCall17Request(validExpr).
		Args(args)
	assertBodyEqual(t, refBuf.Bytes(), req)
	assertBodyEqual(t, refBuf.Bytes(), req16)
	assertBodyEqual(t, refBuf.Bytes(), req17)
}

func TestEvalRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplEvalBody(refEnc, validExpr, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplEvalBody() error: %q", err.Error())
		return
	}

	req := NewEvalRequest(validExpr)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestEvalRequestSetters(t *testing.T) {
	args := []interface{}{uint(34), int(12)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplEvalBody(refEnc, validExpr, args)
	if err != nil {
		t.Errorf("An unexpected RefImplEvalBody() error: %q", err.Error())
		return
	}

	req := NewEvalRequest(validExpr).
		Args(args)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestExecuteRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplExecuteBody(refEnc, validExpr, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplExecuteBody() error: %q", err.Error())
		return
	}

	req := NewExecuteRequest(validExpr)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestExecuteRequestSetters(t *testing.T) {
	args := []interface{}{uint(11)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplExecuteBody(refEnc, validExpr, args)
	if err != nil {
		t.Errorf("An unexpected RefImplExecuteBody() error: %q", err.Error())
		return
	}

	req := NewExecuteRequest(validExpr).
		Args(args)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestPrepareRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplPrepareBody(refEnc, validExpr)
	if err != nil {
		t.Errorf("An unexpected RefImplPrepareBody() error: %q", err.Error())
		return
	}

	req := NewPrepareRequest(validExpr)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestUnprepareRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplUnprepareBody(refEnc, *validStmt)
	if err != nil {
		t.Errorf("An unexpected RefImplUnprepareBody() error: %q", err.Error())
		return
	}

	req := NewUnprepareRequest(validStmt)
	assert.Equal(t, req.Conn(), validStmt.Conn)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestExecutePreparedRequestSetters(t *testing.T) {
	args := []interface{}{uint(11)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplExecutePreparedBody(refEnc, *validStmt, args)
	if err != nil {
		t.Errorf("An unexpected RefImplExecutePreparedBody() error: %q", err.Error())
		return
	}

	req := NewExecutePreparedRequest(validStmt).
		Args(args)
	assert.Equal(t, req.Conn(), validStmt.Conn)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestExecutePreparedRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplExecutePreparedBody(refEnc, *validStmt, []interface{}{})
	if err != nil {
		t.Errorf("An unexpected RefImplExecutePreparedBody() error: %q", err.Error())
		return
	}

	req := NewExecutePreparedRequest(validStmt)
	assert.Equal(t, req.Conn(), validStmt.Conn)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestBeginRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplBeginBody(refEnc, defaultIsolationLevel, defaultTimeout)
	if err != nil {
		t.Errorf("An unexpected RefImplBeginBody() error: %q", err.Error())
		return
	}

	req := NewBeginRequest()
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestBeginRequestSetters(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplBeginBody(refEnc, ReadConfirmedLevel, validTimeout)
	if err != nil {
		t.Errorf("An unexpected RefImplBeginBody() error: %q", err.Error())
		return
	}

	req := NewBeginRequest().TxnIsolation(ReadConfirmedLevel).Timeout(validTimeout)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestCommitRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplCommitBody(refEnc)
	if err != nil {
		t.Errorf("An unexpected RefImplCommitBody() error: %q", err.Error())
		return
	}

	req := NewCommitRequest()
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestRollbackRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	err := RefImplRollbackBody(refEnc)
	if err != nil {
		t.Errorf("An unexpected RefImplRollbackBody() error: %q", err.Error())
		return
	}

	req := NewRollbackRequest()
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestBroadcastRequestDefaultValues(t *testing.T) {
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	expectedArgs := []interface{}{validKey}
	err := RefImplCallBody(refEnc, "box.broadcast", expectedArgs)
	if err != nil {
		t.Errorf("An unexpected RefImplCallBody() error: %q", err.Error())
		return
	}

	req := NewBroadcastRequest(validKey)
	assertBodyEqual(t, refBuf.Bytes(), req)
}

func TestBroadcastRequestSetters(t *testing.T) {
	value := []interface{}{uint(34), int(12)}
	var refBuf bytes.Buffer

	refEnc := NewEncoder(&refBuf)
	expectedArgs := []interface{}{validKey, value}
	err := RefImplCallBody(refEnc, "box.broadcast", expectedArgs)
	if err != nil {
		t.Errorf("An unexpected RefImplCallBody() error: %q", err.Error())
		return
	}

	req := NewBroadcastRequest(validKey).Value(value)
	assertBodyEqual(t, refBuf.Bytes(), req)
}
