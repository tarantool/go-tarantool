package balancer

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/ice-blockchain/go-tarantool"
)

type clusterMemberType string

const (
	Writable    = clusterMemberType("writable")
	NonWritable = clusterMemberType("non-writable")
)

func CheckIfRequiresWriteForRequest(req tarantool.Request, _default bool) bool {
	switch req.(type) {
	case *tarantool.PrepareRequest:
	case *tarantool.UnprepareRequest:
	case *tarantool.ExecutePreparedRequest:
		return _default // How can we get access to privateField to verify with CheckIfRequiresWrite?
	case *tarantool.IdRequest:
		return _default
	case *tarantool.PingRequest:
		return _default
	case *tarantool.SelectRequest:
		return false
	case *tarantool.InsertRequest:
		return true
	case *tarantool.ReplaceRequest:
		return true
	case *tarantool.DeleteRequest:
		return true
	case *tarantool.UpdateRequest:
		return true
	case *tarantool.UpsertRequest:
		return true
	case *tarantool.CallRequest:
	case *tarantool.EvalRequest:
	case *tarantool.ExecuteRequest:
	case *tarantool.BeginRequest:
		return true
	case *tarantool.CommitRequest:
		return true
	case *tarantool.RollbackRequest:
		return true
	default:
		return _default
	}
	return _default
}

func CheckIfRequiresWrite(expr string, _default bool) (string, bool) {
	trimmedS := strings.ToLower(strings.TrimLeftFunc(expr, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}))
	nonWritableTemplate := fmt.Sprintf("{{%s}}", NonWritable)
	if isNonWritable := strings.HasPrefix(trimmedS, nonWritableTemplate); isNonWritable {
		return strings.Replace(expr, nonWritableTemplate, "", 1), false
	}

	writableTemplate := fmt.Sprintf("{{%s}}", Writable)
	if isWritable := strings.HasPrefix(trimmedS, writableTemplate); isWritable {
		return strings.Replace(expr, writableTemplate, "", 1), true
	}

	if isWritable := strings.HasPrefix(trimmedS, "insert"); isWritable {
		return expr, true
	}

	if isWritable := strings.HasPrefix(trimmedS, "delete"); isWritable {
		return expr, true
	}

	if isWritable := strings.HasPrefix(trimmedS, "update"); isWritable {
		return expr, true
	}

	if isWritable := strings.HasPrefix(trimmedS, "replace"); isWritable {
		return expr, true
	}

	if isNonWritable := strings.HasPrefix(trimmedS, "values"); isNonWritable {
		return expr, false
	}

	if isNonWritable := strings.HasPrefix(trimmedS, "with"); isNonWritable {
		return expr, false
	}

	if isNonWritable := strings.HasPrefix(trimmedS, "select"); isNonWritable {
		return expr, false
	}

	return expr, _default
}
