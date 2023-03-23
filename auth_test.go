package tarantool_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/ice-blockchain/go-tarantool"
)

func TestAuth_String(t *testing.T) {
	unknownId := int(PapSha256Auth) + 1
	tests := []struct {
		auth     Auth
		expected string
	}{
		{AutoAuth, "auto"},
		{ChapSha1Auth, "chap-sha1"},
		{PapSha256Auth, "pap-sha256"},
		{Auth(unknownId), fmt.Sprintf("unknown auth type (code %d)", unknownId)},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.auth.String(), tc.expected)
		})
	}
}
