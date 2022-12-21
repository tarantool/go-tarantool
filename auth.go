package tarantool

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
)

const (
	chapSha1  = "chap-sha1"
	papSha256 = "pap-sha256"
)

// Auth is used as a parameter to set up an authentication method.
type Auth int

const (
	// AutoAuth does not force any authentication method. A method will be
	// selected automatically (a value from IPROTO_ID response or
	// ChapSha1Auth).
	AutoAuth Auth = iota
	// ChapSha1Auth forces chap-sha1 authentication method. The method is
	// available both in the Tarantool Community Edition (CE) and the
	// Tarantool Enterprise Edition (EE)
	ChapSha1Auth
	// PapSha256Auth forces pap-sha256 authentication method. The method is
	// available only for the Tarantool Enterprise Edition (EE) with
	// SSL transport.
	PapSha256Auth
)

// String returns a string representation of an authentication method.
func (a Auth) String() string {
	switch a {
	case AutoAuth:
		return "auto"
	case ChapSha1Auth:
		return chapSha1
	case PapSha256Auth:
		return papSha256
	default:
		return fmt.Sprintf("unknown auth type (code %d)", a)
	}
}

func scramble(encodedSalt, pass string) (scramble []byte, err error) {
	/* ==================================================================
		According to: http://tarantool.org/doc/dev_guide/box-protocol.html

		salt = base64_decode(encodedSalt);
		step1 = sha1(password);
		step2 = sha1(step1);
		step3 = sha1(salt, step2);
		scramble = xor(step1, step3);
		return scramble;

	===================================================================== */
	scrambleSize := sha1.Size // == 20

	salt, err := base64.StdEncoding.DecodeString(encodedSalt)
	if err != nil {
		return
	}
	step1 := sha1.Sum([]byte(pass))
	step2 := sha1.Sum(step1[0:])
	hash := sha1.New() // May be create it once per connection?
	hash.Write(salt[0:scrambleSize])
	hash.Write(step2[0:])
	step3 := hash.Sum(nil)

	return xor(step1[0:], step3[0:], scrambleSize), nil
}

func xor(left, right []byte, size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = left[i] ^ right[i]
	}
	return result
}
