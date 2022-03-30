package tarantool

import (
	"crypto/sha1"
	"encoding/base64"
)

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
