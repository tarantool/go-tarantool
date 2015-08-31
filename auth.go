package tarantool

import (
	"crypto/sha1"
	"encoding/base64"
)

func scramble(encoded_salt, pass string) (scramble []byte, err error) {
	/* ==================================================================
		According to: http://tarantool.org/doc/dev_guide/box-protocol.html

		salt = base64_decode(encoded_salt);
		step_1 = sha1(password);
		step_2 = sha1(step_1);
		step_3 = sha1(salt, step_2);
		scramble = xor(step_1, step_3);
		return scramble;

	===================================================================== */
	scrambleSize := sha1.Size // == 20

    salt, err := base64.StdEncoding.DecodeString(encoded_salt)
	if err != nil {
	    return
	}
	step_1 := sha1.Sum([]byte(pass))
	step_2 := sha1.Sum(step_1[0:])
	hash := sha1.New() // may be create it once per connection ?
	hash.Write(salt[0:scrambleSize])
	hash.Write(step_2[0:])
	step_3 := hash.Sum(nil)
	
	return xor(step_1[0:], step_3[0:], scrambleSize), nil
}

func xor(left, right []byte, size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size ; i++ {
		result[i] = left[i] ^ right[i]
	}
	return result
}