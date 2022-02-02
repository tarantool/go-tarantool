package decimal

func EncodeStringToBCD(buf string) ([]byte, error) {
	return encodeStringToBCD(buf)
}

func DecodeStringFromBCD(bcdBuf []byte) (string, error) {
	return decodeStringFromBCD(bcdBuf)
}

func GetNumberLength(buf string) int {
	return getNumberLength(buf)
}

const (
	DecimalPrecision = decimalPrecision
)
