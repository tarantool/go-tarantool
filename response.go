package tarantool

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type Response struct {
	RequestId uint32
	Code      uint32
	Error     string // error message
	// Data contains deserialized data for untyped requests
	Data []interface{}
	buf  smallBuf
}

func (resp *Response) fill(b []byte) {
	resp.buf.b = b
}

func (resp *Response) smallInt(d *msgpack.Decoder) (i int, err error) {
	b, err := resp.buf.ReadByte()
	if err != nil {
		return
	}
	if b <= 127 {
		return int(b), nil
	}
	resp.buf.UnreadByte()
	return d.DecodeInt()
}

func (resp *Response) decodeHeader(d *msgpack.Decoder) (err error) {
	var l int
	d.Reset(&resp.buf)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = resp.smallInt(d); err != nil {
			return
		}
		switch cd {
		case KeySync:
			var rid uint64
			if rid, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.RequestId = uint32(rid)
		case KeyCode:
			var rcode uint64
			if rcode, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.Code = uint32(rcode)
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return nil
}

func (resp *Response) decodeBody() (err error) {
	if resp.buf.Len() > 2 {
		var l int
		d := msgpack.NewDecoder(&resp.buf)
		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = resp.smallInt(d); err != nil {
				return err
			}
			switch cd {
			case KeyData:
				var res interface{}
				var ok bool
				if res, err = d.DecodeInterface(); err != nil {
					return err
				}
				if resp.Data, ok = res.([]interface{}); !ok {
					return fmt.Errorf("result is not array: %v", res)
				}
			case KeyError:
				if resp.Error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.Code != OkCode {
			resp.Code &^= ErrorCodeBit
			err = Error{resp.Code, resp.Error}
		}
	}
	return
}

func (resp *Response) decodeBodyTyped(res interface{}) (err error) {
	if resp.buf.Len() > 0 {
		var l int
		d := msgpack.NewDecoder(&resp.buf)
		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = resp.smallInt(d); err != nil {
				return err
			}
			switch cd {
			case KeyData:
				if err = d.Decode(res); err != nil {
					return err
				}
			case KeyError:
				if resp.Error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.Code != OkCode {
			resp.Code &^= ErrorCodeBit
			err = Error{resp.Code, resp.Error}
		}
	}
	return
}

// String implements Stringer interface
func (resp *Response) String() (str string) {
	if resp.Code == OkCode {
		return fmt.Sprintf("<%d OK %v>", resp.RequestId, resp.Data)
	}
	return fmt.Sprintf("<%d ERR 0x%x %s>", resp.RequestId, resp.Code, resp.Error)
}

// Tuples converts result of Eval and Call17 to same format
// as other actions returns (ie array of arrays).
func (resp *Response) Tuples() (res [][]interface{}) {
	res = make([][]interface{}, len(resp.Data))
	for i, t := range resp.Data {
		switch t := t.(type) {
		case []interface{}:
			res[i] = t
		default:
			res[i] = []interface{}{t}
		}
	}
	return res
}
