package tarantool

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Response struct {
	RequestId uint32
	Code      uint32
	Error     string
	Data      []interface{}
	buf       smallBuf
}

func (resp *Response) fill(b []byte) {
	resp.buf.b = b
}

func newResponse(b []byte) (resp *Response, err error) {
	resp = &Response{buf: smallBuf{b: b}}
	err = resp.decodeHeader()
	return
}

func (resp *Response) decodeHeader() (err error) {
	var l int
	d := msgpack.NewDecoder(&resp.buf)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = d.DecodeInt(); err != nil {
			return
		}
		switch cd {
		case KeySync:
			if resp.RequestId, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyCode:
			if resp.Code, err = d.DecodeUint32(); err != nil {
				return
			}
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
		var body map[int]interface{}
		d := msgpack.NewDecoder(&resp.buf)
		if err = d.Decode(&body); err != nil {
			return err
		}
		if body[KeyData] != nil {
			resp.Data = body[KeyData].([]interface{})
		}
		if body[KeyError] != nil {
			resp.Error = body[KeyError].(string)
		}
		if resp.Code != OkCode {
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
			if cd, err = d.DecodeInt(); err != nil {
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
				if _, err = d.DecodeInterface(); err != nil {
					return err
				}
			}
		}
		if resp.Code != OkCode {
			err = Error{resp.Code, resp.Error}
		}
	}
	return
}

func (resp *Response) String() (str string) {
	if resp.Code == OkCode {
		return fmt.Sprintf("<%d OK %v>", resp.RequestId, resp.Data)
	} else {
		return fmt.Sprintf("<%d ERR 0x%x %s>", resp.RequestId, resp.Code, resp.Error)
	}
}

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
