package tarantool

import (
	"fmt"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

// Response is an interface with operations for the server responses.
type Response interface {
	// Header returns a response header.
	Header() Header
	// Decode decodes a response.
	Decode() ([]interface{}, error)
	// DecodeTyped decodes a response into a given container res.
	DecodeTyped(res interface{}) error
}

type baseResponse struct {
	// header is a response header.
	header Header
	// data contains deserialized data for untyped requests.
	data []interface{}
	buf  smallBuf
	// Was the Decode() func called for this response.
	decoded bool
	// Was the DecodeTyped() func called for this response.
	decodedTyped bool
	err          error
}

func createBaseResponse(header Header, body io.Reader) (baseResponse, error) {
	if body == nil {
		return baseResponse{header: header}, nil
	}
	if buf, ok := body.(*smallBuf); ok {
		return baseResponse{header: header, buf: *buf}, nil
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return baseResponse{}, err
	}
	return baseResponse{header: header, buf: smallBuf{b: data}}, nil
}

// DecodeBaseResponse parse response header and body.
func DecodeBaseResponse(header Header, body io.Reader) (Response, error) {
	resp, err := createBaseResponse(header, body)
	return &resp, err
}

// SelectResponse is used for the select requests.
// It might contain a position descriptor of the last selected tuple.
//
// You need to cast to SelectResponse a response from SelectRequest.
type SelectResponse struct {
	baseResponse
	// pos contains a position descriptor of last selected tuple.
	pos []byte
}

// PrepareResponse is used for the prepare requests.
// It might contain meta-data and sql info.
//
// Be careful: now this is an alias for `ExecuteResponse`,
// but it could be changed in the future.
// You need to cast to PrepareResponse a response from PrepareRequest.
type PrepareResponse ExecuteResponse

// ExecuteResponse is used for the execute requests.
// It might contain meta-data and sql info.
//
// You need to cast to ExecuteResponse a response from ExecuteRequest.
type ExecuteResponse struct {
	baseResponse
	metaData []ColumnMetaData
	sqlInfo  SQLInfo
}

type ColumnMetaData struct {
	FieldName            string
	FieldType            string
	FieldCollation       string
	FieldIsNullable      bool
	FieldIsAutoincrement bool
	FieldSpan            string
}

type SQLInfo struct {
	AffectedCount        uint64
	InfoAutoincrementIds []uint64
}

func (meta *ColumnMetaData) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeMapLen(); err != nil {
		return err
	}
	if l == 0 {
		return fmt.Errorf("map len doesn't match: %d", l)
	}
	for i := 0; i < l; i++ {
		var mk uint64
		var mv interface{}
		if mk, err = d.DecodeUint64(); err != nil {
			return fmt.Errorf("failed to decode meta data")
		}
		if mv, err = d.DecodeInterface(); err != nil {
			return fmt.Errorf("failed to decode meta data")
		}
		switch iproto.MetadataKey(mk) {
		case iproto.IPROTO_FIELD_NAME:
			meta.FieldName = mv.(string)
		case iproto.IPROTO_FIELD_TYPE:
			meta.FieldType = mv.(string)
		case iproto.IPROTO_FIELD_COLL:
			meta.FieldCollation = mv.(string)
		case iproto.IPROTO_FIELD_IS_NULLABLE:
			meta.FieldIsNullable = mv.(bool)
		case iproto.IPROTO_FIELD_IS_AUTOINCREMENT:
			meta.FieldIsAutoincrement = mv.(bool)
		case iproto.IPROTO_FIELD_SPAN:
			meta.FieldSpan = mv.(string)
		default:
			return fmt.Errorf("failed to decode meta data")
		}
	}
	return nil
}

func (info *SQLInfo) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeMapLen(); err != nil {
		return err
	}
	if l == 0 {
		return fmt.Errorf("map len doesn't match")
	}
	for i := 0; i < l; i++ {
		var mk uint64
		if mk, err = d.DecodeUint64(); err != nil {
			return fmt.Errorf("failed to decode meta data")
		}
		switch iproto.SqlInfoKey(mk) {
		case iproto.SQL_INFO_ROW_COUNT:
			if info.AffectedCount, err = d.DecodeUint64(); err != nil {
				return fmt.Errorf("failed to decode meta data")
			}
		case iproto.SQL_INFO_AUTOINCREMENT_IDS:
			if err = d.Decode(&info.InfoAutoincrementIds); err != nil {
				return fmt.Errorf("failed to decode meta data")
			}
		default:
			return fmt.Errorf("failed to decode meta data")
		}
	}
	return nil
}

func smallInt(d *msgpack.Decoder, buf *smallBuf) (i int, err error) {
	b, err := buf.ReadByte()
	if err != nil {
		return
	}
	if b <= 127 {
		return int(b), nil
	}
	if err = buf.UnreadByte(); err != nil {
		return -1, err
	}
	return d.DecodeInt()
}

func decodeHeader(d *msgpack.Decoder, buf *smallBuf) (Header, iproto.Type, error) {
	var l int
	var code int
	var err error
	d.Reset(buf)
	if l, err = d.DecodeMapLen(); err != nil {
		return Header{}, 0, err
	}
	decodedHeader := Header{Error: ErrorNo}
	for ; l > 0; l-- {
		var cd int
		if cd, err = smallInt(d, buf); err != nil {
			return Header{}, 0, err
		}
		switch iproto.Key(cd) {
		case iproto.IPROTO_SYNC:
			var rid uint64
			if rid, err = d.DecodeUint64(); err != nil {
				return Header{}, 0, err
			}
			decodedHeader.RequestId = uint32(rid)
		case iproto.IPROTO_REQUEST_TYPE:
			if code, err = d.DecodeInt(); err != nil {
				return Header{}, 0, err
			}
			if code&int(iproto.IPROTO_TYPE_ERROR) != 0 {
				decodedHeader.Error = iproto.Error(code &^ int(iproto.IPROTO_TYPE_ERROR))
			} else {
				decodedHeader.Error = ErrorNo
			}
		default:
			if err = d.Skip(); err != nil {
				return Header{}, 0, err
			}
		}
	}
	return decodedHeader, iproto.Type(code), nil
}

type decodeInfo struct {
	stmtID             uint64
	bindCount          uint64
	serverProtocolInfo ProtocolInfo
	errorExtendedInfo  *BoxError

	decodedError string
}

func (info *decodeInfo) parseData(resp *baseResponse) error {
	if info.stmtID != 0 {
		stmt := &Prepared{
			StatementID: PreparedID(info.stmtID),
			ParamCount:  info.bindCount,
		}
		resp.data = []interface{}{stmt}
		return nil
	}

	// Tarantool may send only version >= 1.
	if info.serverProtocolInfo.Version != ProtocolVersion(0) ||
		info.serverProtocolInfo.Features != nil {
		if info.serverProtocolInfo.Version == ProtocolVersion(0) {
			return fmt.Errorf("no protocol version provided in Id response")
		}
		if info.serverProtocolInfo.Features == nil {
			return fmt.Errorf("no features provided in Id response")
		}
		resp.data = []interface{}{info.serverProtocolInfo}
		return nil
	}
	return nil
}

func decodeCommonField(d *msgpack.Decoder, cd int, data *[]interface{},
	info *decodeInfo) (bool, error) {
	var feature iproto.Feature
	var err error

	switch iproto.Key(cd) {
	case iproto.IPROTO_DATA:
		var res interface{}
		var ok bool
		if res, err = d.DecodeInterface(); err != nil {
			return false, err
		}
		if *data, ok = res.([]interface{}); !ok {
			return false, fmt.Errorf("result is not array: %v", res)
		}
	case iproto.IPROTO_ERROR:
		if info.errorExtendedInfo, err = decodeBoxError(d); err != nil {
			return false, err
		}
	case iproto.IPROTO_ERROR_24:
		if info.decodedError, err = d.DecodeString(); err != nil {
			return false, err
		}
	case iproto.IPROTO_STMT_ID:
		if info.stmtID, err = d.DecodeUint64(); err != nil {
			return false, err
		}
	case iproto.IPROTO_BIND_COUNT:
		if info.bindCount, err = d.DecodeUint64(); err != nil {
			return false, err
		}
	case iproto.IPROTO_VERSION:
		if err = d.Decode(&info.serverProtocolInfo.Version); err != nil {
			return false, err
		}
	case iproto.IPROTO_FEATURES:
		var larr int
		if larr, err = d.DecodeArrayLen(); err != nil {
			return false, err
		}

		info.serverProtocolInfo.Features = make([]iproto.Feature, larr)
		for i := 0; i < larr; i++ {
			if err = d.Decode(&feature); err != nil {
				return false, err
			}
			info.serverProtocolInfo.Features[i] = feature
		}
	case iproto.IPROTO_AUTH_TYPE:
		var auth string
		if auth, err = d.DecodeString(); err != nil {
			return false, err
		}
		found := false
		for _, a := range [...]Auth{ChapSha1Auth, PapSha256Auth} {
			if auth == a.String() {
				info.serverProtocolInfo.Auth = a
				found = true
			}
		}
		if !found {
			return false, fmt.Errorf("unknown auth type %s", auth)
		}
	default:
		return false, nil
	}
	return true, nil
}

func (resp *baseResponse) Decode() ([]interface{}, error) {
	if resp.decoded {
		return resp.data, resp.err
	}

	resp.decoded = true
	var err error
	if resp.buf.Len() > 2 {
		offset := resp.buf.Offset()
		defer func() {
			_ = resp.buf.Seek(offset)
		}()

		var l int
		info := &decodeInfo{}

		d := getDecoder(&resp.buf)
		defer putDecoder(d)

		if l, err = d.DecodeMapLen(); err != nil {
			resp.err = err
			return nil, resp.err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				resp.err = err
				return nil, resp.err
			}
			decoded, err := decodeCommonField(d, cd, &resp.data, info)
			if err != nil {
				resp.err = err
				return nil, resp.err
			}
			if !decoded {
				if err = d.Skip(); err != nil {
					resp.err = err
					return nil, resp.err
				}
			}
		}
		err = info.parseData(resp)
		if err != nil {
			resp.err = err
			return nil, resp.err
		}

		if info.decodedError != "" {
			resp.err = Error{resp.header.Error, info.decodedError,
				info.errorExtendedInfo}
		}
	}
	return resp.data, resp.err
}

func (resp *SelectResponse) Decode() ([]interface{}, error) {
	if resp.decoded {
		return resp.data, resp.err
	}

	resp.decoded = true
	var err error
	if resp.buf.Len() > 2 {
		offset := resp.buf.Offset()
		defer func() {
			_ = resp.buf.Seek(offset)
		}()

		var l int
		info := &decodeInfo{}

		d := getDecoder(&resp.buf)
		defer putDecoder(d)

		if l, err = d.DecodeMapLen(); err != nil {
			resp.err = err
			return nil, resp.err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				resp.err = err
				return nil, resp.err
			}
			decoded, err := decodeCommonField(d, cd, &resp.data, info)
			if err != nil {
				resp.err = err
				return nil, err
			}
			if !decoded {
				switch iproto.Key(cd) {
				case iproto.IPROTO_POSITION:
					if resp.pos, err = d.DecodeBytes(); err != nil {
						resp.err = err
						return nil, fmt.Errorf("unable to decode a position: %w", resp.err)
					}
				default:
					if err = d.Skip(); err != nil {
						resp.err = err
						return nil, resp.err
					}
				}
			}
		}
		err = info.parseData(&resp.baseResponse)
		if err != nil {
			resp.err = err
			return nil, resp.err
		}

		if info.decodedError != "" {
			resp.err = Error{resp.header.Error, info.decodedError,
				info.errorExtendedInfo}
		}
	}
	return resp.data, resp.err
}

func (resp *ExecuteResponse) Decode() ([]interface{}, error) {
	if resp.decoded {
		return resp.data, resp.err
	}

	resp.decoded = true
	var err error
	if resp.buf.Len() > 2 {
		offset := resp.buf.Offset()
		defer func() {
			_ = resp.buf.Seek(offset)
		}()

		var l int
		info := &decodeInfo{}

		d := getDecoder(&resp.buf)
		defer putDecoder(d)

		if l, err = d.DecodeMapLen(); err != nil {
			resp.err = err
			return nil, resp.err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				resp.err = err
				return nil, resp.err
			}
			decoded, err := decodeCommonField(d, cd, &resp.data, info)
			if err != nil {
				resp.err = err
				return nil, resp.err
			}
			if !decoded {
				switch iproto.Key(cd) {
				case iproto.IPROTO_SQL_INFO:
					if err = d.Decode(&resp.sqlInfo); err != nil {
						resp.err = err
						return nil, resp.err
					}
				case iproto.IPROTO_METADATA:
					if err = d.Decode(&resp.metaData); err != nil {
						resp.err = err
						return nil, resp.err
					}
				default:
					if err = d.Skip(); err != nil {
						resp.err = err
						return nil, resp.err
					}
				}
			}
		}
		err = info.parseData(&resp.baseResponse)
		if err != nil {
			resp.err = err
			return nil, resp.err
		}

		if info.decodedError != "" {
			resp.err = Error{resp.header.Error, info.decodedError,
				info.errorExtendedInfo}
		}
	}
	return resp.data, resp.err
}

func decodeTypedCommonField(d *msgpack.Decoder, res interface{}, cd int,
	info *decodeInfo) (bool, error) {
	var err error

	switch iproto.Key(cd) {
	case iproto.IPROTO_DATA:
		if err = d.Decode(res); err != nil {
			return false, err
		}
	case iproto.IPROTO_ERROR:
		if info.errorExtendedInfo, err = decodeBoxError(d); err != nil {
			return false, err
		}
	case iproto.IPROTO_ERROR_24:
		if info.decodedError, err = d.DecodeString(); err != nil {
			return false, err
		}
	default:
		return false, nil
	}
	return true, nil
}

func (resp *baseResponse) DecodeTyped(res interface{}) error {
	resp.decodedTyped = true

	var err error
	if resp.buf.Len() > 0 {
		offset := resp.buf.Offset()
		defer func() {
			_ = resp.buf.Seek(offset)
		}()

		info := &decodeInfo{}
		var l int

		d := getDecoder(&resp.buf)
		defer putDecoder(d)

		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				return err
			}
			decoded, err := decodeTypedCommonField(d, res, cd, info)
			if err != nil {
				return err
			}
			if !decoded {
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if info.decodedError != "" {
			err = Error{resp.header.Error, info.decodedError, info.errorExtendedInfo}
		}
	}
	return err
}

func (resp *SelectResponse) DecodeTyped(res interface{}) error {
	resp.decodedTyped = true

	var err error
	if resp.buf.Len() > 0 {
		offset := resp.buf.Offset()
		defer func() {
			_ = resp.buf.Seek(offset)
		}()

		info := &decodeInfo{}
		var l int

		d := getDecoder(&resp.buf)
		defer putDecoder(d)

		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				return err
			}
			decoded, err := decodeTypedCommonField(d, res, cd, info)
			if err != nil {
				return err
			}
			if !decoded {
				switch iproto.Key(cd) {
				case iproto.IPROTO_POSITION:
					if resp.pos, err = d.DecodeBytes(); err != nil {
						return fmt.Errorf("unable to decode a position: %w", err)
					}
				default:
					if err = d.Skip(); err != nil {
						return err
					}
				}
			}
		}
		if info.decodedError != "" {
			err = Error{resp.header.Error, info.decodedError, info.errorExtendedInfo}
		}
	}
	return err
}

func (resp *ExecuteResponse) DecodeTyped(res interface{}) error {
	resp.decodedTyped = true

	var err error
	if resp.buf.Len() > 0 {
		offset := resp.buf.Offset()
		defer func() {
			_ = resp.buf.Seek(offset)
		}()
		info := &decodeInfo{}
		var l int

		d := getDecoder(&resp.buf)
		defer putDecoder(d)

		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				return err
			}
			decoded, err := decodeTypedCommonField(d, res, cd, info)
			if err != nil {
				return err
			}
			if !decoded {
				switch iproto.Key(cd) {
				case iproto.IPROTO_SQL_INFO:
					if err = d.Decode(&resp.sqlInfo); err != nil {
						return err
					}
				case iproto.IPROTO_METADATA:
					if err = d.Decode(&resp.metaData); err != nil {
						return err
					}
				default:
					if err = d.Skip(); err != nil {
						return err
					}
				}
			}
		}
		if info.decodedError != "" {
			err = Error{resp.header.Error, info.decodedError, info.errorExtendedInfo}
		}
	}
	return err
}

func (resp *baseResponse) Header() Header {
	return resp.header
}

// Pos returns a position descriptor of the last selected tuple for the SelectResponse.
// If the response was not decoded, this method will call Decode().
func (resp *SelectResponse) Pos() ([]byte, error) {
	if !resp.decoded && !resp.decodedTyped {
		if _, err := resp.Decode(); err != nil {
			return nil, err
		}
	}
	return resp.pos, resp.err
}

// MetaData returns ExecuteResponse meta-data.
// If the response was not decoded, this method will call Decode().
func (resp *ExecuteResponse) MetaData() ([]ColumnMetaData, error) {
	if !resp.decoded && !resp.decodedTyped {
		if _, err := resp.Decode(); err != nil {
			return nil, err
		}
	}
	return resp.metaData, resp.err
}

// SQLInfo returns ExecuteResponse sql info.
// If the response was not decoded, this method will call Decode().
func (resp *ExecuteResponse) SQLInfo() (SQLInfo, error) {
	if !resp.decoded && !resp.decodedTyped {
		if _, err := resp.Decode(); err != nil {
			return resp.sqlInfo, err
		}
	}
	return resp.sqlInfo, resp.err
}

// String implements Stringer interface.
func (resp *baseResponse) String() (str string) {
	if resp.header.Error == ErrorNo {
		return fmt.Sprintf("<%d OK %v>", resp.header.RequestId, resp.data)
	}
	return fmt.Sprintf("<%d ERR %s %v>", resp.header.RequestId, resp.header.Error, resp.err)
}
