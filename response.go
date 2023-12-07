package tarantool

import (
	"fmt"

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

	// Pos returns a position descriptor of the last selected tuple.
	Pos() []byte
	// MetaData returns meta-data.
	MetaData() []ColumnMetaData
	// SQLInfo returns sql info.
	SQLInfo() SQLInfo
}

// ConnResponse is a Response interface implementation.
// It is used for all request types.
type ConnResponse struct {
	header Header
	// data contains deserialized data for untyped requests.
	data []interface{}
	// pos contains a position descriptor of last selected tuple.
	pos      []byte
	metaData []ColumnMetaData
	sqlInfo  SQLInfo
	buf      smallBuf
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
	buf.UnreadByte()
	return d.DecodeInt()
}

func decodeHeader(d *msgpack.Decoder, buf *smallBuf) (Header, error) {
	var l int
	var err error
	d.Reset(buf)
	if l, err = d.DecodeMapLen(); err != nil {
		return Header{}, err
	}
	decodedHeader := Header{}
	for ; l > 0; l-- {
		var cd int
		if cd, err = smallInt(d, buf); err != nil {
			return Header{}, err
		}
		switch iproto.Key(cd) {
		case iproto.IPROTO_SYNC:
			var rid uint64
			if rid, err = d.DecodeUint64(); err != nil {
				return Header{}, err
			}
			decodedHeader.RequestId = uint32(rid)
		case iproto.IPROTO_REQUEST_TYPE:
			var rcode uint64
			if rcode, err = d.DecodeUint64(); err != nil {
				return Header{}, err
			}
			decodedHeader.Code = uint32(rcode)
		default:
			if err = d.Skip(); err != nil {
				return Header{}, err
			}
		}
	}
	return decodedHeader, nil
}

func (resp *ConnResponse) Decode() ([]interface{}, error) {
	var err error
	if resp.buf.Len() > 2 {
		var decodedError string

		offset := resp.buf.Offset()
		defer resp.buf.Seek(offset)

		var l, larr int
		var stmtID, bindCount uint64
		var serverProtocolInfo ProtocolInfo
		var feature iproto.Feature
		var errorExtendedInfo *BoxError = nil

		d := msgpack.NewDecoder(&resp.buf)
		d.SetMapDecoder(func(dec *msgpack.Decoder) (interface{}, error) {
			return dec.DecodeUntypedMap()
		})

		if l, err = d.DecodeMapLen(); err != nil {
			return nil, err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				return nil, err
			}
			switch iproto.Key(cd) {
			case iproto.IPROTO_DATA:
				var res interface{}
				var ok bool
				if res, err = d.DecodeInterface(); err != nil {
					return nil, err
				}
				if resp.data, ok = res.([]interface{}); !ok {
					return nil, fmt.Errorf("result is not array: %v", res)
				}
			case iproto.IPROTO_ERROR:
				if errorExtendedInfo, err = decodeBoxError(d); err != nil {
					return nil, err
				}
			case iproto.IPROTO_ERROR_24:
				if decodedError, err = d.DecodeString(); err != nil {
					return nil, err
				}
			case iproto.IPROTO_SQL_INFO:
				if err = d.Decode(&resp.sqlInfo); err != nil {
					return nil, err
				}
			case iproto.IPROTO_METADATA:
				if err = d.Decode(&resp.metaData); err != nil {
					return nil, err
				}
			case iproto.IPROTO_STMT_ID:
				if stmtID, err = d.DecodeUint64(); err != nil {
					return nil, err
				}
			case iproto.IPROTO_BIND_COUNT:
				if bindCount, err = d.DecodeUint64(); err != nil {
					return nil, err
				}
			case iproto.IPROTO_VERSION:
				if err = d.Decode(&serverProtocolInfo.Version); err != nil {
					return nil, err
				}
			case iproto.IPROTO_FEATURES:
				if larr, err = d.DecodeArrayLen(); err != nil {
					return nil, err
				}

				serverProtocolInfo.Features = make([]iproto.Feature, larr)
				for i := 0; i < larr; i++ {
					if err = d.Decode(&feature); err != nil {
						return nil, err
					}
					serverProtocolInfo.Features[i] = feature
				}
			case iproto.IPROTO_AUTH_TYPE:
				var auth string
				if auth, err = d.DecodeString(); err != nil {
					return nil, err
				}
				found := false
				for _, a := range [...]Auth{ChapSha1Auth, PapSha256Auth} {
					if auth == a.String() {
						serverProtocolInfo.Auth = a
						found = true
					}
				}
				if !found {
					return nil, fmt.Errorf("unknown auth type %s", auth)
				}
			case iproto.IPROTO_POSITION:
				if resp.pos, err = d.DecodeBytes(); err != nil {
					return nil, fmt.Errorf("unable to decode a position: %w", err)
				}
			default:
				if err = d.Skip(); err != nil {
					return nil, err
				}
			}
		}
		if stmtID != 0 {
			stmt := &Prepared{
				StatementID: PreparedID(stmtID),
				ParamCount:  bindCount,
				MetaData:    resp.metaData,
			}
			resp.data = []interface{}{stmt}
		}

		// Tarantool may send only version >= 1
		if serverProtocolInfo.Version != ProtocolVersion(0) || serverProtocolInfo.Features != nil {
			if serverProtocolInfo.Version == ProtocolVersion(0) {
				return nil, fmt.Errorf("no protocol version provided in Id response")
			}
			if serverProtocolInfo.Features == nil {
				return nil, fmt.Errorf("no features provided in Id response")
			}
			resp.data = []interface{}{serverProtocolInfo}
		}

		if decodedError != "" {
			resp.header.Code &^= uint32(iproto.IPROTO_TYPE_ERROR)
			err = Error{iproto.Error(resp.header.Code), decodedError, errorExtendedInfo}
		}
	}
	return resp.data, err
}

func (resp *ConnResponse) DecodeTyped(res interface{}) error {
	var err error
	if resp.buf.Len() > 0 {
		offset := resp.buf.Offset()
		defer resp.buf.Seek(offset)

		var errorExtendedInfo *BoxError = nil

		var l int

		d := msgpack.NewDecoder(&resp.buf)
		d.SetMapDecoder(func(dec *msgpack.Decoder) (interface{}, error) {
			return dec.DecodeUntypedMap()
		})

		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		var decodedError string
		for ; l > 0; l-- {
			var cd int
			if cd, err = smallInt(d, &resp.buf); err != nil {
				return err
			}
			switch iproto.Key(cd) {
			case iproto.IPROTO_DATA:
				if err = d.Decode(res); err != nil {
					return err
				}
			case iproto.IPROTO_ERROR:
				if errorExtendedInfo, err = decodeBoxError(d); err != nil {
					return err
				}
			case iproto.IPROTO_ERROR_24:
				if decodedError, err = d.DecodeString(); err != nil {
					return err
				}
			case iproto.IPROTO_SQL_INFO:
				if err = d.Decode(&resp.sqlInfo); err != nil {
					return err
				}
			case iproto.IPROTO_METADATA:
				if err = d.Decode(&resp.metaData); err != nil {
					return err
				}
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
		if decodedError != "" {
			resp.header.Code &^= uint32(iproto.IPROTO_TYPE_ERROR)
			err = Error{iproto.Error(resp.header.Code), decodedError, errorExtendedInfo}
		}
	}
	return err
}

func (resp *ConnResponse) Header() Header {
	return resp.header
}

func (resp *ConnResponse) Pos() []byte {
	return resp.pos
}

func (resp *ConnResponse) MetaData() []ColumnMetaData {
	return resp.metaData
}

func (resp *ConnResponse) SQLInfo() SQLInfo {
	return resp.sqlInfo
}

// String implements Stringer interface.
func (resp *ConnResponse) String() (str string) {
	if resp.header.Code == OkCode {
		return fmt.Sprintf("<%d OK %v>", resp.header.RequestId, resp.data)
	}
	return fmt.Sprintf("<%d ERR 0x%x>", resp.header.RequestId, resp.header.Code)
}
