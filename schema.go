package tarantool

import (
	"errors"
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	msgpcode "gopkg.in/vmihailenco/msgpack.v2/codes"
)

//nolint: varcheck,deadcode
const (
	maxSchemas             = 10000
	spaceSpId              = 280
	vspaceSpId             = 281
	indexSpId              = 288
	vindexSpId             = 289
	vspaceSpTypeFieldNum   = 6
	vspaceSpFormatFieldNum = 7
)

// SchemaResolver is an interface for resolving schema details.
type SchemaResolver interface {
	// ResolveSpaceIndex returns resolved space and index numbers or an
	// error if it cannot be resolved.
	ResolveSpaceIndex(s interface{}, i interface{}) (spaceNo, indexNo uint32, err error)
}

// Schema contains information about spaces and indexes.
type Schema struct {
	Version uint
	// Spaces is map from space names to spaces.
	Spaces map[string]*Space
	// SpacesById is map from space numbers to spaces.
	SpacesById map[uint32]*Space
}

// Space contains information about Tarantool's space.
type Space struct {
	Id   uint32
	Name string
	// Could be "memtx" or "vinyl".
	Engine    string
	Temporary bool // Is this space temporary?
	// Field configuration is not mandatory and not checked by Tarantool.
	FieldsCount uint32
	Fields      map[string]*Field
	FieldsById  map[uint32]*Field
	// Indexes is map from index names to indexes.
	Indexes map[string]*Index
	// IndexesById is map from index numbers to indexes.
	IndexesById map[uint32]*Index
}

func isUint(code byte) bool {
	return code == msgpcode.Uint8 || code == msgpcode.Uint16 ||
		code == msgpcode.Uint32 || code == msgpcode.Uint64 ||
		msgpcode.IsFixedNum(code)
}

func isMap(code byte) bool {
	return code == msgpcode.Map16 || code == msgpcode.Map32 || msgpcode.IsFixedMap(code)
}

func isArray(code byte) bool {
	return code == msgpcode.Array16 || code == msgpcode.Array32 ||
		msgpcode.IsFixedArray(code)
}

func isString(code byte) bool {
	return msgpcode.IsFixedString(code) || code == msgpcode.Str8 ||
		code == msgpcode.Str16 || code == msgpcode.Str32
}

func (space *Space) DecodeMsgpack(d *msgpack.Decoder) error {
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}
	if space.Id, err = d.DecodeUint32(); err != nil {
		return err
	}
	if err := d.Skip(); err != nil {
		return err
	}
	if space.Name, err = d.DecodeString(); err != nil {
		return err
	}
	if space.Engine, err = d.DecodeString(); err != nil {
		return err
	}
	if space.FieldsCount, err = d.DecodeUint32(); err != nil {
		return err
	}
	if arrayLen >= vspaceSpTypeFieldNum {
		code, err := d.PeekCode()
		if err != nil {
			return err
		}
		if isString(code) {
			val, err := d.DecodeString()
			if err != nil {
				return err
			}
			space.Temporary = val == "temporary"
		} else if isMap(code) {
			mapLen, err := d.DecodeMapLen()
			if err != nil {
				return err
			}
			for i := 0; i < mapLen; i++ {
				key, err := d.DecodeString()
				if err != nil {
					return err
				}
				if key == "temporary" {
					if space.Temporary, err = d.DecodeBool(); err != nil {
						return err
					}
				} else {
					if err = d.Skip(); err != nil {
						return err
					}
				}
			}
		} else {
			return errors.New("unexpected schema format (space flags)")
		}
	}
	space.FieldsById = make(map[uint32]*Field)
	space.Fields = make(map[string]*Field)
	space.IndexesById = make(map[uint32]*Index)
	space.Indexes = make(map[string]*Index)
	if arrayLen >= vspaceSpFormatFieldNum {
		fieldCount, err := d.DecodeArrayLen()
		if err != nil {
			return err
		}
		for i := 0; i < fieldCount; i++ {
			field := &Field{}
			if err := field.DecodeMsgpack(d); err != nil {
				return err
			}
			field.Id = uint32(i)
			space.FieldsById[field.Id] = field
			if field.Name != "" {
				space.Fields[field.Name] = field
			}
		}
	}
	return nil
}

type Field struct {
	Id   uint32
	Name string
	Type string
}

func (field *Field) DecodeMsgpack(d *msgpack.Decoder) error {
	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < l; i++ {
		key, err := d.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "name":
			if field.Name, err = d.DecodeString(); err != nil {
				return err
			}
		case "type":
			if field.Type, err = d.DecodeString(); err != nil {
				return err
			}
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Index contains information about index.
type Index struct {
	Id      uint32
	SpaceId uint32
	Name    string
	Type    string
	Unique  bool
	Fields  []*IndexField
}

func (index *Index) DecodeMsgpack(d *msgpack.Decoder) error {
	_, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if index.SpaceId, err = d.DecodeUint32(); err != nil {
		return err
	}
	if index.Id, err = d.DecodeUint32(); err != nil {
		return err
	}
	if index.Name, err = d.DecodeString(); err != nil {
		return err
	}
	if index.Type, err = d.DecodeString(); err != nil {
		return err
	}

	var code byte
	if code, err = d.PeekCode(); err != nil {
		return err
	}

	if isUint(code) {
		optsUint64, err := d.DecodeUint64()
		if err != nil {
			return nil
		}
		index.Unique = optsUint64 > 0
	} else {
		var optsMap map[string]interface{}
		if err := d.Decode(&optsMap); err != nil {
			return fmt.Errorf("unexpected schema format (index flags): %w", err)
		}

		var ok bool
		if index.Unique, ok = optsMap["unique"].(bool); !ok {
			/* see bug https://github.com/tarantool/tarantool/issues/2060 */
			index.Unique = true
		}
	}

	if code, err = d.PeekCode(); err != nil {
		return err
	}

	if isUint(code) {
		fieldCount, err := d.DecodeUint64()
		if err != nil {
			return err
		}
		index.Fields = make([]*IndexField, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			index.Fields[i] = new(IndexField)
			if index.Fields[i].Id, err = d.DecodeUint32(); err != nil {
				return err
			}
			if index.Fields[i].Type, err = d.DecodeString(); err != nil {
				return err
			}
		}
	} else {
		if err := d.Decode(&index.Fields); err != nil {
			return fmt.Errorf("unexpected schema format (index flags): %w", err)
		}
	}

	return nil
}

type IndexField struct {
	Id   uint32
	Type string
}

func (indexField *IndexField) DecodeMsgpack(d *msgpack.Decoder) error {
	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if isMap(code) {
		mapLen, err := d.DecodeMapLen()
		if err != nil {
			return err
		}
		for i := 0; i < mapLen; i++ {
			key, err := d.DecodeString()
			if err != nil {
				return err
			}
			switch key {
			case "field":
				if indexField.Id, err = d.DecodeUint32(); err != nil {
					return err
				}
			case "type":
				if indexField.Type, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err := d.Skip(); err != nil {
					return err
				}
			}
		}
		return nil
	} else if isArray(code) {
		arrayLen, err := d.DecodeArrayLen()
		if err != nil {
			return err
		}
		if indexField.Id, err = d.DecodeUint32(); err != nil {
			return err
		}
		if indexField.Type, err = d.DecodeString(); err != nil {
			return err
		}
		for i := 2; i < arrayLen; i++ {
			if err := d.Skip(); err != nil {
				return err
			}
		}
		return nil
	}

	return errors.New("unexpected schema format (index fields)")
}

func (conn *Connection) loadSchema() (err error) {
	schema := new(Schema)
	schema.SpacesById = make(map[uint32]*Space)
	schema.Spaces = make(map[string]*Space)

	// Reload spaces.
	var spaces []*Space
	err = conn.SelectTyped(vspaceSpId, 0, 0, maxSchemas, IterAll, []interface{}{}, &spaces)
	if err != nil {
		return err
	}
	for _, space := range spaces {
		schema.SpacesById[space.Id] = space
		schema.Spaces[space.Name] = space
	}

	// Reload indexes.
	var indexes []*Index
	err = conn.SelectTyped(vindexSpId, 0, 0, maxSchemas, IterAll, []interface{}{}, &indexes)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		schema.SpacesById[index.SpaceId].IndexesById[index.Id] = index
		schema.SpacesById[index.SpaceId].Indexes[index.Name] = index
	}

	conn.Schema = schema
	return nil
}

// ResolveSpaceIndex tries to resolve space and index numbers.
// Note: s can be a number, string, or an object of Space type.
// Note: i can be a number, string, or an object of Index type.
func (schema *Schema) ResolveSpaceIndex(s interface{}, i interface{}) (spaceNo, indexNo uint32, err error) {
	var space *Space
	var index *Index
	var ok bool

	switch s := s.(type) {
	case string:
		if schema == nil {
			err = fmt.Errorf("Schema is not loaded")
			return
		}
		if space, ok = schema.Spaces[s]; !ok {
			err = fmt.Errorf("there is no space with name %s", s)
			return
		}
		spaceNo = space.Id
	case uint:
		spaceNo = uint32(s)
	case uint64:
		spaceNo = uint32(s)
	case uint32:
		spaceNo = uint32(s)
	case uint16:
		spaceNo = uint32(s)
	case uint8:
		spaceNo = uint32(s)
	case int:
		spaceNo = uint32(s)
	case int64:
		spaceNo = uint32(s)
	case int32:
		spaceNo = uint32(s)
	case int16:
		spaceNo = uint32(s)
	case int8:
		spaceNo = uint32(s)
	case Space:
		spaceNo = s.Id
	case *Space:
		spaceNo = s.Id
	default:
		panic("unexpected type of space param")
	}

	if i != nil {
		switch i := i.(type) {
		case string:
			if schema == nil {
				err = fmt.Errorf("Schema is not loaded")
				return
			}
			if space == nil {
				if space, ok = schema.SpacesById[spaceNo]; !ok {
					err = fmt.Errorf("there is no space with id %d", spaceNo)
					return
				}
			}
			if index, ok = space.Indexes[i]; !ok {
				err = fmt.Errorf("space %s has not index with name %s", space.Name, i)
				return
			}
			indexNo = index.Id
		case uint:
			indexNo = uint32(i)
		case uint64:
			indexNo = uint32(i)
		case uint32:
			indexNo = uint32(i)
		case uint16:
			indexNo = uint32(i)
		case uint8:
			indexNo = uint32(i)
		case int:
			indexNo = uint32(i)
		case int64:
			indexNo = uint32(i)
		case int32:
			indexNo = uint32(i)
		case int16:
			indexNo = uint32(i)
		case int8:
			indexNo = uint32(i)
		case Index:
			indexNo = i.Id
		case *Index:
			indexNo = i.Id
		default:
			panic("unexpected type of index param")
		}
	}

	return
}
