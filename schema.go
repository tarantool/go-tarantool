package tarantool

import (
	"errors"
	"fmt"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// nolint: varcheck,deadcode
const (
	maxSchemas             = 10000
	spaceSpId              = 280
	vspaceSpId             = 281
	indexSpId              = 288
	vindexSpId             = 289
	vspaceSpTypeFieldNum   = 6
	vspaceSpFormatFieldNum = 7
)

func msgpackIsUint(code byte) bool {
	return code == msgpcode.Uint8 || code == msgpcode.Uint16 ||
		code == msgpcode.Uint32 || code == msgpcode.Uint64 ||
		msgpcode.IsFixedNum(code)
}

func msgpackIsMap(code byte) bool {
	return code == msgpcode.Map16 || code == msgpcode.Map32 || msgpcode.IsFixedMap(code)
}

func msgpackIsArray(code byte) bool {
	return code == msgpcode.Array16 || code == msgpcode.Array32 ||
		msgpcode.IsFixedArray(code)
}

func msgpackIsString(code byte) bool {
	return msgpcode.IsFixedString(code) || code == msgpcode.Str8 ||
		code == msgpcode.Str16 || code == msgpcode.Str32
}

// SchemaResolver is an interface for resolving schema details.
type SchemaResolver interface {
	// ResolveSpace returns resolved space number or an error
	// if it cannot be resolved.
	ResolveSpace(s interface{}) (spaceNo uint32, err error)
	// ResolveIndex returns resolved index number or an error
	// if it cannot be resolved.
	ResolveIndex(i interface{}, spaceNo uint32) (indexNo uint32, err error)
	// NamesUseSupported shows if usage of space and index names, instead of
	// IDs, is supported. It must return true if
	// iproto.IPROTO_FEATURE_SPACE_AND_INDEX_NAMES is supported.
	NamesUseSupported() bool
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
		if msgpackIsString(code) {
			val, err := d.DecodeString()
			if err != nil {
				return err
			}
			space.Temporary = val == "temporary"
		} else if msgpackIsMap(code) {
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

// Field is a schema field.
type Field struct {
	Id         uint32
	Name       string
	Type       string
	IsNullable bool
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
		case "is_nullable":
			if field.IsNullable, err = d.DecodeBool(); err != nil {
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

	if msgpackIsUint(code) {
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

	if msgpackIsUint(code) {
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

// IndexFields is an index field.
type IndexField struct {
	Id   uint32
	Type string
}

func (indexField *IndexField) DecodeMsgpack(d *msgpack.Decoder) error {
	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if msgpackIsMap(code) {
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
	} else if msgpackIsArray(code) {
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
		spaceId := index.SpaceId
		if _, ok := schema.SpacesById[spaceId]; ok {
			schema.SpacesById[spaceId].IndexesById[index.Id] = index
			schema.SpacesById[spaceId].Indexes[index.Name] = index
		} else {
			return errors.New("concurrent schema update")
		}
	}

	spaceAndIndexNamesSupported :=
		isFeatureInSlice(iproto.IPROTO_FEATURE_SPACE_AND_INDEX_NAMES,
			conn.serverProtocolInfo.Features)

	conn.lockShards()
	conn.Schema = schema
	conn.schemaResolver = &loadedSchemaResolver{
		Schema:                      schema,
		SpaceAndIndexNamesSupported: spaceAndIndexNamesSupported,
	}
	conn.unlockShards()

	return nil
}

// resolveSpaceNumber tries to resolve a space number.
// Note: at this point, s can be a number, or an object of Space type.
func resolveSpaceNumber(s interface{}) (uint32, error) {
	var spaceNo uint32

	switch s := s.(type) {
	case uint:
		spaceNo = uint32(s)
	case uint64:
		spaceNo = uint32(s)
	case uint32:
		spaceNo = s
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

	return spaceNo, nil
}

// resolveIndexNumber tries to resolve an index number.
// Note: at this point, i can be a number, or an object of Index type.
func resolveIndexNumber(i interface{}) (uint32, error) {
	var indexNo uint32

	switch i := i.(type) {
	case uint:
		indexNo = uint32(i)
	case uint64:
		indexNo = uint32(i)
	case uint32:
		indexNo = i
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

	return indexNo, nil
}

type loadedSchemaResolver struct {
	Schema *Schema
	// SpaceAndIndexNamesSupported shows if a current Tarantool version supports
	// iproto.IPROTO_FEATURE_SPACE_AND_INDEX_NAMES.
	SpaceAndIndexNamesSupported bool
}

func (r *loadedSchemaResolver) ResolveSpace(s interface{}) (uint32, error) {
	if str, ok := s.(string); ok {
		space, ok := r.Schema.Spaces[str]
		if !ok {
			return 0, fmt.Errorf("there is no space with name %s", s)
		}
		return space.Id, nil
	}
	return resolveSpaceNumber(s)
}

func (r *loadedSchemaResolver) ResolveIndex(i interface{}, spaceNo uint32) (uint32, error) {
	if i == nil {
		return 0, nil
	}
	if str, ok := i.(string); ok {
		space, ok := r.Schema.SpacesById[spaceNo]
		if !ok {
			return 0, fmt.Errorf("there is no space with id %d", spaceNo)
		}
		index, ok := space.Indexes[str]
		if !ok {
			err := fmt.Errorf("space %s has not index with name %s", space.Name, i)
			return 0, err
		}
		return index.Id, nil
	}
	return resolveIndexNumber(i)
}

func (r *loadedSchemaResolver) NamesUseSupported() bool {
	return r.SpaceAndIndexNamesSupported
}

type noSchemaResolver struct {
	// SpaceAndIndexNamesSupported shows if a current Tarantool version supports
	// iproto.IPROTO_FEATURE_SPACE_AND_INDEX_NAMES.
	SpaceAndIndexNamesSupported bool
}

func (*noSchemaResolver) ResolveSpace(s interface{}) (uint32, error) {
	if _, ok := s.(string); ok {
		return 0, fmt.Errorf("unable to use an index name " +
			"because schema is not loaded")
	}
	return resolveSpaceNumber(s)
}

func (*noSchemaResolver) ResolveIndex(i interface{}, spaceNo uint32) (uint32, error) {
	if _, ok := i.(string); ok {
		return 0, fmt.Errorf("unable to use an index name " +
			"because schema is not loaded")
	}
	return resolveIndexNumber(i)
}

func (r *noSchemaResolver) NamesUseSupported() bool {
	return r.SpaceAndIndexNamesSupported
}
