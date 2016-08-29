package tarantool

import (
	"fmt"
)

type Schema struct {
	Version    uint
	Spaces     map[string]*Space
	SpacesById map[uint32]*Space
}

type Space struct {
	Id          uint32
	Name        string
	Engine      string
	Temporary   bool
	FieldsCount uint32
	Fields      map[string]*Field
	FieldsById  map[uint32]*Field
	Indexes     map[string]*Index
	IndexesById map[uint32]*Index
}

type Field struct {
	Id   uint32
	Name string
	Type string
}

type Index struct {
	Id     uint32
	Name   string
	Type   string
	Unique bool
	Fields []*IndexField
}

type IndexField struct {
	Id   uint32
	Type string
}

const (
	maxSchemas = 10000
	spaceSpId  = 280
	vspaceSpId = 281
	indexSpId  = 288
	vindexSpId = 289
)

func (conn *Connection) loadSchema() (err error) {
	var req *Request
	var resp *Response

	schema := new(Schema)
	schema.SpacesById = make(map[uint32]*Space)
	schema.Spaces = make(map[string]*Space)

	// reload spaces
	req = conn.NewRequest(SelectRequest)
	req.fillSearch(vspaceSpId, 0, []interface{}{})
	req.fillIterator(0, maxSchemas, IterAll)
	resp, err = req.perform()
	if err != nil {
		return err
	}
	for _, row := range resp.Data {
		row := row.([]interface{})
		space := new(Space)
		space.Id = uint32(row[0].(uint64))
		space.Name = row[2].(string)
		space.Engine = row[3].(string)
		space.FieldsCount = uint32(row[4].(uint64))
		if len(row) >= 6 {
			switch row5 := row[5].(type) {
			case string:
				space.Temporary = row5 == "temporary"
			case map[interface{}]interface{}:
				if temp, ok := row5["temporary"]; ok {
					space.Temporary = temp.(bool)
				}
			default:
				panic("unexpected schema format (space flags)")
			}
		}
		space.FieldsById = make(map[uint32]*Field)
		space.Fields = make(map[string]*Field)
		space.IndexesById = make(map[uint32]*Index)
		space.Indexes = make(map[string]*Index)
		if len(row) >= 7 {
			for i, f := range row[6].([]interface{}) {
				if f == nil {
					continue
				}
				f := f.(map[interface{}]interface{})
				field := new(Field)
				field.Id = uint32(i)
				if name, ok := f["name"]; ok && name != nil {
					field.Name = name.(string)
				}
				if type_, ok := f["type"]; ok && type_ != nil {
					field.Type = type_.(string)
				}
				space.FieldsById[field.Id] = field
				if field.Name != "" {
					space.Fields[field.Name] = field
				}
			}
		}

		schema.SpacesById[space.Id] = space
		schema.Spaces[space.Name] = space
	}

	// reload indexes
	req = conn.NewRequest(SelectRequest)
	req.fillSearch(vindexSpId, 0, []interface{}{})
	req.fillIterator(0, maxSchemas, IterAll)
	resp, err = req.perform()
	if err != nil {
		return err
	}
	for _, row := range resp.Data {
		row := row.([]interface{})
		index := new(Index)
		index.Id = uint32(row[1].(uint64))
		index.Name = row[2].(string)
		index.Type = row[3].(string)
		switch row[4].(type) {
		case uint64:
			index.Unique = row[4].(uint64) > 0
		case map[interface{}]interface{}:
			opts := row[4].(map[interface{}]interface{})
			index.Unique = opts["unique"].(bool)
		default:
			panic("unexpected schema format (index flags)")
		}
		switch row[5].(type) {
		case uint64:
			cnt := int(row[5].(uint64))
			for i := 0; i < cnt; i += 1 {
				field := new(IndexField)
				field.Id = uint32(row[6+i*2].(uint64))
				field.Type = row[7+i*2].(string)
				index.Fields = append(index.Fields, field)
			}
		case []interface{}:
			for _, f := range row[5].([]interface{}) {
				f := f.([]interface{})
				field := new(IndexField)
				field.Id = uint32(f[0].(uint64))
				field.Type = f[1].(string)
				index.Fields = append(index.Fields, field)
			}
		default:
			panic("unexpected schema format (index fields)")
		}
		spaceId := uint32(row[0].(uint64))
		schema.SpacesById[spaceId].IndexesById[index.Id] = index
		schema.SpacesById[spaceId].Indexes[index.Name] = index
	}
	conn.Schema = schema
	return nil
}

func (schema *Schema) resolveSpaceIndex(s interface{}, i interface{}) (spaceNo, indexNo uint32, err error) {
	var space *Space
	var index *Index
	var ok bool

	switch s := s.(type) {
	case string:
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
