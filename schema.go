package tarantool

import "errors"

const (
	SpaceSchema  = 272
	SpaceSpace   = 280
	SpaceIndex   = 288
	SpaceFunc    = 296
	SpaceVSpace  = 281
	SpaceVIndex  = 289
	SpaceVFunc   = 297
	SpaceUser    = 304
	SpacePriv    = 312
	SpaceCluster = 320
)

const (
	IndexSpacePrimary = 0
	IndexSpaceName    = 2
	IndexIndexPrimary = 0
	IndexIndexName    = 2
)

// Space implements lookups on a box.space
type Space struct {
	No      uint32
	Name    string
	Type    string
	schema  *Schema
	index   map[string]*Index
	indexNo map[uint32]*Index
}

func newSpace(schema *Schema, no uint32, name, kind string) *Space {
	return &Space{
		No:      no,
		Name:    name,
		Type:    kind,
		schema:  schema,
		index:   make(map[string]*Index),
		indexNo: make(map[uint32]*Index),
	}
}

// Index resolves an index by name
func (space *Space) Index(name string) (index *Index, err error) {
	var ok bool
	if index, ok = space.index[name]; ok {
		return
	}

	var res *Response
	if res, err = space.schema.conn.Select(SpaceVIndex, IndexIndexName, 0, 1, 0, []interface{}{space.No, name}); err != nil {
		return
	}
	if res.Data == nil || len(res.Data) == 0 {
		if res, err = space.schema.conn.Select(SpaceIndex, IndexIndexName, 0, 1, 0, []interface{}{space.No, name}); err != nil {
			return
		}
	}

	var row []interface{}
	if res.Data == nil || len(res.Data) == 0 {
		return nil, errors.New(`no such space`)
	}
	if row, ok = res.Data[0].([]interface{}); !ok {
		return nil, errors.New(`unexpected result`)
	}

	return space.add(row)
}

func (space *Space) IndexNo(no uint32) (index *Index, err error) {
	var ok bool
	if index, ok = space.indexNo[no]; ok {
		return
	}

	var res *Response
	if res, err = space.schema.conn.Select(SpaceVIndex, IndexIndexPrimary, 0, 1, 0, []interface{}{space.No, no}); err != nil {
		return
	}
	if res.Data == nil || len(res.Data) == 0 {
		if res, err = space.schema.conn.Select(SpaceIndex, IndexIndexPrimary, 0, 1, 0, []interface{}{space.No, no}); err != nil {
			return
		}
	}

	var row []interface{}
	if res.Data == nil || len(res.Data) == 0 {
		return nil, errors.New(`no such index`)
	}
	if row, ok = res.Data[0].([]interface{}); !ok {
		return nil, errors.New(`unexpected result`)
	}

	return space.add(row)
}

func (space *Space) Indexes() ([]*Index, error) {
	var (
		res   *Response
		row   []interface{}
		index *Index
		err   error
		ok    bool
	)

	if res, err = space.schema.conn.Select(SpaceIndex, IndexIndexPrimary, 0, 0xffffffff, 0, []interface{}{space.No}); err != nil {
		return nil, err
	}

	indexes := make([]*Index, len(res.Data))
	for i, raw := range res.Data {
		if row, ok = raw.([]interface{}); !ok {
			return nil, errors.New("unexpected result")
		}

		if index, err = space.add(row); err != nil {
			return nil, err
		}

		indexes[i] = index
	}

	return indexes, nil
}

func (space *Space) add(row []interface{}) (index *Index, err error) {
	if row == nil || len(row) == 0 {
		return nil, errors.New(`no such index`)
	}

	index = &Index{Space: space, No: uint32(row[1].(uint64)), Name: row[2].(string)}
	space.index[index.Name] = index
	space.indexNo[index.No] = index

	return
}

type Index struct {
	Space *Space
	No    uint32
	Name  string
}

// Schema gives access to the schema definition, such as indexes and spaces.
type Schema struct {
	conn    *Connection
	space   map[string]*Space
	spaceNo map[uint32]*Space
}

func (conn *Connection) NewSchema() *Schema {
	return &Schema{
		conn:    conn,
		space:   make(map[string]*Space),
		spaceNo: make(map[uint32]*Space),
	}
}

// Space resolves a space by name
func (schema *Schema) Space(name string) (space *Space, err error) {
	var ok bool
	if space, ok = schema.space[name]; ok {
		return
	}

	var res *Response
	if res, err = schema.conn.Select(SpaceVSpace, IndexSpaceName, 0, 1, 0, []interface{}{name}); err != nil {
		return
	}
	if res.Data == nil || len(res.Data) == 0 {
		if res, err = schema.conn.Select(SpaceSpace, IndexSpaceName, 0, 1, 0, []interface{}{name}); err != nil {
			return
		}
	}

	var row []interface{}
	if res.Data == nil || len(res.Data) == 0 {
		return nil, errors.New(`no such space`)
	}
	if row, ok = res.Data[0].([]interface{}); !ok {
		return nil, errors.New(`unexpected result`)
	}

	return schema.add(row)
}

// SpaceNo resolves a space by number
func (schema *Schema) SpaceNo(no uint32) (space *Space, err error) {
	var ok bool

	if space, ok = schema.spaceNo[no]; ok {
		return
	}

	var res *Response
	if res, err = schema.conn.Select(SpaceVSpace, IndexSpacePrimary, 0, 1, 0, []interface{}{no}); err != nil {
		return
	}
	if res.Data == nil || len(res.Data) == 0 {
		if res, err = schema.conn.Select(SpaceSpace, IndexSpacePrimary, 0, 1, 0, []interface{}{no}); err != nil {
			return
		}
	}

	var row []interface{}
	if res.Data == nil || len(res.Data) == 0 {
		return nil, errors.New(`no such space`)
	}
	if row, ok = res.Data[0].([]interface{}); !ok {
		return nil, errors.New(`unexpected result`)
	}

	return schema.add(row)
}

// Spaces retrieves all spaces for a schema
func (schema *Schema) Spaces() ([]*Space, error) {
	var (
		res   *Response
		row   []interface{}
		space *Space
		err   error
		ok    bool
	)

	if res, err = schema.conn.Select(SpaceSpace, 0, 0, 0xffffffff, 0, []interface{}{}); err != nil {
		return nil, err
	}

	spaces := make([]*Space, len(res.Data))
	for i, raw := range res.Data {
		if row, ok = raw.([]interface{}); !ok {
			return nil, errors.New("unexpected result")
		}

		if space, err = schema.add(row); err != nil {
			return nil, err
		}

		spaces[i] = space
	}

	return spaces, nil
}

func (schema *Schema) add(row []interface{}) (space *Space, err error) {
	if row == nil || len(row) == 0 {
		return nil, errors.New(`no such space`)
	}

	space = newSpace(schema, uint32(row[0].(uint64)), row[2].(string), row[3].(string))
	schema.space[space.Name] = space
	schema.spaceNo[space.No] = space

	return
}
