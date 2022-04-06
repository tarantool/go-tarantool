package tarantool

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// Ping sends empty request to Tarantool to check connection.
func (conn *Connection) Ping() (resp *Response, err error) {
	future := conn.newFuture(PingRequest)
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error { enc.EncodeMapLen(0); return nil }).Get()
}

func fillSearch(enc *msgpack.Encoder, spaceNo, indexNo uint32, key interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyIndexNo)
	enc.EncodeUint64(uint64(indexNo))
	enc.EncodeUint64(KeyKey)
	return enc.Encode(key)
}

func fillIterator(enc *msgpack.Encoder, offset, limit, iterator uint32) {
	enc.EncodeUint64(KeyIterator)
	enc.EncodeUint64(uint64(iterator))
	enc.EncodeUint64(KeyOffset)
	enc.EncodeUint64(uint64(offset))
	enc.EncodeUint64(KeyLimit)
	enc.EncodeUint64(uint64(limit))
}

func fillInsert(enc *msgpack.Encoder, spaceNo uint32, tuple interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyTuple)
	return enc.Encode(tuple)
}

// Select performs select to box space.
//
// It is equal to conn.SelectAsync(...).Get().
func (conn *Connection) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (resp *Response, err error) {
	return conn.SelectAsync(space, index, offset, limit, iterator, key).Get()
}

// Insert performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// It is equal to conn.InsertAsync(space, tuple).Get().
func (conn *Connection) Insert(space interface{}, tuple interface{}) (resp *Response, err error) {
	return conn.InsertAsync(space, tuple).Get()
}

// Replace performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
//
// It is equal to conn.ReplaceAsync(space, tuple).Get().
func (conn *Connection) Replace(space interface{}, tuple interface{}) (resp *Response, err error) {
	return conn.ReplaceAsync(space, tuple).Get()
}

// Delete performs deletion of a tuple by key.
// Result will contain array with deleted tuple.
//
// It is equal to conn.DeleteAsync(space, tuple).Get().
func (conn *Connection) Delete(space, index interface{}, key interface{}) (resp *Response, err error) {
	return conn.DeleteAsync(space, index, key).Get()
}

// Update performs update of a tuple by key.
// Result will contain array with updated tuple.
//
// It is equal to conn.UpdateAsync(space, tuple).Get().
func (conn *Connection) Update(space, index interface{}, key, ops interface{}) (resp *Response, err error) {
	return conn.UpdateAsync(space, index, key, ops).Get()
}

// Upsert performs "update or insert" action of a tuple by key.
// Result will not contain any tuple.
//
// It is equal to conn.UpsertAsync(space, tuple, ops).Get().
func (conn *Connection) Upsert(space interface{}, tuple, ops interface{}) (resp *Response, err error) {
	return conn.UpsertAsync(space, tuple, ops).Get()
}

// Call calls registered Tarantool function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays
//
// It is equal to conn.CallAsync(functionName, args).Get().
func (conn *Connection) Call(functionName string, args interface{}) (resp *Response, err error) {
	return conn.CallAsync(functionName, args).Get()
}

// Call17 calls registered Tarantool function.
// It uses request code for Tarantool 1.7, so result is not converted
// (though, keep in mind, result is always array)
//
// It is equal to conn.Call17Async(functionName, args).Get().
func (conn *Connection) Call17(functionName string, args interface{}) (resp *Response, err error) {
	return conn.Call17Async(functionName, args).Get()
}

// Eval passes Lua expression for evaluation.
//
// It is equal to conn.EvalAsync(space, tuple).Get().
func (conn *Connection) Eval(expr string, args interface{}) (resp *Response, err error) {
	return conn.EvalAsync(expr, args).Get()
}

// Execute passes sql expression to Tarantool for execution.
//
// It is equal to conn.ExecuteAsync(expr, args).Get().
// Since 1.6.0
func (conn *Connection) Execute(expr string, args interface{}) (resp *Response, err error) {
	return conn.ExecuteAsync(expr, args).Get()
}

// single used for conn.GetTyped for decode one tuple.
type single struct {
	res   interface{}
	found bool
}

func (s *single) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var len int
	if len, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if s.found = len >= 1; !s.found {
		return nil
	}
	if len != 1 {
		return errors.New("Tarantool returns unexpected value for Select(limit=1)")
	}
	return d.Decode(s.res)
}

// GetTyped performs select (with limit = 1 and offset = 0)
// to box space and fills typed result.
//
// It is equal to conn.SelectAsync(space, index, 0, 1, IterEq, key).GetTyped(&result)
func (conn *Connection) GetTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	s := single{res: result}
	err = conn.SelectAsync(space, index, 0, 1, IterEq, key).GetTyped(&s)
	return
}

// SelectTyped performs select to box space and fills typed result.
//
// It is equal to conn.SelectAsync(space, index, offset, limit, iterator, key).GetTyped(&result)
func (conn *Connection) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error) {
	return conn.SelectAsync(space, index, offset, limit, iterator, key).GetTyped(result)
}

// InsertTyped performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// It is equal to conn.InsertAsync(space, tuple).GetTyped(&result).
func (conn *Connection) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return conn.InsertAsync(space, tuple).GetTyped(result)
}

// ReplaceTyped performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
//
// It is equal to conn.ReplaceAsync(space, tuple).GetTyped(&result).
func (conn *Connection) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return conn.ReplaceAsync(space, tuple).GetTyped(result)
}

// DeleteTyped performs deletion of a tuple by key and fills result with deleted tuple.
//
// It is equal to conn.DeleteAsync(space, tuple).GetTyped(&result).
func (conn *Connection) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return conn.DeleteAsync(space, index, key).GetTyped(result)
}

// UpdateTyped performs update of a tuple by key and fills result with updated tuple.
//
// It is equal to conn.UpdateAsync(space, tuple, ops).GetTyped(&result).
func (conn *Connection) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	return conn.UpdateAsync(space, index, key, ops).GetTyped(result)
}

// CallTyped calls registered function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays
//
// It is equal to conn.CallAsync(functionName, args).GetTyped(&result).
func (conn *Connection) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	return conn.CallAsync(functionName, args).GetTyped(result)
}

// Call17Typed calls registered function.
// It uses request code for Tarantool 1.7, so result is not converted
// (though, keep in mind, result is always array)
//
// It is equal to conn.Call17Async(functionName, args).GetTyped(&result).
func (conn *Connection) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	return conn.Call17Async(functionName, args).GetTyped(result)
}

// EvalTyped passes Lua expression for evaluation.
//
// It is equal to conn.EvalAsync(space, tuple).GetTyped(&result).
func (conn *Connection) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	return conn.EvalAsync(expr, args).GetTyped(result)
}

// ExecuteTyped passes sql expression to Tarantool for execution.
//
// In addition to error returns sql info and columns meta data
// Since 1.6.0
func (conn *Connection) ExecuteTyped(expr string, args interface{}, result interface{}) (SQLInfo, []ColumnMetaData, error) {
	fut := conn.ExecuteAsync(expr, args)
	err := fut.GetTyped(&result)
	return fut.resp.SQLInfo, fut.resp.MetaData, err
}

// SelectAsync sends select request to Tarantool and returns Future.
func (conn *Connection) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *Future {
	future := conn.newFuture(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return conn.failFuture(future, err)
	}
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(6)
		fillIterator(enc, offset, limit, iterator)
		return fillSearch(enc, spaceNo, indexNo, key)
	})
}

// InsertAsync sends insert action to Tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
func (conn *Connection) InsertAsync(space interface{}, tuple interface{}) *Future {
	future := conn.newFuture(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return conn.failFuture(future, err)
	}
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return fillInsert(enc, spaceNo, tuple)
	})
}

// ReplaceAsync sends "insert or replace" action to Tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
func (conn *Connection) ReplaceAsync(space interface{}, tuple interface{}) *Future {
	future := conn.newFuture(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return conn.failFuture(future, err)
	}
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return fillInsert(enc, spaceNo, tuple)
	})
}

// DeleteAsync sends deletion action to Tarantool and returns Future.
// Future's result will contain array with deleted tuple.
func (conn *Connection) DeleteAsync(space, index interface{}, key interface{}) *Future {
	future := conn.newFuture(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return conn.failFuture(future, err)
	}
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		return fillSearch(enc, spaceNo, indexNo, key)
	})
}

// Update sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
func (conn *Connection) UpdateAsync(space, index interface{}, key, ops interface{}) *Future {
	future := conn.newFuture(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return conn.failFuture(future, err)
	}
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(4)
		if err := fillSearch(enc, spaceNo, indexNo, key); err != nil {
			return err
		}
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(ops)
	})
}

// UpsertAsync sends "update or insert" action to Tarantool and returns Future.
// Future's sesult will not contain any tuple.
func (conn *Connection) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *Future {
	future := conn.newFuture(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return conn.failFuture(future, err)
	}
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		enc.EncodeUint64(KeySpaceNo)
		enc.EncodeUint64(uint64(spaceNo))
		enc.EncodeUint64(KeyTuple)
		if err := enc.Encode(tuple); err != nil {
			return err
		}
		enc.EncodeUint64(KeyDefTuple)
		return enc.Encode(ops)
	})
}

// CallAsync sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.6, so future's result is always array of arrays
func (conn *Connection) CallAsync(functionName string, args interface{}) *Future {
	future := conn.newFuture(CallRequest)
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyFunctionName)
		enc.EncodeString(functionName)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

// Call17Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.7, so future's result will not be converted
// (though, keep in mind, result is always array)
func (conn *Connection) Call17Async(functionName string, args interface{}) *Future {
	future := conn.newFuture(Call17Request)
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyFunctionName)
		enc.EncodeString(functionName)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

// EvalAsync sends a Lua expression for evaluation and returns Future.
func (conn *Connection) EvalAsync(expr string, args interface{}) *Future {
	future := conn.newFuture(EvalRequest)
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyExpression)
		enc.EncodeString(expr)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

// ExecuteAsync sends a sql expression for execution and returns Future.
// Since 1.6.0
func (conn *Connection) ExecuteAsync(expr string, args interface{}) *Future {
	future := conn.newFuture(ExecuteRequest)
	return conn.sendFuture(future, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeySQLText)
		enc.EncodeString(expr)
		enc.EncodeUint64(KeySQLBind)
		return encodeSQLBind(enc, args)
	})
}

// KeyValueBind is a type for encoding named SQL parameters
type KeyValueBind struct {
	Key   string
	Value interface{}
}

//
// private
//

// this map is needed for caching names of struct fields in lower case
// to avoid extra allocations in heap by calling strings.ToLower()
var lowerCaseNames sync.Map

func encodeSQLBind(enc *msgpack.Encoder, from interface{}) error {
	// internal function for encoding single map in msgpack
	encodeKeyInterface := func(key string, val interface{}) error {
		if err := enc.EncodeMapLen(1); err != nil {
			return err
		}
		if err := enc.EncodeString(":" + key); err != nil {
			return err
		}
		if err := enc.Encode(val); err != nil {
			return err
		}
		return nil
	}

	encodeKeyValue := func(key string, val reflect.Value) error {
		if err := enc.EncodeMapLen(1); err != nil {
			return err
		}
		if err := enc.EncodeString(":" + key); err != nil {
			return err
		}
		if err := enc.EncodeValue(val); err != nil {
			return err
		}
		return nil
	}

	encodeNamedFromMap := func(mp map[string]interface{}) error {
		if err := enc.EncodeSliceLen(len(mp)); err != nil {
			return err
		}
		for k, v := range mp {
			if err := encodeKeyInterface(k, v); err != nil {
				return err
			}
		}
		return nil
	}

	encodeNamedFromStruct := func(val reflect.Value) error {
		if err := enc.EncodeSliceLen(val.NumField()); err != nil {
			return err
		}
		cached, ok := lowerCaseNames.Load(val.Type())
		if !ok {
			fields := make([]string, val.NumField())
			for i := 0; i < val.NumField(); i++ {
				key := val.Type().Field(i).Name
				fields[i] = strings.ToLower(key)
				v := val.Field(i)
				if err := encodeKeyValue(fields[i], v); err != nil {
					return err
				}
			}
			lowerCaseNames.Store(val.Type(), fields)
			return nil
		}

		fields := cached.([]string)
		for i := 0; i < val.NumField(); i++ {
			k := fields[i]
			v := val.Field(i)
			if err := encodeKeyValue(k, v); err != nil {
				return err
			}
		}
		return nil
	}

	encodeSlice := func(from interface{}) error {
		castedSlice, ok := from.([]interface{})
		if !ok {
			castedKVSlice := from.([]KeyValueBind)
			t := len(castedKVSlice)
			if err := enc.EncodeSliceLen(t); err != nil {
				return err
			}
			for _, v := range castedKVSlice {
				if err := encodeKeyInterface(v.Key, v.Value); err != nil {
					return err
				}
			}
			return nil
		}

		if err := enc.EncodeSliceLen(len(castedSlice)); err != nil {
			return err
		}
		for i := 0; i < len(castedSlice); i++ {
			if kvb, ok := castedSlice[i].(KeyValueBind); ok {
				k := kvb.Key
				v := kvb.Value
				if err := encodeKeyInterface(k, v); err != nil {
					return err
				}
			} else {
				if err := enc.Encode(castedSlice[i]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	val := reflect.ValueOf(from)
	switch val.Kind() {
	case reflect.Map:
		mp, ok := from.(map[string]interface{})
		if !ok {
			return errors.New("failed to encode map: wrong format")
		}
		if err := encodeNamedFromMap(mp); err != nil {
			return err
		}
	case reflect.Struct:
		if err := encodeNamedFromStruct(val); err != nil {
			return err
		}
	case reflect.Slice, reflect.Array:
		if err := encodeSlice(from); err != nil {
			return err
		}
	}
	return nil
}
