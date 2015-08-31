package tarantool

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"reflect"
	"testing"
	"time"
)

var server = "127.0.0.1:3013"
var spaceNo = uint32(512)
var indexNo = uint32(0)
var limit = uint32(10)
var offset = uint32(0)
var iterator = IterAll
var key = []interface{}{12}
var tuple1 = []interface{}{12, "Hello World", "Olga"}
var tuple2 = []interface{}{12, "Hello Mars", "Anna"}
var upd_tuple = []interface{}{[]interface{}{"=", 1, "Hello Moon"}, []interface{}{"#", 2, 1}}

var functionName = "box.cfg()"
var functionTuple = []interface{}{"box.schema.SPACE_ID"}
var opts = Opts{Timeout: 500 * time.Millisecond}

const N = 500


func BenchmarkClientSerial(b *testing.B) {
	var err error

	client, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i++ {
		_, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
		if err != nil {
			b.Errorf("No connection available")
		}

	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	client, err := Connect(server, opts)
	if err != nil {
		b.Error(err)
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i += N {
		var fs [N]*Future
		for j := 0; j < N; j++ {
			fs[j] = client.SelectAsync(spaceNo, indexNo, offset, limit, iterator, key)
		}
		for j := 0; j < N; j++ {
			_, err = fs[j].Get()
			if err != nil {
				b.Error(err)
			}
		}

	}
}

type tuple struct {
	Id   int
	Msg  string
	Name string
}

func encodeTuple(e *msgpack.Encoder, v reflect.Value) error {
	t := v.Interface().(tuple)
	if err := e.EncodeSliceLen(3); err != nil {
		return err
	}
	if err := e.EncodeInt(t.Id); err != nil {
		return err
	}
	if err := e.EncodeString(t.Msg); err != nil {
		return err
	}
	if err := e.EncodeString(t.Name); err != nil {
		return err
	}
	return nil
}

func decodeTuple(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	t := v.Addr().Interface().(*tuple)
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if t.Id, err = d.DecodeInt(); err != nil {
		return err
	}
	if t.Msg, err = d.DecodeString(); err != nil {
		return err
	}
	if t.Name, err = d.DecodeString(); err != nil {
		return err
	}
	return nil
}

func init() {
	msgpack.Register(reflect.TypeOf(new(tuple)).Elem(), encodeTuple, decodeTuple)
}

func BenchmarkClientFutureTyped(b *testing.B) {
	var err error

	client, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i += N {
		var fs [N]*Future
		for j := 0; j < N; j++ {
			fs[j] = client.SelectAsync(spaceNo, indexNo, offset, limit, iterator, key)
		}
		for j := 0; j < N; j++ {
			var r []tuple
			err = fs[j].GetTyped(&r)
			if err != nil {
				b.Error(err)
			}
			if len(r) != 1 || r[0].Id != 12 {
				b.Errorf("Doesn't match %v", r)
			}
		}

	}
}

func BenchmarkClientFutureParallel(b *testing.B) {
	var err error

	client, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]*Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = client.SelectAsync(spaceNo, indexNo, offset, limit, iterator, key)
			}
			exit = j < N
			for j > 0 {
				j--
				_, err = fs[j].Get()
				if err != nil {
					b.Error(err)
				}
			}
		}
	})
}

func BenchmarkClientFutureParallelTyped(b *testing.B) {
	var err error

	client, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]*Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = client.SelectAsync(spaceNo, indexNo, offset, limit, iterator, key)
			}
			exit = j < N
			for j > 0 {
				var r []tuple
				j--
				err = fs[j].GetTyped(&r)
				if err != nil {
					b.Error(err)
				}
				if len(r) != 1 || r[0].Id != 12 {
					b.Errorf("Doesn't match %v", r)
				}
			}
		}
	})
}

func BenchmarkClientParrallel(b *testing.B) {
	client, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
			if err != nil {
				b.Errorf("No connection available")
			}
		}
	})
}

func TestClient(t *testing.T) {

	// Valid user/pass
	client, err := Connect(server, Opts{User: "test", Pass: "test"})
	if err != nil {
		t.Errorf("Should pass but error is [%s]", err.Error())
	}

	var resp *Response

	resp, err = client.Ping()
	fmt.Println("Ping")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Insert(spaceNo, tuple1)
	fmt.Println("Insert")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("Select")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	var tpl []tuple
	err = client.SelectTyped(spaceNo, indexNo, offset, limit, iterator, key, &tpl)
	fmt.Println("GetTyped")
	fmt.Println("ERROR", err)
	fmt.Println("Value", tpl)
	fmt.Println("----")

	resp, err = client.Replace(spaceNo, tuple2)
	fmt.Println("Replace")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("Select")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Update(spaceNo, indexNo, key, upd_tuple)
	fmt.Println("Update")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("Select")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	responses := make(chan *Response)
	cnt1 := 50
	cnt2 := 500
	for j := 0; j < cnt1; j++ {
		for i := 0; i < cnt2; i++ {
			go func() {
				resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
				responses <- resp
			}()
		}
		for i := 0; i < cnt2; i++ {
			resp = <-responses
			// fmt.Println(resp)
		}
	}

	resp, err = client.Delete(spaceNo, indexNo, key)
	fmt.Println("Delete")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Call(functionName, functionTuple)
	fmt.Println("Call")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")
}
