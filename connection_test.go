package tarantool_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	. "github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

func TestOptsClonePreservesRequiredProtocolFeatures(t *testing.T) {
	original := Opts{
		RequiredProtocolInfo: ProtocolInfo{
			Version:  ProtocolVersion(100),
			Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
		},
	}

	origCopy := original.Clone()

	original.RequiredProtocolInfo.Features[1] = ProtocolFeature(98)

	require.Equal(t,
		origCopy,
		Opts{
			RequiredProtocolInfo: ProtocolInfo{
				Version:  ProtocolVersion(100),
				Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
			},
		})
}

func TestPrepareExecuteBlackbox(t *testing.T) {
	//ttShutdown, _, err := setTarantoolCluster("3301", "3302", "3303")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	db, err := connection_pool.ConnectWithWritableAwareDefaults(ctx, cancel, true, connection_pool.BasicAuth{"admin", "pass"},
		"localhost:3301",
		"localhost:3302",
		"localhost:3303",
		"bogus:2322",
	)
	defer func(db Connector) {
		if db != nil {
			if cErr := db.Close(); cErr != nil {
				panic(cErr)
			}
			if db.ConnectedNow() {
				panic("still connected")
			}
		}
		//ttShutdown()
	}(db)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execPrepareExecute(ctx, db)
		}()
	}
	wg.Wait()
	time.Sleep(180 * time.Second)
}

func execPrepareExecute(ctx context.Context, db Connector) {
	for ctx != nil && ctx.Err() == nil {
		id := uuid.New().String()
		id2 := uuid.New().String()
		if db == nil || ctx == nil {
			return
		}
		if r, rErr := db.PrepareExecute("INSERT into test_table(id, name, type) values (:id,:name,:type)", map[string]interface{}{"name": id2, "type": 3, "id": id}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != OkCode {
			if r == nil || r.Code != ER_TUPLE_FOUND {
				panic(errors.New(fmt.Sprintf("Insert failed because: %v --- %v\n", rErr, r)))
			}
		}
		if db == nil || ctx == nil {
			return
		}
		if r, rErr := db.PrepareExecute("SELECT * from test_table where name=:name and type=:type", map[string]interface{}{"name": id2, "type": 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != OkCode {
			//if r, rErr := db.Select("TEST_TABLE", "T_IDX_1", 0, 1, tarantool.IterEq, []interface{}{id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
			panic(errors.New(fmt.Sprintf("Query failed because: %v------%v\n", rErr, r)))
		} else {
			if rErr != nil && !errors.Is(rErr, ctx.Err()) {
				panic(rErr)
			}
			if len(r.Tuples()) != 0 {
				single := r.Tuples()[0]
				if single[0].(string) != id {
					panic(errors.New(fmt.Sprintf("expected:%v actual:%v", id, single[0].(string))))
				}
			} else {
				fmt.Sprintln("Query returned nothing")
			}
		}
		if db == nil || ctx == nil {
			return
		}
		if r, rErr := db.PrepareExecute("DELETE from test_table where name=:name and type=:type", map[string]interface{}{"name": id2, "type": 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != OkCode {
			//if r, rErr := db.Delete("TEST_TABLE", "T_IDX_1", []interface{}{id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
			if r == nil || r.Code != ER_TUPLE_NOT_FOUND {
				panic(errors.New(fmt.Sprintf("Delete failed because: %v --- %v\n", rErr, r)))
			}
		}
	}

}

func TestPrepareExecuteConnectionReestablished(t *testing.T) {
	ttShutdown, _, err := setTarantoolCluster("3301", "3302", "3303")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	db, err := connection_pool.ConnectWithWritableAwareDefaults(ctx, cancel, true, connection_pool.BasicAuth{"admin", "pass"},
		"localhost:3301",
		"localhost:3302",
		"localhost:3303",
		"bogus:2322",
	)
	defer func(db Connector) {
		if db != nil {
			if cErr := db.Close(); cErr != nil {
				panic(cErr)
			}
			if db.ConnectedNow() {
				panic("still connected")
			}
		}
		ttShutdown()
	}(db)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}
	execPrepareExecute(ctx, db)
	ttShutdown()
	ttShutdown, _, err = setTarantoolCluster("3301", "3302", "3303")
	execPrepareExecute(ctx, db)
}

func setTarantoolCluster(ports ...string) (func(), []test_helpers.TarantoolInstance, error) {
	tts := make([]test_helpers.TarantoolInstance, len(ports))
	for i, p := range ports {
		var err error
		_ = os.MkdirAll(fmt.Sprintf("/tmp/tarantool_data/%v/memtx", p), 0777)
		_ = os.MkdirAll(fmt.Sprintf("/tmp/tarantool_data/%v/wal", p), 0777)
		tts[i], err = test_helpers.StartTarantool(test_helpers.StartOpts{
			InitScript:   fmt.Sprintf("./.testdata/tarantool_bootstrap_%v.lua", p),
			Listen:       fmt.Sprintf("127.0.0.1:%v", p),
			User:         "admin",
			Pass:         "pass",
			ConnectRetry: 5,
			RetryTimeout: 1 * time.Second,
			WaitStart:    time.Duration(i * int(5*time.Second)),
		})
		if err != nil {
			panic(err)
		}
	}
	return func() {
		for _, tt := range tts {
			test_helpers.StopTarantoolWithCleanup(tt)
		}
	}, tts, nil
}
