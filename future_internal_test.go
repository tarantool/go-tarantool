package tarantool

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestFutureList returns an empty list initialized the way a connShard
// initializes its lists.
func newTestFutureList() *futureList {
	list := &futureList{}
	list.last = &list.first

	return list
}

// newTestFuture returns a pending future that is not attached to any
// connection.
func newTestFuture(reqid uint32) *future {
	fut := &future{requestId: reqid}
	fut.cond.L = &fut.mutex

	return fut
}

// newTestConnection returns a Connection that is just complete enough for
// markDone() to work on it.
func newTestConnection() *Connection {
	conn := &Connection{}
	conn.cond = sync.NewCond(&conn.mutex)

	return conn
}

func TestFutureList_popFirst(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []uint32
	}{
		{name: "empty"},
		{name: "single element", ids: []uint32{1}},
		{name: "head and tail", ids: []uint32{1, 3, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list := newTestFutureList()
			want := make([]*future, 0, len(tc.ids)+1)
			for _, id := range tc.ids {
				fut := newTestFuture(id)
				list.addFuture(fut)
				want = append(want, fut)
			}

			if len(want) > 0 {
				// Take the head, then append: list.last must still point
				// at the real tail, or the append would break the order.
				head := list.popFirst()
				require.Same(t, want[0], head, "popFirst must return the head")
				assert.Nil(t, head.next, "a popped future must be detached")
				if want = want[1:]; len(want) > 0 {
					require.Same(t, want[0], list.first)
				}

				appended := newTestFuture(7)
				list.addFuture(appended)
				want = append(want, appended)
			}

			for i, fut := range want {
				got := list.popFirst()
				require.Samef(t, fut, got, "element %d", i)
				assert.Nilf(t, got.next, "element %d must be detached", i)
			}

			require.Nil(t, list.popFirst(), "the list must be empty")
			assert.Nil(t, list.first)
			// last back at first is what keeps an emptied list usable.
			assert.Same(t, &list.first, list.last,
				"last must point at first once the list is emptied")
		})
	}
}

func TestFutureList_clear(t *testing.T) {
	wantErr := errors.New("connection closed")

	t.Run("empty", func(t *testing.T) {
		conn := newTestConnection()
		list := newTestFutureList()

		list.clear(wantErr, conn)

		assert.Nil(t, list.first)
		assert.Same(t, &list.first, list.last, "last must point at first")
		assert.Zero(t, conn.requestCnt.Load())
	})

	t.Run("non empty", func(t *testing.T) {
		conn := newTestConnection()
		list := newTestFutureList()
		futs := []*future{newTestFuture(1), newTestFuture(3), newTestFuture(5)}
		for _, fut := range futs {
			list.addFuture(fut)
		}
		conn.requestCnt.Store(int64(len(futs)))

		list.clear(wantErr, conn)

		assert.Nil(t, list.first)
		assert.Same(t, &list.first, list.last,
			"last must point at first once the list is cleared")
		assert.Zero(t, conn.requestCnt.Load(), "every future must be marked done")

		for i, fut := range futs {
			assert.Truef(t, fut.isFinished(), "future %d must be finished", i)
			require.ErrorIsf(t, fut.err, wantErr, "future %d must carry the error", i)
			assert.Nilf(t, fut.next, "future %d must be detached", i)
		}
	})

	// This case pins the ordering the list documentation requires: a future
	// must be unlinked before it is finished, because finishing hands it to
	// the caller, who may Release() it at once. A write to fut.next after
	// that point races with the reuse of the object, so the check has to be
	// a real concurrent one.
	t.Run("detaches before finish", func(t *testing.T) {
		conn := newTestConnection()
		list := newTestFutureList()
		futs := []*future{newTestFuture(1), newTestFuture(3), newTestFuture(5)}

		var wg sync.WaitGroup
		for _, fut := range futs {
			list.addFuture(fut)
			ch := fut.WaitChan()
			wg.Add(1)

			go func() {
				defer wg.Done()

				<-ch
				// The future belongs to us from here on, so reading its
				// link is safe unless clear() writes it after finishing.
				assert.Nil(t, fut.next)
			}()
		}
		conn.requestCnt.Store(int64(len(futs)))

		list.clear(wantErr, conn)
		wg.Wait()
	})
}

// TestFutureRelease_UnfinishedKeepsListIntact checks the Release()
// precondition: a future that has not completed yet still belongs to the
// connection, so a fire-and-forget Release() must not zero it. Zeroing a
// linked node truncates the list at it (request ids are never 0), so the
// futures behind it are never finished and never marked done.
func TestFutureRelease_UnfinishedKeepsListIntact(t *testing.T) {
	list := newTestFutureList()
	futs := []*future{newTestFuture(1), newTestFuture(3), newTestFuture(5)}
	for _, fut := range futs {
		list.addFuture(fut)
	}

	Future(futs[1]).Release()

	assert.Equal(t, uint32(3), futs[1].requestId,
		"an unfinished future must not be recycled")
	require.Same(t, futs[0], list.first)
	require.Same(t, futs[2], futs[1].next, "the list must stay intact")
	// The tail must stay reachable, which is what a truncation loses.
	require.Same(t, futs[2], list.findFuture(5, false))
}

// testResponse is a Response that only records its Release().
type testResponse struct {
	released *bool
}

func (*testResponse) Header() Header          { return Header{} }
func (r *testResponse) Release()              { *r.released = true }
func (*testResponse) Decode() ([]any, error)  { return nil, nil }
func (*testResponse) DecodeTyped(_ any) error { return nil }

// TestFutureRelease_FinishedReleasesResponse makes sure the precondition check
// does not disable Release() for the futures that are allowed to use it.
func TestFutureRelease_FinishedReleasesResponse(t *testing.T) {
	released := false

	fut := newTestFuture(1)
	fut.resp = &testResponse{released: &released}
	fut.finish()
	require.True(t, fut.isFinished())

	Future(fut).Release()
	assert.True(t, released, "a finished future must release its response")
}
