// crud/compile_test.go
package crud

import (
	"testing"

	"github.com/tarantool/go-option"
)

// TestOptionTypesCompilation verifies that all option types are compiled correctly.
func TestOptionTypesCompilation(t *testing.T) {
	// Test BaseOpts
	baseOpts := BaseOpts{
		Timeout:      option.SomeFloat64(1.5),
		VshardRouter: option.SomeString("router"),
	}

	// Check that Get() is working.
	if timeout, exists := baseOpts.Timeout.Get(); !exists || timeout != 1.5 {
		t.Errorf("BaseOpts.Timeout.Get() failed")
	}

	// Test SimpleOperationOpts.
	simpleOpts := SimpleOperationOpts{
		Timeout:             option.SomeFloat64(2.0),
		VshardRouter:        option.SomeString("router2"),
		Fields:              MakeOptAny([]interface{}{"field1", "field2"}),
		BucketId:            option.SomeUint(456),
		FetchLatestMetadata: option.SomeBool(true),
		Noreturn:            option.SomeBool(false),
	}

	if bucket, exists := simpleOpts.BucketId.Get(); !exists || bucket != 456 {
		t.Errorf("BucketId.Get() failed: got %v, %v", bucket, exists)
	}

	if fields, exists := simpleOpts.Fields.Get(); !exists {
		t.Errorf("Fields.Get() failed")
	} else {
		t.Logf("Fields: %v", fields)
	}

	// Test OperationManyOpts.
	manyOpts := OperationManyOpts{
		Timeout:     option.SomeFloat64(3.0),
		StopOnError: option.SomeBool(true),
	}

	if stop, exists := manyOpts.StopOnError.Get(); !exists || !stop {
		t.Errorf("StopOnError.Get() failed")
	}
}

// TestMakeOptAny checks the operation of MakeOptAny (replacing MakeOptTuple).
func TestMakeOptAny(t *testing.T) {
	// Test with simple data types.
	testCases := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{"string", "test", "test"},
		{"number", 42, 42},
		{"nil", nil, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opt := MakeOptAny(tc.value)
			val, exists := opt.Get()

			if tc.value == nil {
				if exists {
					t.Errorf("Expected no value for nil input, but got %v", val)
				}
			} else {
				if !exists {
					t.Errorf("Expected value for %v, but got none", tc.value)
				}
				if val != tc.expected {
					t.Errorf("Expected %v, got %v", tc.expected, val)
				}
			}
		})
	}

	// Test with a slice - we check without comparing the values.
	t.Run("slice", func(t *testing.T) {
		sliceValue := []interface{}{"id", "name"}
		opt := MakeOptAny(sliceValue)
		val, exists := opt.Get()

		if !exists {
			t.Errorf("Expected value for slice, but got none")
		}

		// We check the type and length instead of direct comparison.
		if valSlice, ok := val.([]interface{}); !ok {
			t.Errorf("Expected slice type, got %T", val)
		} else if len(valSlice) != 2 {
			t.Errorf("Expected slice length 2, got %d", len(valSlice))
		} else {
			t.Logf("Slice test passed: %v", valSlice)
		}
	})
}
