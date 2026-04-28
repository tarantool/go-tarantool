package crud

import (
	"fmt"
)

// UnflattenRows can be used to convert received tuples to objects.
func UnflattenRows(tuples []any, format []any) ([]MapObject, error) {
	var (
		ok        bool
		fieldName string
		fieldInfo map[any]any
	)

	objects := []MapObject{}

	for _, tuple := range tuples {
		object := make(map[string]any)
		for fieldIdx, field := range tuple.([]any) {
			if fieldInfo, ok = format[fieldIdx].(map[any]any); !ok {
				return nil, fmt.Errorf("unexpected space format: %q", format)
			}

			if fieldName, ok = fieldInfo["name"].(string); !ok {
				return nil, fmt.Errorf("unexpected space format: %q", format)
			}

			object[fieldName] = field
		}

		objects = append(objects, object)
	}

	return objects, nil
}
