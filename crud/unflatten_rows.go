package crud

import (
	"fmt"
)

// UnflattenRows can be used to convert received tuples to objects.
func UnflattenRows(tuples []interface{}, format []interface{}) ([]MapObject, error) {
	var (
		ok        bool
		tuple     Tuple
		fieldName string
		fieldInfo map[interface{}]interface{}
	)

	objects := []MapObject{}

	for _, rawTuple := range tuples {
		object := make(map[string]interface{})
		if tuple, ok = rawTuple.(Tuple); !ok {
			return nil, fmt.Errorf("Unexpected tuple format: %q", rawTuple)
		}

		for fieldIdx, field := range tuple {
			if fieldInfo, ok = format[fieldIdx].(map[interface{}]interface{}); !ok {
				return nil, fmt.Errorf("Unexpected space format: %q", format)
			}

			if fieldName, ok = fieldInfo["name"].(string); !ok {
				return nil, fmt.Errorf("Unexpected space format: %q", format)
			}

			object[fieldName] = field
		}

		objects = append(objects, object)
	}

	return objects, nil
}
