package crud

import (
	"fmt"
)

// UnflattenRows can be used to convert received tuples to objects.
func UnflattenRows(tuples []interface{}, format []interface{}) ([]MapObject, error) {
	var (
		ok        bool
		fieldName string
		fieldInfo map[interface{}]interface{}
	)

	objects := []MapObject{}

	for _, tuple := range tuples {
		object := make(map[string]interface{})
		for fieldIdx, field := range tuple.([]interface{}) {
			if fieldInfo, ok = format[fieldIdx].(map[interface{}]interface{}); !ok {
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
