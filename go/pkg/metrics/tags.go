package metrics

import (
	"fmt"
	"strconv"
)

func tagsFor(payload map[string]interface{}) []string {
	if len(payload) == 0 {
		return nil
	}
	tags := make([]string, 0, len(payload))
	for k, v := range payload {
		if v == nil || k == "payload" || k == "error" {
			continue
		}
		val := formatTagValue(v)
		if val == "" || len(val) > 128 {
			continue
		}
		tags = append(tags, k+":"+val)
	}
	return tags
}

func formatTagValue(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprint(v)
	}
}
