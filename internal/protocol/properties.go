package protocol

import (
	"sort"

	"google.golang.org/protobuf/proto"

	pulsar "minipulsar/pb"
)

// KeyValuesFromProperties converts a properties map into a sorted list of key/value pairs.
func KeyValuesFromProperties(properties map[string]string) []*pulsar.KeyValue {
	if len(properties) == 0 {
		return nil
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]*pulsar.KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, &pulsar.KeyValue{
			Key:   proto.String(key),
			Value: proto.String(properties[key]),
		})
	}
	return values
}

// PropertiesFromKeyValues converts Pulsar key/value pairs into a map.
func PropertiesFromKeyValues(values []*pulsar.KeyValue) map[string]string {
	if len(values) == 0 {
		return nil
	}
	properties := make(map[string]string, len(values))
	for _, kv := range values {
		if kv == nil {
			continue
		}
		properties[kv.GetKey()] = kv.GetValue()
	}
	if len(properties) == 0 {
		return nil
	}
	return properties
}
