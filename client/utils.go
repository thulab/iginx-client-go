package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/thulab/iginx-client-go/rpc"
)

func TimestampsToBytes(timestamps []int64) ([]byte, error) {
	values := make([]interface{}, len(timestamps))
	types := make([]rpc.DataType, len(timestamps))
	for _, timestamp := range timestamps {
		values = append(values, timestamp)
		types = append(types, rpc.DataType_LONG)
	}
	return RowValuesToBytes(values, types)
}

func RowValuesToBytes(values []interface{}, types []rpc.DataType) ([]byte, error) {
	buffer := &bytes.Buffer{}
	for i, value := range values {
		if value == nil {
			continue
		}
		switch types[i] {
		case rpc.DataType_BOOLEAN:
			switch value.(type) {
			case bool:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be bool", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_INTEGER:
			switch value.(type) {
			case int32:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be int32", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_LONG:
			switch value.(type) {
			case int64:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be int64", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_FLOAT:
			switch value.(type) {
			case float32:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be float32", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_DOUBLE:
			switch value.(type) {
			case float64:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be float64", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_BINARY:
			switch s := value.(type) {
			case string:
				size := len(s)
				binary.Write(buffer, binary.BigEndian, int32(size))
				binary.Write(buffer, binary.BigEndian, []byte(s))
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be string", i, value, reflect.TypeOf(value))
			}
		default:
			return nil, fmt.Errorf("types[%d] is incorrect, it must in (BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT)", i)
		}
	}
	return buffer.Bytes(), nil
}

func ColumnValuesToBytes(values []interface{}, dataType rpc.DataType) ([]byte, error) {
	buffer := &bytes.Buffer{}
	for i, value := range values {
		if value == nil {
			continue
		}
		switch dataType {
		case rpc.DataType_BOOLEAN:
			switch value.(type) {
			case bool:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be bool", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_INTEGER:
			switch value.(type) {
			case int32:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be int32", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_LONG:
			switch value.(type) {
			case int64:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be int64", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_FLOAT:
			switch value.(type) {
			case float32:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be float32", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_DOUBLE:
			switch value.(type) {
			case float64:
				binary.Write(buffer, binary.BigEndian, value)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be float64", i, value, reflect.TypeOf(value))
			}
		case rpc.DataType_BINARY:
			switch s := value.(type) {
			case string:
				size := len(s)
				binary.Write(buffer, binary.BigEndian, int32(size))
				binary.Write(buffer, binary.BigEndian, []byte(s))
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be string", i, value, reflect.TypeOf(value))
			}
		default:
			return nil, fmt.Errorf("types[%d] is incorrect, it must in (BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT)", i)
		}
	}
	return buffer.Bytes(), nil
}

func GetLongArrayFromBytes(buffer []byte) []int64 {
	array := make([]int64, len(buffer)/8)
	for i := range array {
		var time interface{}
		time, buffer = GetValueFromBytes(buffer, rpc.DataType_LONG)
		array[i] = time.(int64)
	}
	return array
}

func GetValueByDataTypeList(buffer []byte, dataTypeList []rpc.DataType) []interface{} {
	var values []interface{}
	for _, dataType := range dataTypeList {
		var value interface{}
		value, buffer = GetValueFromBytes(buffer, dataType)
		values = append(values, value)
	}
	return values
}

func GetValueFromBytes(buffer []byte, dataType rpc.DataType) (interface{}, []byte) {
	switch dataType {
	case rpc.DataType_BOOLEAN:
		value := buffer[:1]
		return bool(value[0] != 0), buffer[1:]
	case rpc.DataType_INTEGER:
		value := buffer[:4]
		return bytesToInt32(value), buffer[4:]
	case rpc.DataType_LONG:
		value := buffer[:8]
		return bytesToInt64(value), buffer[8:]
	case rpc.DataType_FLOAT:
		value := buffer[:4]
		bits := binary.BigEndian.Uint32(value)
		return math.Float32frombits(bits), buffer[4:]
	case rpc.DataType_DOUBLE:
		value := buffer[:8]
		bits := binary.BigEndian.Uint64(value)
		return math.Float64frombits(bits), buffer[8:]
	case rpc.DataType_BINARY:
		length := bytesToInt32(buffer[:4])
		value := buffer[4 : 4+length]
		return string(value), buffer[4+length:]
	default:
		return nil, nil
	}
}

func bytesToInt32(bys []byte) int32 {
	bytebuff := bytes.NewBuffer(bys)
	var data int32
	binary.Read(bytebuff, binary.BigEndian, &data)
	return int32(data)
}

func bytesToInt64(bys []byte) int64 {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return int64(data)
}
