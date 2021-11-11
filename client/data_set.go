package client

import (
	"fmt"

	"github.com/thulab/iginx-client-go/rpc"
)

type QueryDataSet struct {
	Paths      []string
	Types      []rpc.DataType
	Timestamps []int64
	Values     [][]interface{}
}

func NewQueryDataSet(paths []string, types []rpc.DataType, timeBuffer []byte, valuesList, bitmapList [][]byte) QueryDataSet {
	var values [][]interface{}
	for i := range valuesList {
		var rowValues []interface{}
		valuesBuffer := valuesList[i]
		bitmapBuffer := bitmapList[i]

		bitmap := NewBitmapWithBuf(len(types), bitmapBuffer)
		for j := range types {
			if notNil, _ := bitmap.Get(j); notNil {
				var value interface{}
				value, valuesBuffer = GetValueFromBytes(valuesBuffer, types[j])
				rowValues = append(rowValues, value)
			} else {
				rowValues = append(rowValues, nil)
			}
		}
		values = append(values, rowValues)
	}

	return QueryDataSet{
		Paths:      paths,
		Types:      types,
		Timestamps: GetLongArrayFromBytes(timeBuffer),
		Values:     values,
	}
}

func (s *QueryDataSet) PrintDataSet() {
	fmt.Println("Start print data set")
	fmt.Println("-------------------------------------")
	fmt.Print("Time ")
	for i := range s.Paths {
		fmt.Print(s.Paths[i], " ")
	}
	fmt.Println()
	for i := range s.Values {
		fmt.Print(s.Timestamps[i], " ")
		for j := range s.Values[i] {
			fmt.Print(s.Values[i][j], " ")
		}
		fmt.Println()
	}
	fmt.Println("-------------------------------------")
	fmt.Println()
}

type AggregateQueryDataSet struct {
	Paths         []string
	AggregateType rpc.AggregateType
	Timestamps    []int64
	Values        []interface{}
}

func NewAggregateQueryDataSet(paths []string, timeBuffer, valuesBuffer []byte, types []rpc.DataType, aggregateType rpc.AggregateType) AggregateQueryDataSet {
	dataSet := AggregateQueryDataSet{
		Paths:         paths,
		AggregateType: aggregateType,
		Values:        GetValueByDataTypeList(valuesBuffer, types),
	}

	if timeBuffer != nil {
		dataSet.Timestamps = GetLongArrayFromBytes(timeBuffer)
	}

	return dataSet
}

func (s *AggregateQueryDataSet) PrintDataSet() {
	fmt.Println("Start print aggregate data set")
	fmt.Println("-------------------------------------")
	if s.Timestamps != nil {
		for _, path := range s.Paths {
			fmt.Print(s.AggregateType.String()+"("+path+")", " ")
		}
		fmt.Println()
		for _, value := range s.Values {
			fmt.Print(value, " ")
		}
		fmt.Println()
	} else {
		for i := range s.Timestamps {
			fmt.Print("Time ")
			fmt.Print(s.AggregateType.String()+"("+s.Paths[i]+")", " ")
			fmt.Println()
			fmt.Print(s.Timestamps[i], " ")
			fmt.Print(s.Values[i])
			fmt.Println()
		}
	}
	fmt.Println("-------------------------------------")
	fmt.Println()
}

type Point struct {
	Path      string
	Type      rpc.DataType
	Timestamp int64
	value     interface{}
}

func NewPoint(path string, dataType rpc.DataType, timestamp int64, value interface{}) Point {
	return Point{
		Path:      path,
		Type:      dataType,
		Timestamp: timestamp,
		value:     value,
	}
}

type LastQueryDataSet struct {
	Points []Point
}

func NewLastQueryDataSet(paths []string, timeBuffer, valuesBuffer []byte, types []rpc.DataType) LastQueryDataSet {
	timestamps := GetLongArrayFromBytes(timeBuffer)
	values := GetValueByDataTypeList(valuesBuffer, types)

	var points []Point
	for i := range paths {
		points = append(points, NewPoint(paths[i], types[i], timestamps[i], values[i]))
	}
	return LastQueryDataSet{Points: points}
}

func (s *LastQueryDataSet) PrintDataSet() {
	fmt.Println("Start print aggregate data set")
	fmt.Println("-------------------------------------")
	fmt.Println("Time Series Value")
	for _, point := range s.Points {
		fmt.Println(point.Timestamp, " ", point.Path, " ", point.value)
	}
	fmt.Println("-------------------------------------")
	fmt.Println()
}
