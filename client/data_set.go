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

func NewQueryDataSet(paths []string, types []rpc.DataType, timeBuffer []byte, valuesList, bitmapList [][]byte) *QueryDataSet {
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

	return &QueryDataSet{
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

func NewAggregateQueryDataSet(paths []string, timeBuffer, valuesBuffer []byte, types []rpc.DataType, aggregateType rpc.AggregateType) *AggregateQueryDataSet {
	dataSet := AggregateQueryDataSet{
		Paths:         paths,
		AggregateType: aggregateType,
		Values:        GetValueByDataTypeList(valuesBuffer, types),
	}

	if timeBuffer != nil {
		dataSet.Timestamps = GetLongArrayFromBytes(timeBuffer)
	}

	return &dataSet
}

func (s *AggregateQueryDataSet) PrintDataSet() {
	fmt.Println("Start print aggregate data set")
	fmt.Println("-------------------------------------")

	for _, path := range s.Paths {
		fmt.Print(s.AggregateType.String()+"("+path+")", " ")
	}
	fmt.Println()
	for _, value := range s.Values {
		fmt.Print(value, " ")
	}
	fmt.Println()

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

func NewLastQueryDataSet(paths []string, timeBuffer, valuesBuffer []byte, types []rpc.DataType) *LastQueryDataSet {
	timestamps := GetLongArrayFromBytes(timeBuffer)
	values := GetValueByDataTypeList(valuesBuffer, types)

	var points []Point
	for i := range paths {
		points = append(points, NewPoint(paths[i], types[i], timestamps[i], values[i]))
	}
	return &LastQueryDataSet{Points: points}
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

type SQLDataSet struct {
	Type          rpc.SqlType
	ParseErrorMsg string

	QueryDataSet          *QueryDataSet
	LastQueryDataSet      *LastQueryDataSet
	AggregateQueryDataSet *AggregateQueryDataSet

	TimeSeries  []*TimeSeries
	ReplicaNum  int32
	PointsNum   int64
	ClusterInfo *ClusterInfo
}

func NewSQLDataSet(resp *rpc.ExecuteSqlResp) *SQLDataSet {
	dataSet := &SQLDataSet{
		Type:          resp.GetType(),
		ParseErrorMsg: resp.GetParseErrorMsg(),
	}

	switch dataSet.Type {
	case rpc.SqlType_GetReplicaNum:
		dataSet.ReplicaNum = resp.GetReplicaNum()
		break
	case rpc.SqlType_CountPoints:
		dataSet.PointsNum = resp.GetPointsNum()
		break
	case rpc.SqlType_ShowTimeSeries:
		var timeSeries []*TimeSeries
		for i := 0; i < len(resp.GetPaths()); i++ {
			ts := NewTimeSeries(resp.GetPaths()[i], resp.GetDataTypeList()[i])
			timeSeries = append(timeSeries, &ts)
		}
		dataSet.TimeSeries = timeSeries
		break
	case rpc.SqlType_ShowClusterInfo:
		dataSet.ClusterInfo = NewClusterInfo(
			resp.GetIginxInfos(),
			resp.GetStorageEngineInfos(),
			resp.GetMetaStorageInfos(),
			resp.GetLocalMetaStorageInfo(),
		)
		break
	case rpc.SqlType_SimpleQuery,
		rpc.SqlType_DownsampleQuery,
		rpc.SqlType_ValueFilterQuery:
		dataSet.QueryDataSet = NewQueryDataSet(
			resp.GetPaths(),
			resp.GetDataTypeList(),
			resp.GetQueryDataSet().GetTimestamps(),
			resp.GetQueryDataSet().GetValuesList(),
			resp.GetQueryDataSet().GetBitmapList(),
		)
		break
	case rpc.SqlType_AggregateQuery:
		if resp.GetAggregateType() == rpc.AggregateType_LAST {
			dataSet.LastQueryDataSet = NewLastQueryDataSet(
				resp.GetPaths(),
				resp.GetTimestamps(),
				resp.GetValuesList(),
				resp.GetDataTypeList(),
			)
		} else {
			dataSet.AggregateQueryDataSet = NewAggregateQueryDataSet(
				resp.GetPaths(),
				resp.GetTimestamps(),
				resp.GetValuesList(),
				resp.GetDataTypeList(),
				resp.GetAggregateType(),
			)
		}
		break
	}

	return dataSet
}

func (s *SQLDataSet) GetParseErrorMsg() string {
	return s.ParseErrorMsg
}

func (s *SQLDataSet) IsQuery() bool {
	return s.Type == rpc.SqlType_SimpleQuery ||
		s.Type == rpc.SqlType_AggregateQuery ||
		s.Type == rpc.SqlType_DownsampleQuery ||
		s.Type == rpc.SqlType_ValueFilterQuery
}

func (s *SQLDataSet) GetReplicaNum() int32 {
	return s.ReplicaNum
}

func (s *SQLDataSet) GetPointsNum() int64 {
	return s.PointsNum
}

func (s *SQLDataSet) GetTimeSeries() []*TimeSeries {
	return s.TimeSeries
}

func (s *SQLDataSet) GetClusterInfo() *ClusterInfo {
	return s.ClusterInfo
}

func (s *SQLDataSet) GetQueryDataSet() *QueryDataSet {
	return s.QueryDataSet
}

func (s *SQLDataSet) GetLastQueryDataSet() *LastQueryDataSet {
	return s.LastQueryDataSet
}

func (s *SQLDataSet) GetAggregateQueryDataSet() *AggregateQueryDataSet {
	return s.AggregateQueryDataSet
}
