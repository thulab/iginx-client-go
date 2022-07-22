package client

import (
	"fmt"
	"log"

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
	if len(s.Timestamps) != 0 {
		fmt.Print("Time ")
	}
	for i := range s.Paths {
		fmt.Print(s.Paths[i], " ")
	}
	fmt.Println()
	for i := range s.Values {
		if len(s.Timestamps) != 0 && i < len(s.Timestamps) {
			fmt.Print(s.Timestamps[i], " ")
		}
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

type StateType int32

const (
	HasMore StateType = 0
	NoMore  StateType = 1
	Unknown StateType = 2
)

type StreamDataSet struct {
	session    *Session
	fetchSize  int32
	queryId    int64
	columns    []string
	types      []rpc.DataType
	valuesList [][]byte
	bitmapList [][]byte
	index      int
	state      StateType
}

func NewStreamDataSet(session *Session, fetchSize int32, queryId int64, columns []string, types []rpc.DataType, valuesList, bitmapList [][]byte) *StreamDataSet {
	return &StreamDataSet{
		session:    session,
		fetchSize:  fetchSize,
		queryId:    queryId,
		columns:    columns,
		types:      types,
		valuesList: valuesList,
		bitmapList: bitmapList,
		index:      0,
		state:      Unknown,
	}
}

func (s *StreamDataSet) Close() error {
	return s.session.closeQuery(s.queryId)
}

func (s *StreamDataSet) fetch() {
	if s.index != len(s.bitmapList) { // 只有之前的被消费完才有可能继续取数据
		return
	}
	s.bitmapList = nil
	s.valuesList = nil
	s.index = 0

	dataSet, hasMore, err := s.session.fetchResult(s.queryId, s.fetchSize)
	if err != nil {
		log.Printf("fail to fetch stream data, err: %s\n", err)
		s.state = Unknown
	}
	if dataSet != nil {
		s.bitmapList = dataSet.GetBitmapList()
		s.valuesList = dataSet.GetValuesList()
	}
	if hasMore {
		s.state = HasMore
	} else {
		s.state = NoMore
	}
}

func (s *StreamDataSet) HasMore() bool {
	if s.index < len(s.valuesList) {
		return true
	}
	s.bitmapList = nil
	s.valuesList = nil
	s.index = 0
	if s.state == HasMore || s.state == Unknown {
		s.fetch()
	}
	return s.valuesList != nil
}

func (s *StreamDataSet) NextRow() []interface{} {
	if !s.HasMore() {
		return nil
	}
	// nextRow 只会返回本地的 row，如果本地没有，在进行 hasMore 操作时候，就一定也已经取回来了
	bitmapBuffer := s.bitmapList[s.index]
	valuesBuffer := s.valuesList[s.index]
	s.index++

	var rowValues []interface{}
	bitmap := NewBitmapWithBuf(len(s.types), bitmapBuffer)
	for i := range s.types {
		if notNil, _ := bitmap.Get(i); notNil {
			var value interface{}
			value, valuesBuffer = GetValueFromBytes(valuesBuffer, s.types[i])
			rowValues = append(rowValues, value)
		} else {
			rowValues = append(rowValues, nil)
		}
	}
	return rowValues
}

func (s *StreamDataSet) PrintDataSet() {
	fmt.Println("Start print stream data set")
	fmt.Println("-------------------------------------")

	for _, column := range s.columns {
		fmt.Print(column, " ")
	}
	fmt.Println()
	for s.HasMore() {
		values := s.NextRow()
		for _, value := range values {
			fmt.Print(value, " ")
		}
	}
	fmt.Println()

	fmt.Println("-------------------------------------")
	fmt.Println()
}

type SQLDataSet struct {
	Type          rpc.SqlType
	ParseErrorMsg string

	QueryDataSet *QueryDataSet

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
	case rpc.SqlType_Query:
		dataSet.QueryDataSet = NewQueryDataSet(
			resp.GetPaths(),
			resp.GetDataTypeList(),
			resp.GetQueryDataSet().GetTimestamps(),
			resp.GetQueryDataSet().GetValuesList(),
			resp.GetQueryDataSet().GetBitmapList(),
		)
		break
	}

	return dataSet
}

func (s *SQLDataSet) GetParseErrorMsg() string {
	return s.ParseErrorMsg
}

func (s *SQLDataSet) IsQuery() bool {
	return s.Type == rpc.SqlType_Query
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
