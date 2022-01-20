package client

import (
	"context"
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/thulab/iginx-client-go/rpc"
)

const (
	DefaultUsername = "root"
	DefaultPassword = "root"

	SuccessCode = 200
)

type Session struct {
	host     string
	port     string
	username string
	password string

	isClose   bool
	client    *rpc.IServiceClient
	sessionId int64
	transport thrift.TTransport
}

func NewSession(host, port, username, password string) *Session {
	return &Session{
		host:      host,
		port:      port,
		username:  username,
		password:  password,
		isClose:   true,
		client:    nil,
		sessionId: 0,
		transport: nil,
	}
}

func NewSessionWithDefaultUser(host, port string) *Session {
	return &Session{
		host:      host,
		port:      port,
		username:  DefaultUsername,
		password:  DefaultPassword,
		isClose:   true,
		client:    nil,
		sessionId: 0,
		transport: nil,
	}
}

func (s *Session) Open() error {
	if !s.isClose {
		return nil
	}

	var err error
	s.transport, err = thrift.NewTSocket(net.JoinHostPort(s.host, s.port))
	if err != nil {
		return err
	}

	if !s.transport.IsOpen() {
		err = s.transport.Open()
		if err != nil {
			return err
		}
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(s.transport)
	oprot := protocolFactory.GetProtocol(s.transport)
	s.client = rpc.NewIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	req := rpc.OpenSessionReq{
		Username: &s.username,
		Password: &s.password,
	}

	resp, err := s.client.OpenSession(context.Background(), &req)
	if err != nil {
		return err
	} else if resp == nil {
		return errors.New("open session resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return err
	}

	s.sessionId = resp.GetSessionId()
	s.isClose = false

	return nil
}

func (s *Session) Close() error {
	if s.isClose {
		return nil
	}

	req := rpc.CloseSessionReq{
		SessionId: s.sessionId,
	}

	defer func() {
		s.isClose = true
		if s.transport.IsOpen() {
			_ = s.transport.Close()
		}
	}()

	status, err := s.client.CloseSession(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) AddStorageEngine(ip, port, engineType string, extra map[string]string) error {
	portInt32, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return err
	}

	engine := rpc.StorageEngine{
		IP:          ip,
		Port:        int32(portInt32),
		Type:        engineType,
		ExtraParams: extra,
	}
	engines := []*rpc.StorageEngine{&engine}

	return s.BatchAddStorageEngine(engines)
}

func (s *Session) BatchAddStorageEngine(engines []*rpc.StorageEngine) error {
	req := rpc.AddStorageEnginesReq{
		SessionId:      s.sessionId,
		StorageEngines: engines,
	}

	status, err := s.client.AddStorageEngines(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) GetReplicaNum() (int32, error) {
	req := rpc.GetReplicaNumReq{
		SessionId: s.sessionId,
	}

	resp, err := s.client.GetReplicaNum(context.Background(), &req)
	if err != nil {
		return 0, err
	} else if resp == nil {
		return 0, errors.New("get replica num resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return 0, err
	}

	return resp.GetReplicaNum(), nil
}

func (s *Session) GetClusterInfo() (*ClusterInfo, error) {
	req := rpc.GetClusterInfoReq{
		SessionId: s.sessionId,
	}

	resp, err := s.client.GetClusterInfo(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("get cluster info resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	ret := NewClusterInfoWithResp(resp)
	return ret, nil
}

func (s *Session) AddUser(username, password string, auths []rpc.AuthType) error {
	req := rpc.AddUserReq{
		SessionId: s.sessionId,
		Username:  username,
		Password:  password,
		Auths:     auths,
	}

	status, err := s.client.AddUser(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) DeleteUser(username string) error {
	req := rpc.DeleteUserReq{
		SessionId: s.sessionId,
		Username:  username,
	}

	status, err := s.client.DeleteUser(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) UpdateUser(username, password string, auths []rpc.AuthType) error {
	req := rpc.UpdateUserReq{
		SessionId: s.sessionId,
		Username:  username,
		Password:  &password,
		Auths:     auths,
	}

	status, err := s.client.UpdateUser(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) ListTimeSeries() ([]TimeSeries, error) {
	req := rpc.ShowColumnsReq{
		SessionId: s.sessionId,
	}

	resp, err := s.client.ShowColumns(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("show columns resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	var ret []TimeSeries
	for i := 0; i < len(resp.GetPaths()); i++ {
		ts := NewTimeSeries(resp.GetPaths()[i], resp.GetDataTypeList()[i])
		ret = append(ret, ts)
	}
	return ret, nil
}

func (s *Session) DeleteTimeSeries(path string) error {
	paths := []string{path}
	return s.BatchDeleteTimeSeries(paths)
}

func (s *Session) BatchDeleteTimeSeries(paths []string) error {
	req := rpc.DeleteColumnsReq{
		SessionId: s.sessionId,
		Paths:     paths,
	}

	status, err := s.client.DeleteColumns(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) InsertRowRecords(paths []string, timestamps []int64, valueList [][]interface{}, dataTypeList []rpc.DataType) error {
	if paths == nil || timestamps == nil || valueList == nil || dataTypeList == nil ||
		len(paths) == 0 || len(timestamps) == 0 || len(valueList) == 0 || len(dataTypeList) == 0 {
		return errors.New("invalid insert request")
	}
	if len(paths) != len(dataTypeList) {
		return errors.New("the sizes of paths and dataTypeList should be equal")
	}
	if len(timestamps) != len(valueList) {
		return errors.New("the sizes of timestamps and valuesList should be equal")
	}

	// 保证时间戳递增
	timeIndex := make([]int, len(timestamps))
	for i := range timestamps {
		timeIndex[i] = i
	}
	sort.Slice(timeIndex, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	var sortedValueList [][]interface{}
	for i := range valueList {
		sortedValueList = append(sortedValueList, valueList[timeIndex[i]])
	}
	// 保证序列递增
	pathIndex := make([]int, len(paths))
	for i := range paths {
		pathIndex[i] = i
	}
	sort.Slice(pathIndex, func(i, j int) bool {
		return paths[i] < paths[j]
	})
	sort.Strings(paths)
	// 重排数据类型
	var sortedDataTypeList []rpc.DataType
	for i := range pathIndex {
		sortedDataTypeList = append(sortedDataTypeList, dataTypeList[pathIndex[i]])
	}
	// 重排数据
	for i := range sortedValueList {
		var tmpValues []interface{}
		for j := range sortedValueList[i] {
			tmpValues = append(tmpValues, sortedValueList[i][pathIndex[j]])
		}
		sortedValueList[i] = tmpValues
	}
	// 压缩数据
	var valueBufferList, bitmapBufferList [][]byte
	for i := range sortedValueList {
		rowValues := sortedValueList[i]
		rowValuesBuffer, err := RowValuesToBytes(rowValues, sortedDataTypeList)
		if err != nil {
			return err
		}
		valueBufferList = append(valueBufferList, rowValuesBuffer)
		bitmap := NewBitmap(len(rowValues))
		for j := range rowValues {
			if rowValues[j] != nil {
				err = bitmap.Mark(j)
				if err != nil {
					return err
				}
			}
		}
		bitmapBufferList = append(bitmapBufferList, bitmap.GetBitmap())
	}
	timeBytes, err := TimestampsToBytes(timestamps)
	if err != nil {
		return err
	}

	req := rpc.InsertRowRecordsReq{
		SessionId:      s.sessionId,
		Paths:          paths,
		Timestamps:     timeBytes,
		ValuesList:     valueBufferList,
		BitmapList:     bitmapBufferList,
		DataTypeList:   sortedDataTypeList,
		AttributesList: nil,
	}

	status, err := s.client.InsertRowRecords(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) InsertNonAlignedRowRecords(paths []string, timestamps []int64, valueList [][]interface{}, dataTypeList []rpc.DataType) error {
	if paths == nil || timestamps == nil || valueList == nil || dataTypeList == nil ||
		len(paths) == 0 || len(timestamps) == 0 || len(valueList) == 0 || len(dataTypeList) == 0 {
		return errors.New("invalid insert request")
	}
	if len(paths) != len(dataTypeList) {
		return errors.New("the sizes of paths and dataTypeList should be equal")
	}
	if len(timestamps) != len(valueList) {
		return errors.New("the sizes of timestamps and valuesList should be equal")
	}

	// 保证时间戳递增
	timeIndex := make([]int, len(timestamps))
	for i := range timestamps {
		timeIndex[i] = i
	}
	sort.Slice(timeIndex, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	var sortedValueList [][]interface{}
	for i := range valueList {
		sortedValueList = append(sortedValueList, valueList[timeIndex[i]])
	}
	// 保证序列递增
	pathIndex := make([]int, len(paths))
	for i := range paths {
		pathIndex[i] = i
	}
	sort.Slice(pathIndex, func(i, j int) bool {
		return paths[i] < paths[j]
	})
	sort.Strings(paths)
	// 重排数据类型
	var sortedDataTypeList []rpc.DataType
	for i := range pathIndex {
		sortedDataTypeList = append(sortedDataTypeList, dataTypeList[pathIndex[i]])
	}
	// 重排数据
	for i := range sortedValueList {
		var tmpValues []interface{}
		for j := range sortedValueList[i] {
			tmpValues = append(tmpValues, sortedValueList[i][pathIndex[j]])
		}
		sortedValueList[i] = tmpValues
	}
	// 压缩数据
	var valueBufferList, bitmapBufferList [][]byte
	for i := range sortedValueList {
		rowValues := sortedValueList[i]
		rowValuesBuffer, err := RowValuesToBytes(rowValues, sortedDataTypeList)
		if err != nil {
			return err
		}
		valueBufferList = append(valueBufferList, rowValuesBuffer)
		bitmap := NewBitmap(len(rowValues))
		for j := range rowValues {
			if rowValues[j] != nil {
				err = bitmap.Mark(j)
				if err != nil {
					return err
				}
			}
		}
		bitmapBufferList = append(bitmapBufferList, bitmap.GetBitmap())
	}
	timeBytes, err := TimestampsToBytes(timestamps)
	if err != nil {
		return err
	}

	req := rpc.InsertNonAlignedRowRecordsReq{
		SessionId:      s.sessionId,
		Paths:          paths,
		Timestamps:     timeBytes,
		ValuesList:     valueBufferList,
		BitmapList:     bitmapBufferList,
		DataTypeList:   sortedDataTypeList,
		AttributesList: nil,
	}

	status, err := s.client.InsertNonAlignedRowRecords(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) InsertColumnRecords(paths []string, timestamps []int64, valueList [][]interface{}, dataTypeList []rpc.DataType) error {
	if paths == nil || timestamps == nil || valueList == nil || dataTypeList == nil ||
		len(paths) == 0 || len(timestamps) == 0 || len(valueList) == 0 || len(dataTypeList) == 0 {
		return errors.New("invalid insert request")
	}
	if len(paths) != len(dataTypeList) {
		return errors.New("the sizes of paths and dataTypeList should be equal")
	}
	if len(paths) != len(valueList) {
		return errors.New("the sizes of paths and valuesList should be equal")
	}

	// 保证时间戳递增
	timeIndex := make([]int, len(timestamps))
	for i := range timestamps {
		timeIndex[i] = i
	}
	sort.Slice(timeIndex, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	for i := range valueList {
		var values []interface{}
		for j := range timestamps {
			values = append(values, valueList[i][timeIndex[j]])
		}
		valueList[i] = values
	}
	// 保证序列递增
	pathIndex := make([]int, len(paths))
	for i := range paths {
		pathIndex[i] = i
	}
	sort.Slice(pathIndex, func(i, j int) bool {
		return paths[i] < paths[j]
	})
	sort.Strings(paths)
	// 重排数据和数据类型
	var sortedValueList [][]interface{}
	var sortedDataTypeList []rpc.DataType
	for i := range pathIndex {
		sortedValueList = append(sortedValueList, valueList[pathIndex[i]])
		sortedDataTypeList = append(sortedDataTypeList, dataTypeList[pathIndex[i]])
	}
	// 压缩数据
	var valueBufferList, bitmapBufferList [][]byte
	for i := range sortedValueList {
		rowValues := sortedValueList[i]
		rowValuesBuffer, err := ColumnValuesToBytes(rowValues, sortedDataTypeList[i])
		if err != nil {
			return err
		}
		valueBufferList = append(valueBufferList, rowValuesBuffer)
		bitmap := NewBitmap(len(rowValues))
		for j := range rowValues {
			if rowValues[j] != nil {
				err = bitmap.Mark(j)
				if err != nil {
					return err
				}
			}
		}
		bitmapBufferList = append(bitmapBufferList, bitmap.GetBitmap())
	}
	timeBytes, err := TimestampsToBytes(timestamps)
	if err != nil {
		return err
	}

	req := rpc.InsertColumnRecordsReq{
		SessionId:      s.sessionId,
		Paths:          paths,
		Timestamps:     timeBytes,
		ValuesList:     valueBufferList,
		BitmapList:     bitmapBufferList,
		DataTypeList:   sortedDataTypeList,
		AttributesList: nil,
	}

	status, err := s.client.InsertColumnRecords(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) InsertNonAlignedColumnRecords(paths []string, timestamps []int64, valueList [][]interface{}, dataTypeList []rpc.DataType) error {
	if paths == nil || timestamps == nil || valueList == nil || dataTypeList == nil ||
		len(paths) == 0 || len(timestamps) == 0 || len(valueList) == 0 || len(dataTypeList) == 0 {
		return errors.New("invalid insert request")
	}
	if len(paths) != len(dataTypeList) {
		return errors.New("the sizes of paths and dataTypeList should be equal")
	}
	if len(paths) != len(valueList) {
		return errors.New("the sizes of paths and valuesList should be equal")
	}

	// 保证时间戳递增
	timeIndex := make([]int, len(timestamps))
	for i := range timestamps {
		timeIndex[i] = i
	}
	sort.Slice(timeIndex, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	for i := range valueList {
		var values []interface{}
		for j := range timestamps {
			values = append(values, valueList[i][timeIndex[j]])
		}
		valueList[i] = values
	}
	// 保证序列递增
	pathIndex := make([]int, len(paths))
	for i := range paths {
		pathIndex[i] = i
	}
	sort.Slice(pathIndex, func(i, j int) bool {
		return paths[i] < paths[j]
	})
	sort.Strings(paths)
	// 重排数据和数据类型
	var sortedValueList [][]interface{}
	var sortedDataTypeList []rpc.DataType
	for i := range pathIndex {
		sortedValueList = append(sortedValueList, valueList[pathIndex[i]])
		sortedDataTypeList = append(sortedDataTypeList, dataTypeList[pathIndex[i]])
	}
	// 压缩数据
	var valueBufferList, bitmapBufferList [][]byte
	for i := range sortedValueList {
		rowValues := sortedValueList[i]
		rowValuesBuffer, err := ColumnValuesToBytes(rowValues, sortedDataTypeList[i])
		if err != nil {
			return err
		}
		valueBufferList = append(valueBufferList, rowValuesBuffer)
		bitmap := NewBitmap(len(rowValues))
		for j := range rowValues {
			if rowValues[j] != nil {
				err = bitmap.Mark(j)
				if err != nil {
					return err
				}
			}
		}
		bitmapBufferList = append(bitmapBufferList, bitmap.GetBitmap())
	}
	timeBytes, err := TimestampsToBytes(timestamps)
	if err != nil {
		return err
	}

	req := rpc.InsertNonAlignedColumnRecordsReq{
		SessionId:      s.sessionId,
		Paths:          paths,
		Timestamps:     timeBytes,
		ValuesList:     valueBufferList,
		BitmapList:     bitmapBufferList,
		DataTypeList:   sortedDataTypeList,
		AttributesList: nil,
	}

	status, err := s.client.InsertNonAlignedColumnRecords(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) DeleteData(path string, startTime, endTime int64) error {
	paths := []string{path}
	return s.BatchDeleteData(paths, startTime, endTime)
}

func (s *Session) BatchDeleteData(paths []string, startTime, endTime int64) error {
	req := rpc.DeleteDataInColumnsReq{
		SessionId: s.sessionId,
		Paths:     paths,
		StartTime: startTime,
		EndTime:   endTime,
	}

	status, err := s.client.DeleteDataInColumns(context.Background(), &req)
	if err != nil {
		return err
	}

	err = s.verifyStatus(status)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) Query(paths []string, startTime, endTime int64) (*QueryDataSet, error) {
	req := rpc.QueryDataReq{
		SessionId: s.sessionId,
		Paths:     s.mergeAndSortPaths(paths),
		StartTime: startTime,
		EndTime:   endTime,
	}

	resp, err := s.client.QueryData(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("query data resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	rawDataSet := resp.GetQueryDataSet()
	ret := NewQueryDataSet(
		resp.GetPaths(),
		resp.GetDataTypeList(),
		rawDataSet.GetTimestamps(),
		rawDataSet.GetValuesList(),
		rawDataSet.GetBitmapList(),
	)
	return ret, nil
}

func (s *Session) DownSampleQuery(paths []string, startTime, endTime int64, aggregateType rpc.AggregateType, precision int64) (*QueryDataSet, error) {
	req := rpc.DownsampleQueryReq{
		SessionId:     s.sessionId,
		Paths:         s.mergeAndSortPaths(paths),
		StartTime:     startTime,
		EndTime:       endTime,
		AggregateType: aggregateType,
		Precision:     precision,
	}

	resp, err := s.client.DownsampleQuery(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("downsample query data resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	rawDataSet := resp.GetQueryDataSet()
	ret := NewQueryDataSet(
		resp.GetPaths(),
		resp.GetDataTypeList(),
		rawDataSet.GetTimestamps(),
		rawDataSet.GetValuesList(),
		rawDataSet.GetBitmapList(),
	)
	return ret, nil
}

func (s *Session) AggregateQuery(paths []string, startTime, endTime int64, aggregateType rpc.AggregateType) (*AggregateQueryDataSet, error) {
	req := rpc.AggregateQueryReq{
		SessionId:     s.sessionId,
		Paths:         s.mergeAndSortPaths(paths),
		StartTime:     startTime,
		EndTime:       endTime,
		AggregateType: aggregateType,
	}

	resp, err := s.client.AggregateQuery(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("query data resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	ret := NewAggregateQueryDataSet(
		resp.GetPaths(),
		resp.GetTimestamps(),
		resp.GetValuesList(),
		resp.GetDataTypeList(),
		aggregateType,
	)
	return ret, nil
}

func (s *Session) LastQuery(paths []string, startTime int64) (*QueryDataSet, error) {
	req := rpc.LastQueryReq{
		SessionId: s.sessionId,
		Paths:     s.mergeAndSortPaths(paths),
		StartTime: startTime,
	}

	resp, err := s.client.LastQuery(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("query data resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	rawDataSet := resp.GetQueryDataSet()
	ret := NewQueryDataSet(
		resp.GetPaths(),
		resp.GetDataTypeList(),
		rawDataSet.GetTimestamps(),
		rawDataSet.GetValuesList(),
		rawDataSet.GetBitmapList(),
	)
	return ret, nil
}

func (s *Session) ExecuteSQL(sql string) (*SQLDataSet, error) {
	req := rpc.ExecuteSqlReq{
		SessionId: s.sessionId,
		Statement: sql,
	}

	resp, err := s.client.ExecuteSql(context.Background(), &req)
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, errors.New("execute SQL resp is nil")
	}

	err = s.verifyStatus(resp.GetStatus())
	if err != nil {
		return nil, err
	}

	return NewSQLDataSet(resp), err
}

func (s *Session) verifyStatus(status *rpc.Status) error {
	if status.GetCode() != SuccessCode {
		return errors.New("error occurs: " + status.GetMessage())
	}
	return nil
}

func (s *Session) mergeAndSortPaths(paths []string) []string {
	for _, path := range paths {
		if path == "*" {
			return []string{"*"}
		}
	}

	var prefixes []string
	for _, path := range paths {
		index := strings.Index(path, "*")
		if index != -1 {
			prefixes = append(prefixes, path[0:index])
		}
	}
	if len(prefixes) == 0 {
		sort.Strings(paths)
		return paths
	}

	var mergedPaths []string
	for _, path := range paths {
		if !strings.Contains(path, "*") {
			skip := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(path, prefix) {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
		}
		mergedPaths = append(mergedPaths, path)
	}

	sort.Strings(mergedPaths)
	return mergedPaths
}
