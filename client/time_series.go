package client

import "github.com/thulab/iginx-client-go/rpc"

type TimeSeries struct {
	path     string
	dataType rpc.DataType
}

func NewTimeSeries(path string, dataType rpc.DataType) TimeSeries {
	return TimeSeries{
		path:     path,
		dataType: dataType,
	}
}

func (ts *TimeSeries) GetPath() string {
	return ts.path
}

func (ts *TimeSeries) GetType() rpc.DataType {
	return ts.dataType
}

func (ts *TimeSeries) ToString() string {
	return "Path: " + ts.path + ", Type: " + ts.dataType.String()
}
