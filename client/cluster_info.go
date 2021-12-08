package client

import (
	"fmt"
	"strconv"

	"github.com/thulab/iginx-client-go/rpc"
)

type IginxInfo struct {
	Id   int64
	Ip   string
	Port int32
}

type StorageEngineInfo struct {
	Id   int64
	Ip   string
	Port int32
	Type string
}

type MetaStorageInfo struct {
	Ip   string
	Port int32
	Type string
}

type LocalMetaStorageInfo struct {
	path string
}

type ClusterInfo struct {
	iginxInfo            []IginxInfo
	storageEngineInfo    []StorageEngineInfo
	metaStorageInfo      []MetaStorageInfo
	localMetaStorageInfo *LocalMetaStorageInfo
}

func NewClusterInfoWithResp(resp *rpc.GetClusterInfoResp) *ClusterInfo {
	return NewClusterInfo(
		resp.GetIginxInfos(),
		resp.GetStorageEngineInfos(),
		resp.GetMetaStorageInfos(),
		resp.GetLocalMetaStorageInfo(),
	)
}

func NewClusterInfo(
	iginxInfos []*rpc.IginxInfo,
	storageEngineInfos []*rpc.StorageEngineInfo,
	metaStorageInfos []*rpc.MetaStorageInfo,
	localMetaStorageInfo *rpc.LocalMetaStorageInfo,
) *ClusterInfo {

	var iginxInfoList []IginxInfo
	for _, info := range iginxInfos {
		iginxInfo := IginxInfo{
			Id:   info.ID,
			Ip:   info.IP,
			Port: info.Port,
		}
		iginxInfoList = append(iginxInfoList, iginxInfo)
	}

	var storageEngineInfoList []StorageEngineInfo
	for _, info := range storageEngineInfos {
		storageInfo := StorageEngineInfo{
			Id:   info.ID,
			Ip:   info.IP,
			Port: info.Port,
			Type: info.Type,
		}
		storageEngineInfoList = append(storageEngineInfoList, storageInfo)
	}

	var metaStorageInfoList []MetaStorageInfo
	for _, info := range metaStorageInfos {
		metaInfo := MetaStorageInfo{
			Ip:   info.IP,
			Port: info.Port,
			Type: info.Type,
		}
		metaStorageInfoList = append(metaStorageInfoList, metaInfo)
	}

	var localInfo *LocalMetaStorageInfo
	if localMetaStorageInfo != nil {
		localInfo = &LocalMetaStorageInfo{
			path: localMetaStorageInfo.Path,
		}
	}

	return &ClusterInfo{
		iginxInfo:            iginxInfoList,
		storageEngineInfo:    storageEngineInfoList,
		metaStorageInfo:      metaStorageInfoList,
		localMetaStorageInfo: localInfo,
	}
}

func (c *ClusterInfo) IsUseLocalMetaStorage() bool {
	return c.localMetaStorageInfo != nil
}

func (i *IginxInfo) ToString() string {
	return "Id: " + strconv.FormatInt(i.Id, 10) +
		", Ip: " + i.Ip +
		", Port: " + strconv.Itoa(int(i.Port)) +
		"\n"
}

func (s *StorageEngineInfo) ToString() string {
	return "Id: " + strconv.FormatInt(s.Id, 10) +
		", Ip: " + s.Ip +
		", Port: " + strconv.Itoa(int(s.Port)) +
		", Type: " + s.Type +
		"\n"
}

func (m *MetaStorageInfo) ToString() string {
	return "Ip: " + m.Ip +
		", Port: " + strconv.Itoa(int(m.Port)) +
		", Type: " + m.Type +
		"\n"
}

func (l *LocalMetaStorageInfo) ToString() string {
	return l.path + "\n"
}

func (c *ClusterInfo) PrintInfo() {
	fmt.Println(c.ToString())
}

func (c *ClusterInfo) ToString() string {
	var ret string
	ret += "Iginx info:\n"
	for _, info := range c.iginxInfo {
		ret += info.ToString()
	}
	ret += "Storage engine info:\n"
	for _, info := range c.storageEngineInfo {
		ret += info.ToString()
	}
	if c.IsUseLocalMetaStorage() {
		ret += "Local meta storage info:\n"
		ret += c.localMetaStorageInfo.ToString()
	} else {
		ret += "Meta storage info:\n"
		for _, info := range c.metaStorageInfo {
			ret += info.ToString()
		}
	}
	return ret
}
