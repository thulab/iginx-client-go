package main

import (
	"fmt"
	"log"

	"github.com/thulab/iginx-client-go/client"
)

var session *client.Session

func main() {
	session = client.NewSession("127.0.0.1", "6888", "root", "root")

	if err := session.Open(); err != nil {
		log.Fatal(err)
	}

	defer session.Close()

	// 清空数据
	clearData()

	// 查询副本数量
	showReplicaNum()
	// 查询集群信息
	showClusterInfo()
	// 插入数据
	insertData()

	// 查询时间序列
	showTimeSeries()

	// 查询全部数据
	queryAllData()
	// 值过滤查询
	valueFilterQuery()
	// 降采样查询
	downSampleQuery()
	// 聚合查询
	aggregateQuery()
	// last 查询
	lastQuery()

	// 删除部分数据
	deleteData()
	// 查询全部数据
	queryAllData()
	// 删除时间序列
	deleteTimeSeries()
	// 查询时间序列
	showTimeSeries()

	// 清空数据
	clearData()
	// 查询时间序列
	showTimeSeries()
}

func showReplicaNum() {
	sql := "SHOW REPLICA NUMBER;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("replica num: %d\n\n", resp.GetReplicaNum())
}

func showClusterInfo() {
	fmt.Println("showClusterInfo")

	sql := "SHOW CLUSTER INFO;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	resp.GetClusterInfo().PrintInfo()
	fmt.Println()
}

func insertData() {
	fmt.Println("insertData")

	sql := "INSERT INTO test.go (TIMESTAMP, a, b, c, d, e, f) VALUES " +
		"(1, 'one', 1, 1, 1.1, 1.1, true), " +
		"(2, 'two', 2, 2, 2.1, 2.1, false), " +
		"(3, 'three', 3, 3, 3.1, 3.1, true), " +
		"(4, 'four', 4, 4, 4.1, 4.1, false), " +
		"(5, 'five', 5, 5, 5.1, 5.1, true), " +
		"(6, 'six', 6, 6, 6.1, 6.1, false), " +
		"(7, 'seven', 7, 7, 7.1, 7.1, true), " +
		"(8, 'eight', 8, 8, 8.1, 8.1, false), " +
		"(9, 'nine', 9, 9, 9.1, 9.1, true), " +
		"(10, 'ten', 10, 10, 10.1, 10.1, false), " +
		"(11, 'eleven', 11, 11, 11.1, 11.1, true), " +
		"(12, 'twelve', 12, 12, 12.1, 12.1, false), " +
		"(13, 'thirteen', 13, 13, 13.1, 13.1, true); "

	_, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func showTimeSeries() {
	fmt.Println("show time series:")

	sql := "SHOW TIME SERIES;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	for _, ts := range resp.GetTimeSeries() {
		fmt.Println(ts.ToString())
	}
	fmt.Println()
}

func queryAllData() {
	fmt.Println("query all data:")

	sql := "SELECT * FROM test.go;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	resp.GetQueryDataSet().PrintDataSet()
}

func valueFilterQuery() {
	fmt.Println("value filter query:")

	sql := "SELECT * FROM test.go WHERE b > 6 AND c < 9;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	resp.GetQueryDataSet().PrintDataSet()
}

func downSampleQuery() {
	fmt.Println("downSample query:")

	sql := "SELECT MAX(b), MAX(e) FROM test.go GROUP [0, 10) BY 5ms;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	resp.GetQueryDataSet().PrintDataSet()
}

func aggregateQuery() {
	fmt.Println("aggregate query:")

	sql := "SELECT MAX(a), MAX(b) FROM test.go WHERE TIME >= 0 AND TIME < 10;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	resp.GetQueryDataSet().PrintDataSet()
}

func lastQuery() {
	fmt.Println("last query:")

	sql := "SELECT LAST(a), LAST(b), LAST(c) FROM test.go WHERE TIME >= 5;"
	resp, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	resp.GetQueryDataSet().PrintDataSet()
}

func deleteData() {
	fmt.Println("deleteData")

	sql := "DELETE FROM test.go.* WHERE TIME >= 10 AND TIME < 15;"
	_, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func deleteTimeSeries() {
	fmt.Println("deleteTimeSeries")

	sql := "DELETE TIME SERIES test.go.f;"
	_, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func clearData() {
	fmt.Println("clearData")

	sql := "CLEAR DATA;"
	_, err := session.ExecuteSQL(sql)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}
