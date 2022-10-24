package utils

import "os"

const (
	DefaultOutPutDir  = "output"
	DefaultOutPutName = "output.sql"
	DefaultConfigDir  = "./config/conf.json"
)

var (
	// SqlFile 文件
	SqlFile *os.File
	// LevelDBFile leveldb配置
	LevelDBFile = "binlog/leveldb"
)
