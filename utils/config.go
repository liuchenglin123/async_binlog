package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Server struct {
	Auth struct {
		Host     string `json:"host"`     // 数据库地址
		Port     int    `json:"port"`     // 数据库端口号
		User     string `json:"user"`     // 数据库root账号
		Password string `json:"password"` // 数据库密码
	} `json:"auth"`
	SyncType int `json:"sync_type"` // 1 全量同步 2 区间同步
	Position struct {
		Start struct {
			FileName string `json:"file_name"` // mysql-bin.00000X
			Pos      uint32 `json:"pos"`       // 0
		}
		End struct {
			FileName string `json:"file_name"` // mysql-bin.00000X
			Pos      uint32 `json:"pos"`       // 0
		}
	}
}

var env *Server

func InitConf() error {
	if _, err := os.Stat(DefaultConfigDir); err != nil {
		return err
	}
	file, err := ioutil.ReadFile(DefaultConfigDir)
	if err != nil {
		return err
	}

	env = &Server{}
	if err = json.Unmarshal(file, env); err != nil {
		return err
	}
	log.Println(env)
	if env.SyncType != 1 && env.SyncType != 2 {
		return errors.New("未定义的同步类型")
	}
	if env.SyncType == 2 {
		if env.Position.Start.FileName == "" || env.Position.End.FileName == "" {
			return errors.New("binlog文件名不能未空")
		}
		if env.Position.Start.FileName == env.Position.End.FileName {
			if env.Position.Start.Pos > env.Position.End.Pos {
				return errors.New("开始点位不能大于结束点位")
			}
		}
	}
	outputFileName := fmt.Sprintf("%s_%s", env.Auth.Host, "output.sql")
	if _, err = os.Stat(outputFileName); err != nil {
		SqlFile, _ = os.Create(outputFileName)
	} else {
		SqlFile, err = os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		_, err = SqlFile.Seek(0, 2)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetConf() Server {
	return *env
}
