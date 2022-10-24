package boot

import (
	"fmt"
	"log"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"mysqlbinlog/entity"
	"mysqlbinlog/handler"
	"mysqlbinlog/interfaces"
	"mysqlbinlog/utils"
)

func Run(rowChange interfaces.IRowChange, over chan struct{}) {
	e := handler.NewEventHandler(rowChange)
	// 开启canal

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", utils.GetConf().Auth.Host, utils.GetConf().Auth.Port)
	cfg.User = utils.GetConf().Auth.User
	cfg.Password = utils.GetConf().Auth.Password
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		panic(err)
	}
	c.SetEventHandler(e)

	// 确定position，首先从shadow读取，没有就从mysql读取当前的位置
	// shadow用于保存当前读取的位置和表结构信息
	var position entity.Position
	position, err = e.Shadow.GetPosition()
	if err != nil {
		log.Print(err)
		panic(err)
	}
	if position.FileName == "" {
		mpos, err := c.GetMasterPos()
		if err != nil {
			panic(err)
		}

		if err := e.Shadow.SetPosition(entity.Position{FileName: mpos.Name, FilePos: mpos.Pos}); err != nil {
			panic(err)
		}
	}

	// 确定schema，首先从mysql读出最新的表结构信息，然后同步到shadow[shadow中不存在的表结构保存，存在的表结构不做处理]，
	rr, err := c.Execute("SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION")
	if err != nil {
		panic(err)
	}
	schemas := make(map[string][]string, 0)
	for _, row := range rr.Values {
		database := string(row[0].AsString())
		if database == "mysql" ||
			database == "sys" ||
			database == "information_schema" ||
			database == "performance_schema" {
			continue
		}
		table := string(row[1].AsString())
		column := string(row[2].AsString())
		key := handler.SchemaKey(database, table)
		if _, ok := schemas[key]; !ok {
			schemas[key] = make([]string, 0)
		}

		schemas[key] = append(schemas[key], column)
	}
	rr.Close()
	if err := e.Shadow.InitSchema(schemas, position); err != nil {
		panic(err)
	}

	go func() {
		log.Println("wait over")
		<-over
		c.Close()
	}()
	conf := utils.GetConf()
	if conf.SyncType == 2 {
		position.FileName = conf.Position.Start.FileName
		position.FilePos = conf.Position.Start.Pos
	}
	// 从读取的位置信息开始同步处理
	if err = c.RunFrom(mysql.Position{
		Name: position.FileName,
		Pos:  position.FilePos,
	}); err != nil {
		panic(err)
	}
}
