package repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"mysqlbinlog/entity"
	"mysqlbinlog/utils"
)

type LevelDBStorage struct {
	f           string
	ldb         *leveldb.DB
	positionKey []byte
	schemaKey   []byte
}

func (l LevelDBStorage) GetPosition() (entity.Position, error) {
	pos := entity.Position{}

	res, err := l.ldb.Get(l.positionKey, nil)
	if err == leveldb.ErrNotFound || len(res) == 0 {
		return pos, nil
	}
	if err != nil {
		return pos, err
	}

	_ = json.Unmarshal(res, &pos)

	return pos, nil
}

func (l LevelDBStorage) SetPosition(position entity.Position) error {
	bs, _ := json.Marshal(position)
	if err := l.ldb.Put(l.positionKey, bs, nil); err != nil {
		log.Println("设置position失败", position, err)
		return err
	}
	if utils.GetConf().SyncType == 1 {
		return nil
	}
	fmt.Println(position.FileName)
	fmt.Println(position.FilePos)
	if utils.GetConf().Position.End.FileName == position.FileName &&
		utils.GetConf().Position.End.Pos <= position.FilePos {
		return errors.New("到达结束位置")
	}
	return nil
}

func (l LevelDBStorage) GetSchemas() (map[string][]string, error) {
	schemas := make(map[string][]string)

	val, err := l.ldb.Get(l.schemaKey, nil)
	if err == leveldb.ErrNotFound || len(val) == 0 {
		return schemas, nil
	}

	if err != nil {
		return schemas, err
	}

	_ = json.Unmarshal(val, &schemas)

	return schemas, nil
}

func (l LevelDBStorage) SetSchemas(position entity.Position, schemas map[string][]string) error {
	bs, _ := json.Marshal(schemas)
	if err := l.ldb.Put(l.schemaKey, bs, nil); err != nil {
		log.Println("设置schema失败", schemas, err)
		return err
	}
	return nil
}

func NewLevelDBStorage() *LevelDBStorage {
	ldb, err := leveldb.OpenFile(utils.LevelDBFile, nil)
	if err != nil {
		panic(err)
	}
	// _ = ldb.Close()
	return &LevelDBStorage{
		f:           utils.LevelDBFile,
		ldb:         ldb,
		positionKey: []byte("position"),
		schemaKey:   []byte("schema"),
	}
}
