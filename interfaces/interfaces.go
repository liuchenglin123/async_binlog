package interfaces

import "mysqlbinlog/entity"

// IShadowStorage 保存位置和表结构实现的接口
type IShadowStorage interface {
	GetPosition() (entity.Position, error)
	SetPosition(entity.Position) error
	GetSchemas() (map[string][]string, error)
	SetSchemas(position entity.Position, schemas map[string][]string) error
}

// IRowChange 消费者实现的接口
type IRowChange interface {
	Insert(database string, table string, rows []entity.RowValue)
	Update(database string, table string, rows []entity.RowValue) // 0,2,4...为变更前数据，1,3,5...为变更后数据
	Delete(database string, table string, rows []entity.RowValue)
}
