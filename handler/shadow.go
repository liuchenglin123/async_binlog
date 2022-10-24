package handler

import (
	"fmt"

	"mysqlbinlog/entity"
	"mysqlbinlog/interfaces"
	"mysqlbinlog/repo"
)

type Shadow struct {
	schemas map[string][]string

	storage interfaces.IShadowStorage
}

func NewShadow() *Shadow {
	s := &Shadow{
		schemas: make(map[string][]string),
	}
	s.storage = repo.NewLevelDBStorage()

	schemas, err := s.storage.GetSchemas()
	if err != nil {
		panic(err)
	}

	s.schemas = schemas

	return s
}

func (s *Shadow) GetColumns(key string) ([]string, error) {
	return s.schemas[key], nil
}

func (s *Shadow) AddKey(key string, columns []string) error {
	s.schemas[key] = columns
	return nil
}

func (s *Shadow) DropKey(key string) error {
	delete(s.schemas, key)
	return nil
}

func (s *Shadow) RenameKey(oldKey string, newKey string) error {
	columns, ok := s.schemas[oldKey]
	if ok {
		s.schemas[newKey] = columns
		delete(s.schemas, oldKey)
	}
	return nil
}

// DropColumn 删除字段
func (s *Shadow) DropColumn(key string, dropColumn string) error {
	newColumns := make([]string, 0)
	columns, ok := s.schemas[key]
	if ok {
		for _, column := range columns {
			if column == dropColumn {
				continue
			}

			newColumns = append(newColumns, column)
		}

		s.schemas[key] = newColumns
		fmt.Println(key, s.schemas[key])
	}
	return nil
}

// RenameColumn done
func (s *Shadow) RenameColumn(key string, oldColumn string, newColumn string) error {
	if oldColumn == newColumn {
		return nil
	}
	columns, ok := s.schemas[key]
	if ok {
		for k, column := range columns {
			if column == oldColumn {
				columns[k] = newColumn
			}
		}

		s.schemas[key] = columns
		fmt.Println(key, s.schemas[key])
	}
	return nil
}

// FirstColumn 将columns放置队首
func (s *Shadow) FirstColumn(key string, columns []string) error {
	oldColumns, ok := s.schemas[key]
	if ok {
		// 从oldColumns依次查找不存在于columns的元素放入tempColumns
		tempColumns := make([]string, 0)
		for _, v := range oldColumns {
			for _, vv := range columns {
				if v != vv {
					tempColumns = append(tempColumns, v)
				}
			}
		}

		// 将columns置于tempColumns队首，
		s.schemas[key] = append(columns, tempColumns...)
		fmt.Println(key, s.schemas[key])
	}
	return nil
}

// AfterColumn 将columns放置relativeColumn之后
func (s *Shadow) AfterColumn(key string, relativeColumn string, columns []string) error {
	oldColumns, ok := s.schemas[key]
	if ok {
		// 从oldColumns依次查找不存在于columns的元素放入tempColumns
		tempColumns := make([]string, 0)
		for _, v := range oldColumns {
			for _, vv := range columns {
				if v != vv {
					tempColumns = append(tempColumns, v)
				}
			}
		}

		// 从tempColumns找到relativeColumn，将columns放入newColumns
		newColumns := make([]string, 0)
		for _, v := range tempColumns {
			newColumns = append(newColumns, v)
			if v == relativeColumn {
				newColumns = append(newColumns, columns...)
			}
		}

		s.schemas[key] = newColumns
		fmt.Println(key, s.schemas[key])
	}
	return nil
}

// 将columns放置队尾
func (s *Shadow) LastColumn(key string, columns []string) error {
	oldColumns, ok := s.schemas[key]
	if ok {
		// 从oldColumns依次查找不存在于columns的元素放入tempColumns
		tempColumns := make([]string, 0)
		for _, v := range oldColumns {
			for _, vv := range columns {
				if v != vv {
					tempColumns = append(tempColumns, v)
				}
			}
		}

		// 将columns放入队尾
		s.schemas[key] = append(tempColumns, columns...)
		fmt.Println(key, s.schemas[key])
	}
	return nil
}

func (s *Shadow) InitSchema(schemas map[string][]string, position entity.Position) error {
	// 只保存不存在的表结构，不修改已有的表结构
	for k, v := range schemas {
		if _, ok := s.schemas[k]; !ok {
			s.schemas[k] = v
		}
	}
	return s.SyncSchema(position)
}

func (s *Shadow) SyncSchema(position entity.Position) error {
	return s.storage.SetSchemas(position, s.schemas)
}

func (s *Shadow) GetPosition() (entity.Position, error) {
	return s.storage.GetPosition()
}

func (s *Shadow) SetPosition(pos entity.Position) error {
	return s.storage.SetPosition(pos)
}

func SchemaKey(databaseName string, tableName string) string {
	return databaseName + "." + tableName
}
