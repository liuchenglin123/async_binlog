package handler

import (
	"log"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"mysqlbinlog/entity"
	"mysqlbinlog/interfaces"
	"mysqlbinlog/utils"
)

type EventHandler struct {
	rowChange interfaces.IRowChange
	Shadow    *Shadow
}

func NewEventHandler(rowChange interfaces.IRowChange) *EventHandler {
	return &EventHandler{
		rowChange: rowChange,
		Shadow:    NewShadow(),
	}
}

func (e *EventHandler) OnRotate(roateEvent *replication.RotateEvent) error {
	return nil
}

func (e *EventHandler) OnTableChanged(schema string, table string) error {
	return nil
}

func (e *EventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	_, err := utils.SqlFile.Write(queryEvent.Query)
	if err != nil {
		log.Println(err)
		return err
	}
	_, err = utils.SqlFile.WriteString(";\n")
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
	currentDataBase := string(queryEvent.Schema)
	results, err := Parser(string(queryEvent.Query))
	if err != nil {
		log.Print("parse err", string(queryEvent.Query), err)
		return nil
	}

	// 只需关注 create|drop|alter|rename
	for _, result := range results {
		currentKey := SchemaKey(currentDataBase, result.AlterParams.TableName)
		switch result.Action {
		case RAAlter:
			switch result.AlterParams.AlterAction {
			case AAAdd:
				p := result.AlterParams.AddColumnParams

				if err := e.changeColumns(currentKey, p.Position, p.Columns); err != nil {
					log.Println("alter add", err)
					return err
				}
			case AADrop:
				p := result.AlterParams.DropColumnParams

				if err := e.Shadow.DropColumn(currentKey, p.Column); err != nil {
					log.Println("alter drop", err)
					return err
				}
			case AAChange:
				p := result.AlterParams.ChangeColumnParams

				if err := e.Shadow.RenameColumn(currentKey, p.OldColumnName, p.NewColumnName); err != nil {
					log.Println("alter change rename", err)
					return err
				}

				if p.Position.TP != 0 {
					if err := e.changeColumns(currentKey, p.Position, []string{p.NewColumnName}); err != nil {
						log.Println("alter change column", err)
						return err
					}
				}
			case AAModify:
				p := result.AlterParams.ModifyColumnParams

				if p.Position.TP != 0 {
					if err := e.changeColumns(currentKey, p.Position, p.Columns); err != nil {
						log.Println("alter modify", err)
						return err
					}
				}
			case AARenameColumn:
				p := result.AlterParams.RenameColumnParams

				if err := e.Shadow.RenameColumn(currentKey, p.OldColumnName, p.NewColumnName); err != nil {
					log.Println("rename_column", err)
					return err
				}
			case AARenameTable:
				p := result.AlterParams.RenameTableParams

				oldKey := SchemaKey(currentDataBase, p.OldTableName)
				newKey := SchemaKey(currentDataBase, p.NewTableName)
				if err := e.Shadow.RenameKey(oldKey, newKey); err != nil {
					log.Println("rename_table", err)
					return err
				}
			}
		case RACreate:
			currentKey := SchemaKey(currentDataBase, result.CreateParams.TableName)
			if err := e.Shadow.AddKey(currentKey, result.CreateParams.Columns); err != nil {
				log.Println("create", err)
				return err
			}
		case RADrop:
			for database, tables := range result.DropParams.DropTableNames {
				if database == "" {
					database = currentDataBase
				}
				for _, table := range tables {
					if err := e.Shadow.DropKey(SchemaKey(database, table)); err != nil {
						log.Println("create", err)
						return err
					}
				}
			}
		case RARename:
			p := result.RenameParams

			oldKey := SchemaKey(currentDataBase, p.OldTableName)
			newKey := SchemaKey(currentDataBase, p.NewTableName)
			if err := e.Shadow.RenameKey(oldKey, newKey); err != nil {
				log.Println("rename", err)
				return err
			}
		}
	}

	return e.Shadow.SyncSchema(entity.Position{FileName: nextPos.Name, FilePos: nextPos.Pos})
}

func (e *EventHandler) changeColumns(currentKey string, p ColumnPosition, changeColumn []string) error {
	if p.TP == 0 {
		if err := e.Shadow.LastColumn(currentKey, changeColumn); err != nil {
			return err
		}
	}

	if p.TP == 1 {
		if err := e.Shadow.FirstColumn(currentKey, changeColumn); err != nil {
			return err
		}
	}

	if p.TP == 2 {
		if err := e.Shadow.AfterColumn(currentKey, p.RelativeColumn, changeColumn); err != nil {
			return err
		}
	}

	return nil
}

func (e *EventHandler) OnRow(c *canal.RowsEvent) error {
	key := SchemaKey(c.Table.Schema, c.Table.Name)
	columns, err := e.Shadow.GetColumns(key)
	if err != nil {
		log.Println(err)
		return err
	}
	if len(columns) == 0 {
		// 表不存在了，放弃处理
		log.Println("表不存在了，放弃处理")
		return nil
	}

	rows := make([]entity.RowValue, 0)
	for _, r := range c.Rows {
		row := make(entity.RowValue)
		// if len(columns) != len(r) {
		// 	log.Println(columns, len(columns))
		// 	log.Println(r, len(r))
		// 	return errors.New("长度不一致")
		// }
		for index, column := range columns {
			if index == len(r) {
				break
			}
			row[column] = r[index]
		}
		rows = append(rows, row)
	}

	// fmt.Println(e.Action) // update  insert delete
	switch c.Action {
	case "insert":
		e.rowChange.Insert(c.Table.Schema, c.Table.Name, rows)
	case "update":
		e.rowChange.Update(c.Table.Schema, c.Table.Name, rows)
	case "delete":
		e.rowChange.Delete(c.Table.Schema, c.Table.Name, rows)
	}

	return nil
}

func (e *EventHandler) OnXID(nextPos mysql.Position) error {
	return e.Shadow.SetPosition(entity.Position{FileName: nextPos.Name, FilePos: nextPos.Pos})
}

func (e *EventHandler) OnGTID(_ mysql.GTIDSet) error {
	return nil
}

func (e *EventHandler) OnPosSynced(pos mysql.Position, _ mysql.GTIDSet, force bool) error {
	if force {
		// 这里必须强制保存
		if err := e.Shadow.SetPosition(entity.Position{FileName: pos.Name, FilePos: pos.Pos}); err != nil {
			return err
		}
	}
	return nil
}

func (e *EventHandler) String() string {
	return "EventHandler"
}
