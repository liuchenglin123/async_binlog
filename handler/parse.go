package handler

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"mysqlbinlog/utils"
)

func Parser(sql string) ([]ParserResult, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	utils.SqlFile.WriteString("alter " + sql + ";\n")

	res := make([]ParserResult, 0)
	for _, stmt := range stmtNodes {
		alter, ok := stmt.(*ast.AlterTableStmt)
		if ok {
			res = append(res, getAlterResult(alter)...)
			continue
		}

		create, ok := stmt.(*ast.CreateTableStmt)
		if ok {
			res = append(res, getCreateResult(create))
			continue
		}

		drop, ok := stmt.(*ast.DropTableStmt)
		if ok {
			res = append(res, getDropResult(drop))
			continue
		}

		rename, ok := stmt.(*ast.RenameTableStmt)
		if ok {
			res = append(res, getRenameResult(rename)...)
			continue
		}
	}

	return res, nil
}

// 解析sql的结果
type ParserResult struct {
	Action       ResultAction // alter 更改表， create 创建表， drop 删除表， rename 重命名， 空无操作
	AlterParams  AlterParams  // alter参数
	CreateParams CreateParams // create参数
	DropParams   DropParams   // drop参数
	RenameParams RenameParams // rename参数
}

type ResultAction string

const (
	RAAlter  ResultAction = "alter"
	RACreate ResultAction = "create"
	RADrop   ResultAction = "drop"
	RARename ResultAction = "rename"
)

// 更改表字段的位置
type ColumnPosition struct {
	TP             int // 0 none 默认队尾或者无修改， 1 first 队首，2 after 在RelativeColumn之后
	RelativeColumn string
}

// alter参数
type AlterParams struct {
	// add 新增字段，drop 删除字段，rename_table 修改表名，rename_column 修改字段名
	// change 可以改名，可以改顺序 modify 可以改顺序
	AlterAction AlterAction
	TableName   string

	AddColumnParams    AddColumnParams    // AlterTableAddColumns 在某个位置添加字段
	DropColumnParams   DropColumnParams   // AlterTableDropColumn 删除字段
	RenameTableParams  RenameTableParams  // AlterTableRenameTable 修改表名
	RenameColumnParams RenameColumnParams // AlterTableRenameColumn 修改字段名
	ChangeColumnParams ChangeColumnParams // AlterTableChangeColumn 修改字段名，修改字段顺序
	ModifyColumnParams ModifyColumnParams // AlterTableModifyColumn 修改字段顺序
}

type AlterAction string

const (
	AAAdd          AlterAction = "add"
	AADrop         AlterAction = "drop"
	AAChange       AlterAction = "change"
	AARenameTable  AlterAction = "rename_table"
	AARenameColumn AlterAction = "rename_column"
	AAModify       AlterAction = "modify"
)

// alter change
type ChangeColumnParams struct {
	OldColumnName string
	NewColumnName string
	Position      ColumnPosition
}

// alter modify
type ModifyColumnParams struct {
	Columns  []string
	Position ColumnPosition
}

// alter rename column
type RenameColumnParams struct {
	OldColumnName string
	NewColumnName string
}

// alter add
type AddColumnParams struct {
	Columns  []string
	Position ColumnPosition
}

// alter drop
type DropColumnParams struct {
	Column string
}

// alter rename table
type RenameTableParams struct {
	OldTableName string
	NewTableName string
}

// create参数
type CreateParams struct {
	TableName string
	Columns   []string
}

// drop参数
type DropParams struct {
	DropTableNames map[string][]string // 库名 ：[]表名
}

// rename参数
type RenameParams struct {
	OldTableName string
	NewTableName string
}

func getAlterResult(stmt *ast.AlterTableStmt) []ParserResult {
	result := make([]ParserResult, 0)
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			newCols := make([]string, 0)
			for _, col := range spec.NewColumns {
				newCols = append(newCols, col.Name.Name.String())
			}

			pos := ColumnPosition{
				TP:             int(spec.Position.Tp),
				RelativeColumn: "",
			}
			if spec.Position.Tp == ast.ColumnPositionAfter {
				pos.RelativeColumn = spec.Position.RelativeColumn.Name.String()
			}

			result = append(result, ParserResult{
				Action: RAAlter,
				AlterParams: AlterParams{
					AlterAction: AAAdd,
					TableName:   stmt.Table.Name.String(),
					AddColumnParams: AddColumnParams{
						Columns:  newCols,
						Position: pos,
					},
				},
			})
		case ast.AlterTableDropColumn:
			result = append(result, ParserResult{
				Action: RAAlter,
				AlterParams: AlterParams{
					AlterAction: AADrop,
					TableName:   stmt.Table.Name.String(),
					DropColumnParams: DropColumnParams{
						Column: spec.OldColumnName.String(),
					},
				},
			})
		case ast.AlterTableModifyColumn:
			pos := ColumnPosition{
				TP:             int(spec.Position.Tp),
				RelativeColumn: "",
			}
			if spec.Position.Tp == ast.ColumnPositionAfter {
				pos.RelativeColumn = spec.Position.RelativeColumn.Name.String()
			}

			columns := make([]string, 0)
			for _, col := range spec.NewColumns {
				columns = append(columns, col.Name.String())
			}

			result = append(result, ParserResult{
				Action: RAAlter,
				AlterParams: AlterParams{
					AlterAction: AAModify,
					TableName:   stmt.Table.Name.String(),
					ModifyColumnParams: ModifyColumnParams{
						Columns:  columns,
						Position: pos,
					},
				},
			})
		case ast.AlterTableChangeColumn:
			pos := ColumnPosition{
				TP:             int(spec.Position.Tp),
				RelativeColumn: "",
			}
			if spec.Position.Tp == ast.ColumnPositionAfter {
				pos.RelativeColumn = spec.Position.RelativeColumn.Name.String()
			}

			result = append(result, ParserResult{
				Action: RAAlter,
				AlterParams: AlterParams{
					AlterAction: AAChange,
					TableName:   stmt.Table.Name.String(),
					ChangeColumnParams: ChangeColumnParams{
						OldColumnName: spec.OldColumnName.String(),
						NewColumnName: spec.NewColumns[0].Name.Name.String(),
						Position:      pos,
					},
				},
			})
		case ast.AlterTableRenameColumn:
			result = append(result, ParserResult{
				Action: RAAlter,
				AlterParams: AlterParams{
					AlterAction: AARenameColumn,
					TableName:   stmt.Table.Name.String(),
					RenameColumnParams: RenameColumnParams{
						OldColumnName: spec.OldColumnName.String(),
						NewColumnName: spec.NewColumnName.String(),
					},
				},
			})
		case ast.AlterTableRenameTable:
			result = append(result, ParserResult{
				Action: RAAlter,
				AlterParams: AlterParams{
					AlterAction: AARenameTable,
					TableName:   stmt.Table.Name.String(),
					RenameTableParams: RenameTableParams{
						OldTableName: stmt.Table.Name.String(),
						NewTableName: spec.NewTable.Name.String(),
					},
				},
			})
		}
	}
	return result
}

func getCreateResult(stmt *ast.CreateTableStmt) ParserResult {
	columns := make([]string, 0)
	for _, col := range stmt.Cols {
		columns = append(columns, col.Name.String())
	}
	return ParserResult{
		Action: RACreate,
		CreateParams: CreateParams{
			TableName: stmt.Table.Name.String(),
			Columns:   columns,
		},
	}
}

func getDropResult(stmt *ast.DropTableStmt) ParserResult {
	tableNames := make(map[string][]string, 0)
	for _, table := range stmt.Tables {
		schema := table.Schema.String()
		name := table.Name.String()

		if _, ok := tableNames[schema]; !ok {
			tableNames[schema] = make([]string, 0)
		}

		tableNames[schema] = append(tableNames[schema], name)
	}
	return ParserResult{
		Action: RADrop,
		DropParams: DropParams{
			DropTableNames: tableNames,
		},
	}
}

func getRenameResult(stmt *ast.RenameTableStmt) []ParserResult {
	result := make([]ParserResult, 0)
	for _, table := range stmt.TableToTables {
		result = append(result, ParserResult{
			Action: RARename,
			RenameParams: RenameParams{
				OldTableName: table.OldTable.Name.String(),
				NewTableName: table.NewTable.Name.String(),
			},
		})
	}

	return result
}
