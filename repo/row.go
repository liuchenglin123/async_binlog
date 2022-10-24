package repo

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"mysqlbinlog/entity"
	"mysqlbinlog/utils"
)

type Row struct {
}

func (r *Row) Insert(database string, table string, rows []entity.RowValue) {
	if len(rows) == 0 {
		return
	}
	marshal, _ := json.Marshal(rows)
	fields := make([]string, 0)
	sqlStr := string(marshal)
	// 获取第一个
	dataReg := regexp.MustCompile("(?msi:{\".*?\":)+")
	dataArr := dataReg.FindAllString(sqlStr, -1)
	fields = append(fields, dataArr...)
	// 获取剩余的
	dataReg2 := regexp.MustCompile("(?msi:,\".*?\":)+")
	dataArr2 := dataReg2.FindAllString(sqlStr, -1)
	fields = append(fields, dataArr2...)
	fieldMap := make(map[string]int)

	var i int
	for _, field := range fields {
		dataReg3 := regexp.MustCompile("(?msi:\".*?\")+")
		dataArr3 := dataReg3.FindAllString(field, -1)
		if _, ok := fieldMap[dataArr3[0]]; ok {
			continue
		}
		sqlStr = strings.Replace(sqlStr, fmt.Sprintf("%s:", dataArr3[0]), "", -1)
		fieldMap[dataArr3[0]] = i
		i++
	}
	sqlStr = strings.Replace(sqlStr, fmt.Sprintf("%s", "["), "", -1)
	sqlStr = strings.Replace(sqlStr, fmt.Sprintf("%s", "]"), "", -1)
	sqlStr = strings.Replace(sqlStr, fmt.Sprintf("%s", "{"), "(", -1)
	sqlStr = strings.Replace(sqlStr, fmt.Sprintf("%s", "}"), ")", -1)
	tableField := make([]string, len(fieldMap))
	for k, v := range fieldMap {
		tableField[v] = k
	}
	_, err := utils.SqlFile.WriteString(getInsertTemplate(database, table, tableField, sqlStr))
	if err != nil {
		log.Println(err)
	}
}

func (r *Row) Update(database string, table string, rows []entity.RowValue) {
	arrays := splitArray(rows, 2)
	fmt.Println(arrays)
	for _, arr := range arrays {
		_, err := utils.SqlFile.WriteString(getUpdateTemplate(database,
			table,
			strings.Join(getValue(arr[1]), ","),
			strings.Join(getValue(arr[0]), " AND ")))
		if err != nil {
			log.Println(err)
		}
	}

}

func (r *Row) Delete(database string, table string, rows []entity.RowValue) {
	for _, arr := range rows {
		_, err := utils.SqlFile.WriteString(getDeleteTemplate(database, table, strings.Join(getValue(arr), ",")))
		if err != nil {
			log.Println(err)
		}
	}
}

func NewRow() *Row {
	return &Row{}
}
