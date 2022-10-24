package repo

import (
	"fmt"
	"strings"

	"mysqlbinlog/entity"
)

const InsertSql = "INSERT INTO `%s`.`%s` (%s) VALUES %s;\n"

func getInsertTemplate(databaseName, tableName string, tableField []string, values string) string {
	return fmt.Sprintf(InsertSql, databaseName, tableName, strings.Join(tableField, ","), values)
}

const UpdateSql = "UPDATE  `%s`.`%s` SET  %s WHERE %s;\n"

func getUpdateTemplate(databaseName, tableName string, values string, whereParams string) string {
	return fmt.Sprintf(UpdateSql, databaseName, tableName, values, whereParams)
}

const DeleteSql = "DELETE FROM  `%s`.`%s` WHERE %s;\n"

func getDeleteTemplate(databaseName, tableName string, whereParams string) string {
	return fmt.Sprintf(DeleteSql, databaseName, tableName, whereParams)
}
func splitArray(arr []entity.RowValue, num int64) [][]entity.RowValue {
	max := int64(len(arr))
	// 判断数组大小是否小于等于指定分割大小的值，是则把原数组放入二维数组返回
	if max <= num {
		return [][]entity.RowValue{arr}
	}
	// 获取应该数组分割为多少份
	var quantity int64
	if max%num == 0 {
		quantity = max / num
	} else {
		quantity = (max / num) + 1
	}
	// 声明分割好的二维数组
	var segments = make([][]entity.RowValue, 0)
	// 声明分割数组的截止下标
	var start, end, i int64
	for i = 1; i <= quantity; i++ {
		end = i * num
		if i != quantity {
			segments = append(segments, arr[start:end])
		} else {
			segments = append(segments, arr[start:])
		}
		start = i * num
	}
	return segments
}

func getValue(val entity.RowValue) []string {
	vales := make([]string, len(val))
	var i int
	for k, v := range val {
		vales[i] = fmt.Sprintf(getValType(v), k, getVal(v))
		i++
	}
	return vales
}

func getValType(v interface{}) string {
	if v == nil {
		return "`%s`=%s"
	}
	if _, ok := v.(int); ok {
		return "`%s`=%d"
	}
	if _, ok := v.(int8); ok {
		return "`%s`=%d"
	}
	if _, ok := v.(int64); ok {
		return "`%s`=%d"
	}
	if _, ok := v.(int32); ok {
		return "`%s`=%d"
	}
	if _, ok := v.(uint32); ok {
		return "`%s`=%d"
	}
	if _, ok := v.(uint8); ok {
		return "`%s`=%d"
	}
	if _, ok := v.(string); ok {
		return "`%s`=\"%s\""
	}
	return "`%s`=\"%s\""
}

func getVal(v interface{}) interface{} {
	if v == nil {
		return "NULL"
	}
	return v
}
