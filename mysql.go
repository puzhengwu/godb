package mysql

import (
	"database/sql"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type Mysql struct {
	conn    *sql.DB
	connStr string
}

func NewMysql() *Mysql {
	mysql := &Mysql{}
	return mysql
}

func (this *Mysql) BeginTx() (*Tx, error) {
	tx, errTx := this.conn.Begin()
	if errTx != nil {
		tx.Rollback()
		return nil, errTx
	}
	return &Tx{Tx: tx, hasError: false}, nil
}

///**
//处理数据库错误，记录日志，回滚事务，并返回错误代码 系统异常
//baisu 2015-07-30
//*/
//func (this *Mysql) ErrorHandlerTx(tx *sql.Tx, err interface{}) error {
//	tx.Rollback()
//	log.Error(err)
//	return errors.New(Error(constant.ERR_DATABASE_FAULT))
//}
//
///**
//处理数据库错误，记录日志，并返回错误代码 系统异常
//baisu 2015-07-30
//*/
//func (this *Mysql) ErrorHandler(err interface{}) error {
//	log.Error(err)
//	return errors.New(Error(constant.ERR_DATABASE_FAULT))
//}

func (m *Mysql) Open(dbConn string, maxIdle int, maxConns int) error {
	m.connStr = dbConn
	conn, err := sql.Open("mysql", dbConn)
	if err != nil {
		return err
	}
	conn.SetMaxIdleConns(maxIdle)
	conn.SetMaxOpenConns(maxConns)
	m.conn = conn
	return nil
}

func (m *Mysql) OpenOne(dbConn string) error {
	m.connStr = dbConn
	conn, err := sql.Open("mysql", dbConn)
	if err != nil {
		return err
	}
	m.conn = conn
	return nil
}

func (m *Mysql) Close() error {
	err := m.conn.Close()
	return err
}

func (m *Mysql) GetConnection() *sql.DB {
	return m.conn
}

func (m *Mysql) Insert(query string, args ...interface{}) (int64, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		return -1, err
	}
	return res.LastInsertId()
}

func (m *Mysql) Delete(query string, args ...interface{}) (int64, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}

func (m *Mysql) InsertTx(tx *Tx, query string, args ...interface{}) (int64, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		tx.ErrorHappen()
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		tx.ErrorHappen()
		return -1, err
	}
	return res.LastInsertId()
}

func (m *Mysql) TranBatchExec(querys []string, args [][]interface{}) error {
	tx, err := m.conn.Begin()
	if err != nil {

		return err
	}
	for i, query := range querys {
		_, err = tx.Exec(query, args[i]...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

type BatchPack struct {
	Querys []string
	Args   [][]interface{}
}

func MakeBatchPack(pack *BatchPack, query string, args ...interface{}) *BatchPack {
	pack.Querys = append(pack.Querys, query)
	pack.Args = append(pack.Args, args)
	return pack
}

func (m *Mysql) Update(query string, args ...interface{}) (int64, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}

func (m *Mysql) UpdateTx(tx *Tx, query string, args ...interface{}) (int64, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		tx.ErrorHappen()
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		tx.ErrorHappen()
		return -1, err
	}
	return res.RowsAffected()
}

//存储过程查询，返回值为单行内容，目前项目不要使用
func (m *Mysql) ProcForMap(query string, args ...interface{}) (map[string]interface{}, error) {
	conn, err := sql.Open("mysql", m.connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			if value.Elem().Kind() == reflect.Slice {
				result[key] = string(value.Interface().([]byte))
			} else {
				result[key] = value.Interface()
			}
		}
		return result, nil
	}
	return nil, nil
}

//存储过程查询，返回值为多行内容，目前项目不要使用
func (m *Mysql) ProcForMapSlice(query string, args ...interface{}) ([]map[string]interface{}, error) {
	conn, err := sql.Open("mysql", m.connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			if value.Elem().Kind() == reflect.Slice {
				result[key] = string(value.Interface().([]byte))
			} else {
				result[key] = value.Interface()
			}
		}
		results = append(results, result)
	}
	return results, nil
}

func (m *Mysql) QueryForMap(query string, args ...interface{}) (map[string]interface{}, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			if value.Elem().Kind() == reflect.Slice {
				result[key] = string(value.Interface().([]byte))
			} else {
				result[key] = value.Interface()
			}
		}
		return result, nil
	}
	return nil, nil
}

func (m *Mysql) QueryForMapUint642Str(query string, args ...interface{}) (map[string]interface{}, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			ve := value.Elem()
			switch ve.Kind() {
			case reflect.Slice:
				result[key] = string(value.Interface().([]byte))
			case reflect.Uint64:
				result[key] = strconv.FormatUint(ve.Uint(), 10)
			default:
				result[key] = value.Interface()
			}
		}
		return result, nil
	}
	return nil, nil
}

func (m *Mysql) QueryForMapU642StrSlice(query string, args ...interface{}) ([]map[string]interface{}, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			ve := value.Elem()
			switch ve.Kind() {
			case reflect.Slice:
				result[key] = string(value.Interface().([]byte))
			case reflect.Uint64:
				result[key] = strconv.FormatUint(ve.Uint(), 10)
			default:
				result[key] = value.Interface()
			}
		}
		results = append(results, result)
	}
	return results, nil
}

func (m *Mysql) QueryForMapTx(tx *Tx, query string, args ...interface{}) (map[string]interface{}, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		tx.ErrorHappen()
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		tx.ErrorHappen()
		return nil, err
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		tx.ErrorHappen()
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			tx.ErrorHappen()
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			if value.Elem().Kind() == reflect.Slice {
				result[key] = string(value.Interface().([]byte))
			} else {
				result[key] = value.Interface()
			}
		}
		return result, nil
	}
	return nil, nil
}

func (m *Mysql) QueryForMapSlice(query string, args ...interface{}) ([]map[string]interface{}, error) {
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			if value.Elem().Kind() == reflect.Slice {
				result[key] = string(value.Interface().([]byte))
			} else {
				result[key] = value.Interface()
			}
		}
		results = append(results, result)
	}
	return results, nil
}

func (m *Mysql) QueryForMapSliceTx(tx *Tx, query string, args ...interface{}) ([]map[string]interface{}, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		tx.ErrorHappen()
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		tx.ErrorHappen()
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		tx.ErrorHappen()
		return nil, err
	}

	values := make([]interface{}, len(cols))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			tx.ErrorHappen()
			return nil, err
		}
		result := make(map[string]interface{}, len(cols))
		for ii, key := range cols {
			if scanArgs[ii] == nil {
				continue
			}
			value := reflect.Indirect(reflect.ValueOf(scanArgs[ii]))
			if value.Elem().Kind() == reflect.Slice {
				result[key] = string(value.Interface().([]byte))
			} else {
				result[key] = value.Interface()
			}
		}
		results = append(results, result)
	}
	return results, nil
}

func (m *Mysql) QueryForModelSlice(model interface{}, query string, args ...interface{}) error {
	rows, err := m.conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	sliceValue := reflect.Indirect(reflect.ValueOf(model))
	sliceElementType := sliceValue.Type().Elem()

	var isPtr bool
	if sliceElementType.Kind() == reflect.Ptr {
		isPtr = true
		sliceElementType = sliceElementType.Elem()
	}

	colsNum := len(cols)
	scanArgs := make([]interface{}, colsNum)

	var fieldToStructIndex = make(map[string]int)

	for i, c := range cols {
		for n := 0; n < sliceElementType.NumField(); n++ {
			field := sliceElementType.Field(n).Tag.Get("field")
			if field == "" {
				if strings.ToLower(c) == strings.ToLower(sliceElementType.Field(n).Name) {
					fieldToStructIndex[c] = n
					break
				}
			} else {
				if strings.ToLower(c) == strings.ToLower(field) {
					fieldToStructIndex[c] = n
					break
				}
			}
		}
		var arg interface{}
		scanArgs[i] = &arg
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		resultPtr := reflect.New(sliceElementType)
		result := reflect.Indirect(resultPtr)

		for ii, key := range cols {
			arg := scanArgs[ii]
			if arg == nil {
				continue
			}
			value := reflect.ValueOf(arg).Elem().Interface()

			if index, ok := fieldToStructIndex[key]; ok {
				field := result.Field(index)
				switch field.Type().Kind() {
				case reflect.Bool:
					if v, ok := value.(bool); ok {
						field.SetBool(v)
					} else {
						v, _ := StrTo(ToStr(value)).Bool()
						field.SetBool(v)
					}
				case reflect.String:
					field.SetString(ToStr(value))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					val := reflect.ValueOf(value)
					switch val.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						field.SetInt(val.Int())
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						field.SetInt(int64(val.Uint()))
					default:
						v, _ := StrTo(ToStr(value)).Int64()
						field.SetInt(v)
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					val := reflect.ValueOf(value)
					switch val.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						field.SetUint(uint64(val.Int()))
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						field.SetUint(val.Uint())
					default:
						v, _ := StrTo(ToStr(value)).Uint64()
						field.SetUint(v)
					}
				case reflect.Float64, reflect.Float32:
					val := reflect.ValueOf(value)
					switch val.Kind() {
					case reflect.Float64:
						field.SetFloat(val.Float())
					default:
						v, _ := StrTo(ToStr(value)).Float64()
						field.SetFloat(v)
					}
				case reflect.Struct:
					var str string
					switch d := value.(type) {
					case time.Time:
						d = d.In(time.Local)
						field.Set(reflect.ValueOf(d))
					case []byte:
						str = string(d)
					case string:
						str = d
					}
					if str != "" {
						if len(str) >= 19 {
							str = str[:19]
							t, err := time.ParseInLocation(format_DateTime, str, time.Local)
							if err == nil {
								t = t.In(DefaultTimeLoc)
								field.Set(reflect.ValueOf(t))
							}
						} else if len(str) >= 10 {
							str = str[:10]
							t, err := time.ParseInLocation(format_Date, str, DefaultTimeLoc)
							if err == nil {
								field.Set(reflect.ValueOf(t))
							}
						}
					}
				}
			}
		}

		if isPtr {
			sliceValue.Set(reflect.Append(sliceValue, resultPtr))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, result))
		}
	}
	return nil
}

func (m *Mysql) QueryForModel(model interface{}, query string, args ...interface{}) (bool, error) {
	rows, err := m.conn.Query(query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}

	modelValue := reflect.Indirect(reflect.ValueOf(model))
	modelType := modelValue.Type()

	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	colsNum := len(cols)
	scanArgs := make([]interface{}, colsNum)

	var fieldToStructIndex = make(map[string]int)

	for i, c := range cols {
		for n := 0; n < modelType.NumField(); n++ {
			field := modelType.Field(n).Tag.Get("field")
			if field == "" {
				if strings.ToLower(c) == strings.ToLower(modelType.Field(n).Name) {
					fieldToStructIndex[c] = n
					break
				}
			} else {
				if strings.ToLower(c) == strings.ToLower(field) {
					fieldToStructIndex[c] = n
					break
				}
			}
		}
		var arg interface{}
		scanArgs[i] = &arg
	}

	var b bool

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return b, err
		}

		for ii, key := range cols {
			arg := scanArgs[ii]
			if arg == nil {
				continue
			}
			value := reflect.ValueOf(arg).Elem().Interface()

			if index, ok := fieldToStructIndex[key]; ok {
				field := modelValue.Field(index)
				switch field.Type().Kind() {
				case reflect.Bool:
					if v, ok := value.(bool); ok {
						field.SetBool(v)
					} else {
						v, _ := StrTo(ToStr(value)).Bool()
						field.SetBool(v)
					}
				case reflect.String:
					field.SetString(ToStr(value))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					val := reflect.ValueOf(value)
					switch val.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						field.SetInt(val.Int())
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						field.SetInt(int64(val.Uint()))
					default:
						v, _ := StrTo(ToStr(value)).Int64()
						field.SetInt(v)
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					val := reflect.ValueOf(value)
					switch val.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						field.SetUint(uint64(val.Int()))
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						field.SetUint(val.Uint())
					default:
						v, _ := StrTo(ToStr(value)).Uint64()
						field.SetUint(v)
					}
				case reflect.Float64, reflect.Float32:
					val := reflect.ValueOf(value)
					switch val.Kind() {
					case reflect.Float64:
						field.SetFloat(val.Float())
					default:
						v, _ := StrTo(ToStr(value)).Float64()
						field.SetFloat(v)
					}
				case reflect.Struct:
					var str string
					switch d := value.(type) {
					case time.Time:
						d = d.In(time.Local)
						field.Set(reflect.ValueOf(d))
					case []byte:
						str = string(d)
					case string:
						str = d
					}
					if str != "" {
						if len(str) >= 19 {
							str = str[:19]
							t, err := time.ParseInLocation(format_DateTime, str, time.Local)
							if err == nil {
								t = t.In(DefaultTimeLoc)
								field.Set(reflect.ValueOf(t))
							}
						} else if len(str) >= 10 {
							str = str[:10]
							t, err := time.ParseInLocation(format_Date, str, DefaultTimeLoc)
							if err == nil {
								field.Set(reflect.ValueOf(t))
							}
						}
					}
				}
			}
		}
		b = true
	}
	return b, nil
}
