package mysql

import (
	"config.1kf.com/server/base_go/log"
	"database/sql"
	"reflect"
	"strings"
	"time"
)

type Tx struct {
	Tx       *sql.Tx
	hasError bool //有一些错误 - -
}

func (this *Tx) Close() {
	if this.hasError {
		err := this.Tx.Rollback()
		if err != nil {
			log.Error(err)
		}
	} else {
		err := this.Tx.Commit()
		if err != nil {
			log.Error(err)

			err := this.Tx.Rollback()
			if err != nil {
				log.Error(err)
			}
		}
	}
}

func (this *Tx) ErrorHappen() {
	this.hasError = true
}

func (tx *Tx) Insert(query string, args ...interface{}) (int64, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		log.Error(err)
		return -1, err
	}
	return res.LastInsertId()
}

func (tx *Tx) Update(query string, args ...interface{}) (int64, error) {
	stmt, err := tx.Tx.Prepare(query)
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

func (tx *Tx) QueryForMap(query string, args ...interface{}) (map[string]interface{}, error) {
	stmt, err := tx.Tx.Prepare(query)
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

func (tx *Tx) QueryForMapSlice(query string, args ...interface{}) ([]map[string]interface{}, error) {
	stmt, err := tx.Tx.Prepare(query)
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

func (tx *Tx) QueryForModel(model interface{}, query string, args ...interface{}) (bool, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
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
