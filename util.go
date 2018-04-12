package mysql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

const (
	COMMAND_LEN = 2
	HEADER_LEN  = 2
)

const (
	format_Date     = "2006-01-02"
	format_DateTime = "2006-01-02 15:04:05"
)

func EmptyArray() []map[string]interface{} {
	return make([]map[string]interface{}, 0)
}

func EmptyMap() map[string]interface{} {
	return make(map[string]interface{})
}

var DefaultTimeLoc = time.Local

func ReadMessage(conn *net.TCPConn) (uint16, []byte, error) {
	//读取命令ID头
	cmdBuf := make([]byte, COMMAND_LEN)
	n, err := io.ReadFull(conn, cmdBuf)
	if n == 0 && err == io.EOF {
		return 0, nil, errors.New("connection closed")
	} else if err != nil {
		return 0, nil, err
	}
	cmdId := binary.LittleEndian.Uint16(cmdBuf)
	//读取内容长度头
	headerBuf := make([]byte, HEADER_LEN)
	n, err = io.ReadFull(conn, headerBuf)
	if n == 0 && err == io.EOF {
		return 0, nil, errors.New("connection closed")
	} else if err != nil {
		return 0, nil, err
	}
	size := binary.LittleEndian.Uint16(headerBuf)
	//读取内容
	if size > 0 {
		data := make([]byte, size)
		n, err = io.ReadFull(conn, data)

		if n == 0 && err == io.EOF {
			return 0, nil, errors.New("connection closed")
		} else if err != nil {
			return 0, nil, err
		} else {
			return cmdId, data, nil
		}
	} else {
		return cmdId, nil, nil
	}
}

func GetTableNumber(id string, n uint32) uint32 {
	h := crc32.ChecksumIEEE([]byte(id)) >> 16 & 0xffff
	return h % n
}

func GetUUID() string {
	return strings.Replace(uuid.NewUUID().String(), "-", "", 0)
}

func GetUUIDShort() string {
	return strings.Replace(uuid.NewUUID().String(), "-", "", -1)
}

type StrTo string

func (f *StrTo) Set(v string) {
	if v != "" {
		*f = StrTo(v)
	} else {
		f.Clear()
	}
}

func (f *StrTo) Clear() {
	*f = StrTo(0x1E)
}

func (f StrTo) Exist() bool {
	return string(f) != string(0x1E)
}

func (f StrTo) Bool() (bool, error) {
	return strconv.ParseBool(f.String())
}

func (f StrTo) Float32() (float32, error) {
	v, err := strconv.ParseFloat(f.String(), 32)
	return float32(v), err
}

func (f StrTo) Float64() (float64, error) {
	return strconv.ParseFloat(f.String(), 64)
}

func (f StrTo) Int() (int, error) {
	v, err := strconv.ParseInt(f.String(), 10, 32)
	return int(v), err
}

func (f StrTo) Int8() (int8, error) {
	v, err := strconv.ParseInt(f.String(), 10, 8)
	return int8(v), err
}

func (f StrTo) Int16() (int16, error) {
	v, err := strconv.ParseInt(f.String(), 10, 16)
	return int16(v), err
}

func (f StrTo) Int32() (int32, error) {
	v, err := strconv.ParseInt(f.String(), 10, 32)
	return int32(v), err
}

func (f StrTo) Int64() (int64, error) {
	v, err := strconv.ParseInt(f.String(), 10, 64)
	return int64(v), err
}

func (f StrTo) Uint() (uint, error) {
	v, err := strconv.ParseUint(f.String(), 10, 32)
	return uint(v), err
}

func (f StrTo) Uint8() (uint8, error) {
	v, err := strconv.ParseUint(f.String(), 10, 8)
	return uint8(v), err
}

func (f StrTo) Uint16() (uint16, error) {
	v, err := strconv.ParseUint(f.String(), 10, 16)
	return uint16(v), err
}

func (f StrTo) Uint32() (uint32, error) {
	v, err := strconv.ParseUint(f.String(), 10, 32)
	return uint32(v), err
}

func (f StrTo) Uint64() (uint64, error) {
	v, err := strconv.ParseUint(f.String(), 10, 64)
	return uint64(v), err
}

func (f StrTo) String() string {
	if f.Exist() {
		return string(f)
	}
	return ""
}

func ToStr(value interface{}, args ...int) (s string) {
	switch v := value.(type) {
	case bool:
		s = strconv.FormatBool(v)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 32))
	case float64:
		s = strconv.FormatFloat(v, 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 64))
	case int:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int8:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int16:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int32:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int64:
		s = strconv.FormatInt(v, argInt(args).Get(0, 10))
	case uint:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint8:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint16:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint32:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint64:
		s = strconv.FormatUint(v, argInt(args).Get(0, 10))
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}
func ToInt64(value interface{}) (d int64) {
	val := reflect.ValueOf(value)
	switch value.(type) {
	case int, int8, int16, int32, int64:
		d = val.Int()
	case uint, uint8, uint16, uint32, uint64:
		d = int64(val.Uint())
	default:
		panic(fmt.Errorf("ToInt64 need numeric not `%T`", value))
	}
	return
}

func snakeString(s string) string {
	data := make([]byte, 0, len(s)*2)
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			data = append(data, '_')
		}
		if d != '_' {
			j = true
		}
		data = append(data, d)
	}
	return strings.ToLower(string(data[:len(data)]))
}

func camelString(s string) string {
	data := make([]byte, 0, len(s))
	j := false
	k := false
	num := len(s) - 1
	for i := 0; i <= num; i++ {
		d := s[i]
		if k == false && d >= 'A' && d <= 'Z' {
			k = true
		}
		if d >= 'a' && d <= 'z' && (j || k == false) {
			d = d - 32
			j = false
			k = true
		}
		if k && d == '_' && num > i && s[i+1] >= 'a' && s[i+1] <= 'z' {
			j = true
			continue
		}
		data = append(data, d)
	}
	return string(data[:len(data)])
}

type argString []string

func (a argString) Get(i int, args ...string) (r string) {
	if i >= 0 && i < len(a) {
		r = a[i]
	} else if len(args) > 0 {
		r = args[0]
	}
	return
}

type argInt []int

func (a argInt) Get(i int, args ...int) (r int) {
	if i >= 0 && i < len(a) {
		r = a[i]
	}
	if len(args) > 0 {
		r = args[0]
	}
	return
}

type argAny []interface{}

func (a argAny) Get(i int, args ...interface{}) (r interface{}) {
	if i >= 0 && i < len(a) {
		r = a[i]
	}
	if len(args) > 0 {
		r = args[0]
	}
	return
}

func timeParse(dateString, format string) (time.Time, error) {
	tp, err := time.ParseInLocation(format, dateString, DefaultTimeLoc)
	return tp, err
}

func timeFormat(t time.Time, format string) string {
	return t.Format(format)
}

func indirectType(v reflect.Type) reflect.Type {
	switch v.Kind() {
	case reflect.Ptr:
		return indirectType(v.Elem())
	default:
		return v
	}
	return v
}

func FindIpByPrefix(prefix string) (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ip := addr.String()
		if strings.Index(ip, prefix) >= 0 {
			if index := strings.Index(ip, "/"); index > 0 {
				return ip[0:index], nil
			} else {
				return ip, nil
			}
		}
	}
	return "", errors.New("not find")
}

func StrSplitInts(str string, sep string) []int {
	pos := 0
	var result []int
	length := len(str)

	for pos < length {
		index := strings.Index(str[pos:], ",")
		if index >= 0 {
			i, err := strconv.Atoi(str[pos : pos+index])
			if err != nil {
				continue
			}
			result = append(result, i)
			pos += index + 1
		} else {
			i, err := strconv.Atoi(str[pos:])
			if err != nil {
				continue
			}
			result = append(result, i)
			pos = length
		}
	}
	return result
}

func StrSplitStrs(str string, sep string) []string {
	pos := 0
	var result []string
	length := len(str)

	for pos < length {
		index := strings.Index(str[pos:], ",")
		if index >= 0 {
			result = append(result, str[pos:pos+index])
			pos += index + 1
		} else {
			result = append(result, str[pos:])
			pos = length
		}
	}
	return result
}

/*func GetUniqueId(id uint32, databaseIndex uint8) uint64 {
	return uint64(id)*1000 + uint64(databaseIndex)
}*/
