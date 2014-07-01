package pghelper

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

const (
	TypeString = iota
	TypeBool
	TypeInt64
	TypeFloat64
	TypeTime
	TypeBytea
	TypeStringSlice
	TypeBoolSlice
	TypeInt64Slice
	TypeFloat64Slice
	TypeTimeSlice
	TypeJSON
	TypeJSONSlice
)

var (
	regVarchar      = regexp.MustCompile(`^character varying\((\d+)\)$`)
	regVarcharArray = regexp.MustCompile(`^character varying\((\d+)\)\[\]$`)
)

type PGTypeType int
type PGType struct {
	Type    PGTypeType
	MaxSize int
	NotNull bool
}

func NewPGType(t PGTypeType, maxsize int, notnull bool) *PGType {
	return &PGType{t, maxsize, notnull}
}
func (p *PGType) Clone() *PGType {
	rev := PGType{}
	rev = *p
	return &rev
}

func (p *PGType) DBString() string {
	notnull := ""
	if p.NotNull {
		notnull = " NOT NULL"
	}
	switch p.Type {
	case TypeBool:
		return "boolean" + notnull
	case TypeBoolSlice:
		return "boolean[]" + notnull
	case TypeBytea:
		return "bytea" + notnull
	case TypeFloat64:
		return "double precision" + notnull
	case TypeFloat64Slice:
		return "double precision[]" + notnull
	case TypeInt64:
		return "bigint" + notnull
	case TypeInt64Slice:
		return "bigint[]" + notnull
	case TypeString:
		if p.MaxSize == 0 {
			return "text" + notnull
		} else {
			return fmt.Sprintf("character varying(%v)", p.MaxSize) + notnull
		}
	case TypeStringSlice:
		if p.MaxSize == 0 {
			return "text[]" + notnull
		} else {
			return fmt.Sprintf("character varying(%v)[]", p.MaxSize) + notnull
		}
	case TypeTime:
		return "timestamp without time zone" + notnull
	case TypeTimeSlice:
		return "timestamp without time zone[]" + notnull
	case TypeJSON:
		return "jsonb" + notnull
	case TypeJSONSlice:
		return "jsonb[]" + notnull
	default:
		panic(ERROR_DataTypeInvalid(p))

	}
}
func (p *PGType) ReflectType() reflect.Type {
	if !p.NotNull {
		switch p.Type {
		case TypeBool:
			return reflect.TypeOf(NullBool{})
		case TypeBoolSlice:
			return reflect.TypeOf(NullBoolSlice{})
		case TypeBytea:
			return reflect.TypeOf(NullBytea{})
		case TypeFloat64:
			return reflect.TypeOf(NullFloat64{})
		case TypeFloat64Slice:
			return reflect.TypeOf(NullFloat64Slice{})
		case TypeInt64:
			return reflect.TypeOf(NullInt64{})
		case TypeInt64Slice:
			return reflect.TypeOf(NullInt64Slice{})
		case TypeString:
			return reflect.TypeOf(NullString{})
		case TypeStringSlice:
			return reflect.TypeOf(NullStringSlice{})
		case TypeTime:
			return reflect.TypeOf(NullTime{})
		case TypeTimeSlice:
			return reflect.TypeOf(NullTimeSlice{})
		case TypeJSON:
			return reflect.TypeOf(NullJSON{})
		case TypeJSONSlice:
			return reflect.TypeOf(NullJSONSlice{})
		default:
			panic(ERROR_DataTypeInvalid(p))

		}

	} else {
		switch p.Type {
		case TypeBool:
			return reflect.TypeOf(true)
		case TypeBoolSlice:
			return reflect.TypeOf(BoolSlice{})
		case TypeBytea:
			return reflect.TypeOf(Bytea{})
		case TypeFloat64:
			return reflect.TypeOf(float64(0))
		case TypeFloat64Slice:
			return reflect.TypeOf(Float64Slice{})
		case TypeInt64:
			return reflect.TypeOf(int64(0))
		case TypeInt64Slice:
			return reflect.TypeOf(Int64Slice{})
		case TypeString:
			return reflect.TypeOf("")
		case TypeStringSlice:
			return reflect.TypeOf(StringSlice{})
		case TypeTime:
			return reflect.TypeOf(time.Time{})
		case TypeTimeSlice:
			return reflect.TypeOf(TimeSlice{})
		case TypeJSON:
			return reflect.TypeOf(JSON{})
		case TypeJSONSlice:
			return reflect.TypeOf(JSONSlice{})
		default:
			panic(ERROR_DataTypeInvalid(p))

		}
	}
}

func (p *PGType) SetReflectType(value interface{}) error {
	switch value.(type) {
	case string:
		p.Type = TypeString
	case int64:
		p.Type = TypeInt64
	case bool:
		p.Type = TypeBool
	case float64:
		p.Type = TypeFloat64
	case time.Time:
		p.Type = TypeTime
	case []byte:
		p.Type = TypeBytea
	default:
		return ERROR_DataTypeInvalid(value)
	}
	return nil
}
func (p *PGType) SetDBType(t string) error {
	switch {
	case t == "text":
		p.Type = TypeString
		p.MaxSize = 0
	case t == "text[]":
		p.Type = TypeStringSlice
		p.MaxSize = 0
	case t == "boolean":
		p.Type = TypeBool
	case t == "boolean[]":
		p.Type = TypeBoolSlice
	case t == "bigint":
		p.Type = TypeInt64
	case t == "bigint[]":
		p.Type = TypeInt64Slice
	case t == "double precision":
		p.Type = TypeFloat64
	case t == "double precision[]":
		p.Type = TypeFloat64Slice
	case regVarchar.MatchString(t):
		p.Type = TypeString
		var err error

		if p.MaxSize, err = strconv.Atoi(regVarchar.FindStringSubmatch(t)[1]); err != nil {
			return err
		}
	case regVarcharArray.MatchString(t):
		p.Type = TypeStringSlice
		var err error
		if p.MaxSize, err = strconv.Atoi(regVarcharArray.FindStringSubmatch(t)[1]); err != nil {
			return err
		}
	case t == "timestamp without time zone" ||
		t == "timestamp with time zone" ||
		t == "date":
		p.Type = TypeTime
	case t == "timestamp without time zone[]" ||
		t == "timestamp with time zone[]" ||
		t == "date[]":
		p.Type = TypeTimeSlice
	case t == "bytea":
		p.Type = TypeBytea
	case t == "jsonb" || t == "json":
		p.Type = TypeJSON
	case t == "jsonb[]" || t == "json[]":
		p.Type = TypeJSONSlice
	default:
		return ERROR_DataTypeInvalid(t)
	}
	return nil
}
func (dataType *PGType) EncodeString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	if !dataType.NotNull {
		val, err := value.(driver.Valuer).Value()
		if err != nil {
			return "", err
		}
		if val == nil {
			return "", nil
		}
		value = val
	}
	switch dataType.Type {
	case TypeString:
		if value.(string) == "" {
			return "", nil
		}
		return value.(string), nil
	case TypeBool:
		if value.(bool) {
			return "t", nil
		} else {
			return "f", nil
		}

	case TypeInt64:
		return fmt.Sprint(value.(int64)), nil
	case TypeFloat64:
		return fmt.Sprintf("%.17f", value.(float64)), nil
	case TypeTime:
		return value.(time.Time).Format(time.RFC3339Nano), nil
	case TypeBytea:
		if len(value.(Bytea)) == 0 {
			return "", nil
		}
		return fmt.Sprintf("\\x%x", value), nil
	case TypeStringSlice:
		if len(value.(StringSlice)) == 0 {
			return "", nil
		}
		return encodePGArray(value.(StringSlice)), nil

	case TypeBoolSlice:
		if len(value.(BoolSlice)) == 0 {
			return "", nil
		}
		tmpv := make([]string, len(value.(BoolSlice)))
		for i, v := range value.(BoolSlice) {
			if v {
				tmpv[i] = "t"
			} else {
				tmpv[i] = "f"
			}
		}
		return encodePGArray(tmpv), nil
	case TypeInt64Slice:
		if len(value.(Int64Slice)) == 0 {
			return "", nil
		}
		tmpv := make([]string, len(value.(Int64Slice)))
		for i, v := range value.(Int64Slice) {
			tmpv[i] = fmt.Sprint(v)
		}
		return encodePGArray(tmpv), nil
	case TypeFloat64Slice:
		if len(value.(Float64Slice)) == 0 {
			return "", nil
		}
		tmpv := make([]string, len(value.(Float64Slice)))
		for i, v := range value.(Float64Slice) {
			tmpv[i] = fmt.Sprint(v)
		}
		return encodePGArray(tmpv), nil

	case TypeTimeSlice:
		if len(value.(TimeSlice)) == 0 {
			return "", nil
		}
		tmpv := make([]string, len(value.(TimeSlice)))
		for i, v := range value.(TimeSlice) {
			tmpv[i] = v.Format(time.RFC3339Nano)
		}
		return encodePGArray(tmpv), nil
	case TypeJSON:
		if len(value.(JSON)) == 0 {
			return "", nil
		}
		buf, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	case TypeJSONSlice:
		if len(value.(JSONSlice)) == 0 {
			return "", nil
		}
		rev := make([]string, len(value.(JSONSlice)))
		for i, v := range value.(JSONSlice) {
			bys, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			rev[i] = string(bys)
		}
		return encodePGArray(rev), nil
	default:
		return "", fmt.Errorf("invalid type %T", dataType)
	}

}
func (dataType *PGType) DecodeString(value string) (result interface{}, result_err error) {
	if value == "" {
		return nil, nil
	}
	switch dataType.Type {
	case TypeString:
		result = value
	case TypeBool:
		if string(value) == "t" {
			result = true
		} else {
			result = false
		}
	case TypeInt64:
		result, result_err = strconv.ParseInt(string(value), 10, 64)
	case TypeFloat64:
		result, result_err = strconv.ParseFloat(string(value), 64)
	case TypeTime:
		result, result_err = time.Parse(time.RFC3339Nano, string(value))
	case TypeBytea:
		if len(value) >= 2 && bytes.Equal([]byte(value)[:2], []byte("\\x")) {
			// bytea_output = hex
			s := []byte(value)[2:] // trim off leading "\\x"
			rev := make(Bytea, hex.DecodedLen(len(s)))
			_, result_err := hex.Decode(rev, s)
			if result_err == nil {
				result = rev
			}
		} else {
			result_err = fmt.Errorf("%s is invalid hex string", value)
		}
	case TypeStringSlice:
		result = parsePGArray(value)
	case TypeBoolSlice:
		tmp := parsePGArray(value)
		rev := make(BoolSlice, len(tmp))
		for i, tv := range tmp {
			if tv == "t" {
				rev[i] = true
			} else {
				rev[i] = false
			}
		}
		result = rev
	case TypeInt64Slice:
		tmp := parsePGArray(value)
		rev := make(Int64Slice, len(tmp))
		for i, tv := range tmp {
			ti, err := strconv.ParseInt(tv, 10, 64)
			if err != nil {
				result_err = err
				break
			}
			rev[i] = ti
		}
		if result_err == nil {
			result = rev
		}
	case TypeFloat64Slice:
		tmp := parsePGArray(value)
		rev := make(Float64Slice, len(tmp))
		for i, tv := range tmp {
			ti, err := strconv.ParseFloat(tv, 64)
			if err != nil {
				result_err = err
				break
			}
			rev[i] = ti
		}
		if result_err == nil {
			result = rev
		}
	case TypeTimeSlice:
		tmp := parsePGArray(value)
		rev := make(TimeSlice, len(tmp))
		for i, tv := range tmp {
			ti, err := time.Parse(time.RFC3339Nano, tv)
			if err != nil {
				result_err = err
				break
			}
			rev[i] = ti
		}
		if result_err == nil {
			result = rev
		}
	case TypeJSON:
		rev := JSON{}
		err := json.Unmarshal([]byte(value), &rev)
		if err != nil {
			result_err = err
		} else {
			result = rev
		}
	case TypeJSONSlice:
		tmp := parsePGArray(value)
		rev := make(JSONSlice, len(tmp))
		for i, tv := range tmp {
			ti := JSON{}
			err := json.Unmarshal([]byte(tv), &ti)
			if err != nil {
				result_err = err
				break
			}
			rev[i] = ti
		}
		if result_err == nil {
			result = rev
		}
	default:
		result_err = fmt.Errorf("invalid type %q", dataType)
	}
	if !dataType.NotNull {
		switch dataType.Type {
		case TypeString:
			rev := NullString{}
			if result != nil {
				rev.Valid = true
				rev.String = result.(string)
			}
			result = rev
		case TypeBool:
			rev := NullBool{}
			if result != nil {
				rev.Valid = true
				rev.Bool = result.(bool)
			}
			result = rev
		case TypeInt64:
			rev := NullInt64{}
			if result != nil {
				rev.Valid = true
				rev.Int64 = result.(int64)
			}
			result = rev
		case TypeFloat64:
			rev := NullFloat64{}
			if result != nil {
				rev.Valid = true
				rev.Float64 = result.(float64)
			}
			result = rev
		case TypeTime:
			rev := NullTime{}
			if result != nil {
				rev.Valid = true
				rev.Time = result.(time.Time)
			}
			result = rev
		case TypeBytea:
			rev := NullBytea{}
			if result != nil {
				rev.Valid = true
				rev.Bytea = result.(Bytea)
			}
			result = rev
		case TypeStringSlice:
			rev := NullStringSlice{}

			if result != nil {
				rev.Valid = true
				rev.Slice = result.(StringSlice)
			}
			result = rev
		case TypeBoolSlice:
			rev := NullBoolSlice{}
			if result != nil {
				rev.Valid = true
				rev.Slice = result.(BoolSlice)
			}
			result = rev
		case TypeInt64Slice:
			rev := NullInt64Slice{}
			if result != nil {
				rev.Valid = true
				rev.Slice = result.(Int64Slice)
			}
			result = rev
		case TypeFloat64Slice:
			rev := NullFloat64Slice{}
			if result != nil {
				rev.Valid = true
				rev.Slice = result.(Float64Slice)
			}
			result = rev
		case TypeTimeSlice:
			rev := NullTimeSlice{}
			if result != nil {
				rev.Valid = true
				rev.Slice = result.(TimeSlice)
			}
			result = rev
		case TypeJSON:
			rev := NullJSON{}
			if result != nil {
				rev.Valid = true
				rev.Json = result.(JSON)
			}
			result = rev
		case TypeJSONSlice:
			rev := NullJSONSlice{}
			if result != nil {
				rev.Valid = true
				rev.Slice = result.(JSONSlice)
			}
		default:
			result_err = fmt.Errorf("invalid nullable type %v", dataType.Type)
		}
	}
	return
}
