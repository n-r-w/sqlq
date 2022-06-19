package sqlq

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/n-r-w/nerr"
	"github.com/n-r-w/sqlb"
)

// Query - wrapper for pgx Query/Exec
type Query struct {
	tx   *Tx
	pool *pgxpool.Pool

	ctx    context.Context
	rows   pgx.Rows
	tag    pgconn.CommandTag
	fields map[string]int

	lastValues       []interface{}
	lastDescriptions []pgproto3.FieldDescription
}

// NewQuery - create a Query based on *sqlq.Tx
func NewQuery(pool *pgxpool.Pool, context context.Context) *Query {
	return &Query{
		pool:   pool,
		ctx:    context,
		rows:   nil,
		tag:    []byte{},
		fields: map[string]int{},
	}
}

// NewQuery - create a Query based on *pgxpool.Pool
func NewQueryTx(tx *Tx, context context.Context) *Query {
	return &Query{
		tx:     tx,
		pool:   tx.pool,
		ctx:    context,
		rows:   nil,
		tag:    []byte{},
		fields: map[string]int{},
	}
}

// Context - active context
func (t *Query) Context() context.Context {
	return t.ctx
}

// Tx - active transaction
func (t *Query) Tx() *Tx {
	return t.tx
}

// Pool - active connection pool
func (t *Query) Pool() *pgxpool.Pool {
	return t.pool
}

// Close - close the selection. Use for Select in case we don't get to the end of Next
func (q *Query) Close() error {
	if q.rows != nil {
		q.lastValues, _ = q.rows.Values()
		q.lastDescriptions = q.rows.FieldDescriptions()

		// ошибка в rows появляется только после закрытия (это не баг, а фича)
		q.rows.Close()
		err := q.rows.Err()
		q.rows = nil
		return err
	}
	return nil
}

// RowsAffected -  the number of processed rows, for Select closes the query (Next will not work)
func (q *Query) RowsAffected() int64 {
	if q.rows != nil {
		q.rows.Close()
		return q.rows.CommandTag().RowsAffected()
	}
	return 0
}

// IsSelect - Select request type
func (q *Query) IsSelect() bool {
	return q.rows != nil
}

// IsCommand - request type Insert, Delete, Update
func (q *Query) IsCommand() bool {
	return len(q.tag) > 0
}

// Exec - executing the insert, update, delete command
func (q *Query) Exec(sql string) error {
	q.rows = nil
	q.lastValues = nil
	q.lastDescriptions = nil
	q.fields = make(map[string]int)

	var err error
	if q.tx != nil {
		q.tag, err = q.tx.tx.Exec(q.ctx, sql, pgx.QuerySimpleProtocol(true))
	} else {
		q.tag, err = q.pool.Exec(q.ctx, sql, pgx.QuerySimpleProtocol(true))
	}

	return nerr.New(err)
}

// ExecBind - execution of the insert, update, delete command with the substitution of values in the template
func (q *Query) ExecBind(sqlTemplate string, values map[string]interface{}, key string) error {
	binder := sqlb.NewBinder(sqlTemplate, key)
	if err := binder.BindValues(values); err != nil {
		return err
	}

	if sql, err := binder.Sql(); err != nil {
		return err
	} else {
		return q.Exec(sql)
	}
}

// Select - executing the select command
func (q *Query) Select(sql string) error {
	q.tag = []byte{}
	q.fields = map[string]int{}
	q.lastValues = nil
	q.lastDescriptions = nil

	var err error
	if q.tx != nil {
		q.rows, err = q.tx.tx.Query(q.ctx, sql, pgx.QuerySimpleProtocol(true))
	} else {
		q.rows, err = q.pool.Query(q.ctx, sql, pgx.QuerySimpleProtocol(true))
	}

	if err != nil {
		q.rows = nil
		return nerr.New(err)
	}

	for i, d := range q.Fields() {
		q.fields[strings.ToLower(string(d.Name))] = i
	}

	return nil
}

// SelectRow - executing the select command for 1 row select
func (q *Query) SelectRow(sql string) (bool, error) {
	err := q.Select(sql)
	if err != nil {
		return false, err
	}

	ok := q.Next()
	err = q.Close()

	return ok, err
}

// SelectBind - executing the select command with the substitution of values in the template
func (q *Query) SelectBind(sqlTemplate string, values map[string]interface{}, key string) error {
	binder := sqlb.NewBinder(sqlTemplate, key)
	if err := binder.BindValues(values); err != nil {
		return err
	}

	if sql, err := binder.Sql(); err != nil {
		return err
	} else {
		return q.Select(sql)
	}
}

// SelectBindRow - executing the select command with the substitution of values in the template for 1 row select
func (q *Query) SelectBindRow(sqlTemplate string, values map[string]interface{}, key string) (bool, error) {

	err := q.SelectBind(sqlTemplate, values, key)
	if err != nil {
		return false, err
	}

	ok := q.Next()
	err = q.Close()

	return ok, err
}

// Next - get the next row (Select only)
func (q *Query) Next() bool {
	if q.rows == nil {
		return false
	}

	return q.rows.Next()
}

// Fields - list of fields (Select only)
func (q *Query) Fields() []pgproto3.FieldDescription {
	if q.rows == nil {
		if len(q.lastDescriptions) == 0 {
			return []pgproto3.FieldDescription{}
		} else {
			return q.lastDescriptions
		}
	}

	return q.rows.FieldDescriptions()
}

// FieldName -  field name by index
func (q *Query) FieldName(index int) string {
	if q.rows == nil || index < 0 || index >= len(q.Fields()) {
		return ""
	}

	return string(q.Fields()[index].Name)
}

// FieldTypeIndex -  field type by index. Result: pgtype.BoolOID, ... etc
func (q *Query) FieldTypeIndex(index int) uint32 {
	if q.rows == nil || index < 0 || index >= len(q.Fields()) {
		return 0
	}

	c, err := q.pool.Acquire(q.Context())
	if err != nil {
		return 0
	}

	if dt, ok := c.Conn().ConnInfo().DataTypeForOID(q.Fields()[index].DataTypeOID); !ok {
		return 0
	} else {
		return dt.OID
	}
}

// FieldTypeIndex -  field type by index. Result: type name
func (q *Query) FieldTypeNameIndex(index int) string {
	if q.rows == nil || index < 0 || index >= len(q.Fields()) {
		return ""
	}

	c, err := q.pool.Acquire(q.Context())
	if err != nil {
		return ""
	}

	if dt, ok := c.Conn().ConnInfo().DataTypeForOID(q.Fields()[index].DataTypeOID); !ok {
		return ""
	} else {
		return dt.Name
	}
}

// FieldType -  field type by name. Result: pgtype.BoolOID, ... etc
func (q *Query) FieldType(name string) uint32 {
	if index, ok := q.fields[name]; ok {
		return q.FieldTypeIndex(index)
	}

	return 0
}

// FieldType -  field type by name. Result: type name
func (q *Query) FieldTypeName(name string) string {
	if index, ok := q.fields[name]; ok {
		return q.FieldTypeNameIndex(index)
	}

	return ""
}

// Contains - does the specified field contain (Select only)
func (q *Query) Contains(field string) bool {
	_, ok := q.fields[strings.ToLower(field)]
	return ok
}

func (q *Query) Values() ([]interface{}, error) {
	if q.rows == nil {
		if len(q.lastDescriptions) == 0 {
			return []interface{}{}, nil
		} else {
			return q.lastValues, nil
		}
	}

	return q.rows.Values()
}

func (q *Query) IsNull(field string) bool {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	return q.Value(field) == nil
}

// Value - field value by name (only for Select and after a successful Next call)
func (q *Query) Value(field string) interface{} {
	pos, ok := q.fields[strings.ToLower(field)]
	if !ok {
		return nil
	}

	values, err := q.Values()
	if err != nil || len(values) <= pos {
		return nil
	}

	return values[pos]
}

// ValueIndex - field value by index (only for Select and after a successful Next call)
func (q *Query) ValueIndex(fieldIndex int) interface{} {
	if q.rows == nil || fieldIndex < 0 || fieldIndex >= len(q.fields) {
		return nil
	}

	vals, err := q.Values()
	if err != nil || len(vals) <= fieldIndex {
		return nil
	}

	return vals[fieldIndex]
}

// String - field value by name, converted to string (only for Select and after a successful Next call)
func (q *Query) String(field string) string {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return ""
	}

	switch d := v.(type) {
	case string:
		return d
	case pgtype.Text:
		return d.String
	case pgtype.Varchar:
		return d.String
	case []byte:
		if b, err := hex.DecodeString(string(d)); err != nil {
			return string(d)
		} else {
			return string(b)
		}

	default:
		return fmt.Sprintf("%v", d)
	}
}

func (q *Query) Json(field string) interface{} {
	return q.Value(field)
}

func (q *Query) StringArray(field string) []string {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return []string{}
	}

	switch d := v.(type) {
	case []string:
		return d
	case pgtype.TextArray:
		var res []string
		for _, x := range d.Elements {
			res = append(res, x.String)
		}
		return res
	case pgtype.VarcharArray:
		var res []string
		for _, x := range d.Elements {
			res = append(res, x.String)
		}
		return res
	default:
		s := q.String(field)
		var res []string
		res = append(res, s)
		return res
	}
}

func (q *Query) TimeArray(field string) []time.Time {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return []time.Time{}
	}

	switch d := v.(type) {
	case []time.Time:
		return d
	case pgtype.TimestampArray:
		var res []time.Time
		for _, x := range d.Elements {
			res = append(res, x.Time)
		}
		return res
	case pgtype.TimestamptzArray:
		var res []time.Time
		for _, x := range d.Elements {
			res = append(res, x.Time)
		}
		return res

	default:
		t := q.Time(field)
		var res []time.Time
		res = append(res, t)
		return res
	}
}

func (q *Query) IntArray(field string) []int {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return []int{}
	}

	return intArrayHelper[int](q, field)
}

func (q *Query) IntArray64(field string) []int64 {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return []int64{}
	}

	return intArrayHelper[int64](q, field)
}

func intArrayHelper[T int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64](q *Query, field string) []T {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return []T{}
	}

	switch d := v.(type) {
	case pgtype.Int2Array:
		var res []T
		for _, x := range d.Elements {
			res = append(res, T(x.Int))
		}
		return res
	case pgtype.Int4Array:
		var res []T
		for _, x := range d.Elements {
			res = append(res, T(x.Int))
		}
		return res
	case pgtype.Int8Array:
		var res []T
		for _, x := range d.Elements {
			res = append(res, T(x.Int))
		}
		return res
	default:
		i := q.Int(field)
		var res []T
		res = append(res, T(i))
		return res
	}
}

func intConvertHelper[T int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64](v interface{}) T {
	if v == nil {
		return T(0)
	}

	switch d := v.(type) {
	case bool:
		if d {
			return 1
		} else {
			return 0
		}
	case int:
		return T(d)
	case int8:
		return T(d)
	case int16:
		return T(d)
	case int32:
		return T(d)
	case int64:
		return T(d)
	case uint:
		return T(d)
	case uint8:
		return T(d)
	case uint16:
		return T(d)
	case uint32:
		return T(d)
	case uint64:
		return T(d)
	case float32:
		return T(d)
	case float64:
		return T(d)
	case string:
		if r, err := strconv.ParseInt(d, 10, 64); err == nil {
			return T(r)
		}

	default:
	}

	panic("can't convert")
}

// Int64 - field value by name, converted to int64 (only for Select and after a successful Next call)
func (q *Query) Int64(field string) int64 {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return 0
	}

	return intConvertHelper[int64](v)
}

// UInt64 - field value by name, converted to uint64 (only for Select and after a successful Next call)
func (q *Query) UInt64(field string) uint64 {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return 0
	}

	return intConvertHelper[uint64](v)
}

// Bool - field value by name, converted to int64 (only for Select and after a successful Next call)
func (q *Query) Bool(field string) bool {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return false
	}

	switch d := v.(type) {
	case bool:
		return d
	case int:
		return d > 0
	case int8:
		return d > 0
	case int16:
		return d > 0
	case int32:
		return d > 0
	case int64:
		return d > 0
	case uint:
		return d > 0
	case uint8:
		return d > 0
	case uint16:
		return d > 0
	case uint32:
		return d > 0
	case uint64:
		return d > 0
	case float32:
		return d > 0
	case float64:
		return d > 0
	case string:
		s := strings.ToLower(d)
		if s == "true" || s == "yes" {
			return true
		} else if s == "false" || s == "no" {
			return false
		}

	default:
	}

	panic(fmt.Errorf("can't convert to bool: %s", field))
}

// Int - field value by name, converted to int64 (only for Select and after a successful Next call)
func (q *Query) Int(field string) int {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return 0
	}

	return intConvertHelper[int](v)
}

// Float64 - field value by name, converted to float64 (only for Select and after a successful Next call)
func (q *Query) Float64(field string) float64 {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return 0
	}

	switch d := v.(type) {
	case bool:
		if d {
			return 1
		} else {
			return 0
		}
	case int:
		return float64(d)
	case int8:
		return float64(d)
	case int16:
		return float64(d)
	case int32:
		return float64(d)
	case int64:
		return float64(d)
	case uint:
		return float64(d)
	case uint8:
		return float64(d)
	case uint16:
		return float64(d)
	case uint32:
		return float64(d)
	case uint64:
		return float64(d)
	case float32:
		return float64(d)
	case float64:
		return d
	case string:
		if r, err := strconv.ParseFloat(d, 64); err == nil {
			return r
		}

	default:
	}

	panic(fmt.Errorf("can't convert to float: %s", field))
}

// Float32 - field value by name, converted to float32 (only for Select and after a successful Next call)
func (q *Query) Float32(field string) float32 {
	return float32(q.Float64(field))
}

// Time - field value by name, converted to time.Time (only for Select and after a successful Next call)
func (q *Query) Time(field string) time.Time {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return time.Time{}
	}

	switch d := v.(type) {

	case time.Time:
		return d
	case string:
		format := "2006-01-02 15:04:05"
		if len(d) > len(format) {
			format = "2006-01-02 15:04:05.000"
		}
		if len(d) > len(format) {
			format = "2006-01-02 15:04:05.000 -0700"
		}

		t, err := time.Parse(format, d)
		if err == nil {
			return t
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		dur, err := time.ParseDuration(fmt.Sprintf("%dµs", d)) // postrgesql хранит time как микросекунды с начала суток
		if err == nil {
			t := time.Time{}
			t = t.Add(dur)
			return t
		}

	default:
	}

	panic(fmt.Errorf("can't convert to time.Time: %s", field))
}

// Duration - field value by name, converted to time.Duration (only for Select and after a successful Next call)
func (q *Query) Duration(field string) time.Duration {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	t := q.FieldType(field)
	if t == 0 {
		return 0
	}

	if t == pgtype.TimeOID {
		ms := q.Int64(field)
		return time.Duration(int64(time.Microsecond) * ms)
	}

	panic(fmt.Errorf("can't convert to time.Duration: %s", field))
}

// Bytes - field value by name, converted to []byte (only for Select and after a successful Next call)
func (q *Query) Bytes(field string) []byte {
	if !q.Contains(field) {
		panic(fmt.Errorf("can't find field %s", field))
	}

	v := q.Value(field)
	if v == nil {
		return []byte{}
	}

	switch d := v.(type) {
	case []byte:
		return d
	case string:
		return []byte(d)
	default:
	}

	panic(fmt.Errorf("can't convert to []byte: %s", field))
}

func Select(pool *pgxpool.Pool, context context.Context, sql string) (*Query, error) {
	q := NewQuery(pool, context)
	if err := q.Select(sql); err != nil {
		return nil, err
	}
	return q, nil
}

func SelectRow(pool *pgxpool.Pool, context context.Context, sql string) (*Query, error) {
	q := NewQuery(pool, context)
	if ok, err := q.SelectRow(sql); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}

	return q, nil
}

func SelectTx(tx *Tx, sql string) (*Query, error) {
	q := NewQueryTx(tx, tx.ctx)
	if err := q.Select(sql); err != nil {
		return nil, err
	}
	return q, nil
}

func SelectTxRow(tx *Tx, sql string) (*Query, error) {
	q := NewQueryTx(tx, tx.ctx)
	if ok, err := q.SelectRow(sql); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}

	return q, nil
}

func Exec(pool *pgxpool.Pool, context context.Context, sql string) (*Query, error) {
	q := NewQuery(pool, context)
	if err := q.Exec(sql); err != nil {
		return nil, err
	}
	return q, nil
}

func ExecTx(tx *Tx, sql string) error {
	q := NewQueryTx(tx, tx.ctx)
	if err := q.Exec(sql); err != nil {
		return err
	}
	return nil
}
