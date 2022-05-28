package dbop

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type (
	Record  map[string]interface{}
	Records []Record
)

func (r Record) Str(field string) string {
	switch r[field] {
	case nil:
		return ""
	default:
		return r[field].(string)
	}
}

func (r Record) NStr(field string) sql.NullString {
	switch r[field] {
	case nil:
		return sql.NullString{Valid: false}
	default:
		return sql.NullString{String: r[field].(string), Valid: true}
	}
}

func (r Record) Float64(field string) float64 {
	switch r[field] {
	case nil:
		return 0
	default:
		v, _ := strconv.ParseFloat(r.Str(field), 64)
		return v
	}
}

func (r Record) Int(field string) int64 {
	v, _ := strconv.Atoi(r.Str(field))
	return int64(v)
}

func (r Record) Time(field string) time.Time {
	t, err := time.Parse(time.RFC3339, r.Str(field))
	if err != nil {
		return time.Time{}
	}
	return t
}

func (r Record) NTime(field string) sql.NullTime {
	t, err := time.Parse(time.RFC3339, r.Str(field))
	if err != nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: t, Valid: true}
}

func stringify(r Record) map[string]string {
	s := make(map[string]string)
	for k, v := range r {
		if v == nil {
			s[k] = ""
		} else {
			s[k] = v.(string)
		}
	}
	return s
}

func Stringify(rs Records) []map[string]string {
	var ss []map[string]string
	for _, r := range rs {
		ss = append(ss, stringify(r))
	}
	return ss
}

func RangeRows(rows *sql.Rows, proc func() bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
		rows.Close()
	}()
	for rows.Next() {
		if !proc() {
			break
		}
	}
	assert(rows.Err())
	return
}

func FetchRows(rows *sql.Rows) (recs Records, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
		rows.Close()
	}()
	cols, err := rows.Columns()
	assert(err)
	raw := make([][]byte, len(cols))
	ptr := make([]interface{}, len(cols))
	for i := range raw {
		ptr[i] = &raw[i]
	}
	for rows.Next() {
		assert(rows.Scan(ptr...))
		rec := make(map[string]interface{})
		for i, r := range raw {
			if r == nil {
				rec[cols[i]] = nil
			} else {
				rec[cols[i]] = string(r)
			}
		}
		recs = append(recs, rec)
	}
	assert(rows.Err())
	return
}

func FetchRow(rows *sql.Rows, proc func(Record) bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
		rows.Close()
	}()
	cols, err := rows.Columns()
	assert(err)
	raw := make([][]byte, len(cols))
	ptr := make([]interface{}, len(cols))
	for i := range raw {
		ptr[i] = &raw[i]
	}
	for rows.Next() {
		assert(rows.Scan(ptr...))
		rec := make(map[string]interface{})
		for i, r := range raw {
			if r == nil {
				rec[cols[i]] = nil
			} else {
				rec[cols[i]] = string(r)
			}
		}
		if !proc(rec) {
			break
		}
	}
	assert(rows.Err())
	return
}

func insRows(conn interface{}, oper, table string, rows Records) (cnt int, err error) {
	if len(rows) == 0 {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			err = trace("%v", e)
		}
	}()
	var tx *sql.Tx
	switch c := conn.(type) {
	case *sql.DB:
		tx, err = c.Begin()
		assert(err)
		defer func() {
			if e := recover(); e != nil {
				tx.Rollback()
				panic(e)
			}
			assert(tx.Commit())
		}()
	case *sql.Tx:
		tx = c
	default:
		panic(errors.New("dbop: conn must be *sql.DB or *sql.Tx"))
	}
	var keys []string
	for k := range rows[0] {
		keys = append(keys, k)
	}
	stmt := fmt.Sprintf("%s INTO `%s` (`%s`) VALUES (?"+
		strings.Repeat(`,?`, len(keys)-1)+")", oper, table,
		strings.Join(keys, "`,`"))
	st, err := tx.Prepare(stmt)
	assert(err)
	for _, r := range rows {
		var args []interface{}
		for _, k := range keys {
			args = append(args, r[k])
		}
		res, err := st.Exec(args...)
		assert(err)
		ra, err := res.RowsAffected()
		assert(err)
		cnt += int(ra)
	}
	return
}

func InsertRows(conn interface{}, table string, rows Records) (cnt int, err error) {
	return insRows(conn, "INSERT", table, rows)
}

func ReplaceRows(conn interface{}, table string, rows Records) (cnt int, err error) {
	return insRows(conn, "REPLACE", table, rows)
}

func DeleteRows(conn interface{}, table string, rows Records, keys []string) (cnt int, err error) {
	if len(rows) == 0 || len(keys) == 0 {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			err = trace("%v", e)
		}
	}()
	var tx *sql.Tx
	switch c := conn.(type) {
	case *sql.DB:
		tx, err = c.Begin()
		assert(err)
		defer func() {
			if e := recover(); e != nil {
				tx.Rollback()
				panic(e)
			}
			assert(tx.Commit())
		}()
	case *sql.Tx:
		tx = c
	default:
		panic(errors.New("DeleteRows: conn must be *sql.DB or *sql.Tx"))
	}
	stmt := fmt.Sprintf("DELETE FROM `%s` WHERE ", table)
	for _, r := range rows {
		var args []interface{}
		var where []string
		for _, k := range keys {
			v := r[k]
			if v == nil {
				where = append(where, fmt.Sprintf("(`%s` IS NULL)", k))
			} else {
				where = append(where, fmt.Sprintf("(`%s`=?)", k))
				args = append(args, v)
			}
		}
		cmd := stmt + strings.Join(where, " AND ")
		res, err := tx.Exec(cmd, args...)
		assert(err)
		ra, err := res.RowsAffected()
		assert(err)
		cnt += int(ra)
	}
	return
}

func UpdateRows(conn interface{}, table string, rows Records, keys []string) (cnt int, err error) {
	if len(rows) == 0 || len(keys) == 0 {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			err = trace("%v", e)
		}
	}()
	var tx *sql.Tx
	switch c := conn.(type) {
	case *sql.DB:
		tx, err = c.Begin()
		assert(err)
		defer func() {
			if e := recover(); e != nil {
				tx.Rollback()
				panic(e)
			}
			assert(tx.Commit())
		}()
	case *sql.Tx:
		tx = c
	default:
		panic(errors.New("UpdateRows: conn must be *sql.DB or *sql.Tx"))
	}
	stmt := fmt.Sprintf("UPDATE `%s` SET ", table)
	for _, r := range rows {
		var args, cond []interface{}
		var sets, where []string
		for _, k := range keys {
			v := r[k]
			if v == nil {
				where = append(where, fmt.Sprintf("(`%s` IS NULL)", k))
			} else {
				where = append(where, fmt.Sprintf("(`%s`=?)", k))
				cond = append(cond, v)
			}
			delete(r, k)
		}
		if len(r) == 0 {
			continue
		}
		for k, v := range r {
			if v == nil {
				sets = append(sets, fmt.Sprintf("`%s`=NULL", k))
			} else {
				sets = append(sets, fmt.Sprintf("`%s`=?", k))
				args = append(args, v)
			}
		}
		cmd := stmt + strings.Join(sets, ",") + " WHERE " + strings.Join(where, " AND ")
		args = append(args, cond...)
		res, err := tx.Exec(cmd, args...)
		assert(err)
		ra, err := res.RowsAffected()
		assert(err)
		cnt += int(ra)
	}
	return
}
