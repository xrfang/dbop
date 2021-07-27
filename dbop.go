package dbop

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func stringify(r map[string]interface{}) map[string]string {
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

func Stringify(rs []map[string]interface{}) []map[string]string {
	var ss []map[string]string
	for _, r := range rs {
		ss = append(ss, stringify(r))
	}
	return ss
}

func RangeRows(rows *sql.Rows, proc func() bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			rows.Close()
			err = trace("%v", e)
		}
	}()
	for rows.Next() {
		if !proc() {
			break
		}
	}
	assert(rows.Err())
	return
}

func FetchRows(rows *sql.Rows) (recs []map[string]interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			rows.Close()
			err = trace("%v", e)
		}
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

func FetchRow(rows *sql.Rows, proc func(map[string]interface{}) bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			rows.Close()
			err = trace("%v", e)
		}
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

func InsertRows(conn interface{}, table string, rows []map[string]interface{}) (cnt int, err error) {
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
		panic(errors.New("InsertRows: conn must be *sql.DB or *sql.Tx"))
	}
	var keys []string
	for k := range rows[0] {
		keys = append(keys, k)
	}
	stmt := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (?"+
		strings.Repeat(`,?`, len(keys)-1)+")", table,
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

func DeleteRows(conn interface{}, table string, rows []map[string]interface{}, keys []string) (cnt int, err error) {
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
