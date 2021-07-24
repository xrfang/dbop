package dbop

import (
	"database/sql"
)

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
