package clients

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/kndndrj/nvim-dbee/dbee/conn"
	_ "github.com/lib/pq"
)

type PostgresClient struct {
	sql *sqlClient
}

func NewPostgres(url string) (*PostgresClient, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to database: %v\n", err)
	}

	return &PostgresClient{
		sql: newSql(db),
	}, nil
}

func (c *PostgresClient) Query(query string) (conn.IterResult, error) {

	dbRows, err := c.sql.query(query)
	if err != nil {
		return nil, err
	}

	meta := conn.Meta{
		Query:     query,
		Timestamp: time.Now(),
	}

	rows := newPGRows(dbRows, meta)

	return rows, nil
}

func (c *PostgresClient) Schema() (conn.Schema, error) {
	query := `
		SELECT table_schema, table_name FROM information_schema.tables UNION ALL
		SELECT schemaname, matviewname FROM pg_matviews;
	`

	rows, err := c.Query(query)
	if err != nil {
		return nil, err
	}

	var schema = make(conn.Schema)

	for {
		row, err := rows.Next()
		if row == nil {
			break
		}
		if err != nil {
			return nil, err
		}

		// We know for a fact there are 2 string fields (see query above)
		key := row[0].(string)
		val := row[1].(string)
		schema[key] = append(schema[key], val)
	}

	return schema, nil
}

func (c *PostgresClient) Close() {
	c.sql.close()
}

type PostgresRows struct {
	dbRows *sqlRows
	meta   conn.Meta
}

func newPGRows(rows *sqlRows, meta conn.Meta) *PostgresRows {
	return &PostgresRows{
		dbRows: rows,
		meta:   meta,
	}
}

func (r *PostgresRows) Meta() (conn.Meta, error) {
	return r.meta, nil
}

func (r *PostgresRows) Header() (conn.Header, error) {
	return r.dbRows.header()
}

func (r *PostgresRows) Next() (conn.Row, error) {

	row, err := r.dbRows.next()
	if err != nil {
		return nil, err
	}

	// fix for pq interpreting strings as bytes - hopefully does not break
	for i, val := range row {
		valb, ok := val.([]byte)
		if ok {
			val = string(valb)
		}
		row[i] = val
	}

	return row, nil
}

func (r *PostgresRows) Close() {
	r.dbRows.close()
}
