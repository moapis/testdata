// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ValueFunc generates and returns a type specific value for a column.
type ValueFunc func(*Column) interface{}

var (
	// Registry holds a ValueFunc for each SQL type, identified by string.
	Registry = map[string]ValueFunc{
		"int":       genInt32,
		"bigint":    genInt64,
		"timestamp": genTime,
		"text":      genText,
		"char":      genChar,
		"bytea":     genByteA,
	}

	wordMap map[string][]string
)

// Column generation config
type Column struct {
	// Name of this column, can be prefixed with SQL schema name if required.
	// Eg: "public.articles"
	Name string

	// SQLType should be one of the supported type strings.
	// See the global Registry variable.
	SQLType string

	// WordListID should be a label from any specified word list.
	WordListID string

	// Seed is used for the deterministic pseudo-random generator.
	// Random values are used for numeric values, the amount of words/characters
	// and which word/characters are selected from the list.
	// When 0, incremented values are used.
	Seed int64

	// Min and Max values for all numeric types.
	// Or Min and Max amounbt of words or characters.
	Min, Max int64

	// Null-able column.
	// If set to true, 0 values or 0 lenght text or characters will be instered as null
	Null bool

	rand     *rand.Rand
	n        int64 // Incrementer
	wordList []string

	value ValueFunc
}

func genInt32(col *Column) interface{} {
	if col.rand == nil {
		col.n++
		return int32(col.n)
	}

	min, max := int32(col.Min), int32(col.Max)

	for {
		var v int32
		if max == 0 {
			v = col.rand.Int31()
		} else {
			v = col.rand.Int31n(max)
		}

		switch {
		case col.Null && v == 0:
			return nil
		case v > min:
			return v
		}
	}
}

func genInt64(col *Column) interface{} {
	if col.rand == nil {
		col.n++
		return col.n
	}

	for {
		var v int64
		if col.Max == 0 {
			v = col.rand.Int63()
		} else {
			v = col.rand.Int63n(col.Max)
		}

		switch {
		case col.Null && v == 0:
			return nil
		case v >= col.Min:
			return v
		}
	}
}

func genTime(col *Column) interface{} {
	secs := genInt64(col)
	if secs == nil {
		return nil
	}
	return time.Unix(secs.(int64), 0)
}

func genText(col *Column) interface{} {
	l, _ := genInt64(col).(int64)
	if l == 0 {
		return nil
	}

	words := make([]string, l)
	for i := range words {
		words[i] = col.wordList[col.rand.Intn(len(col.wordList))]
	}

	return strings.Join(words, " ")
}

const alphaNumeric = "0123456789aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ"

func genChar(col *Column) interface{} {
	l := genInt64(col)
	if l == nil {
		return nil
	}

	chars := make([]byte, l.(int64))
	for i := range chars {
		chars[i] = alphaNumeric[col.rand.Intn(len(alphaNumeric))]
	}

	return string(chars)
}

func genByteA(col *Column) interface{} {
	l := genInt64(col)
	if l == nil {
		return nil
	}

	bs := make([]byte, l.(int64))
	col.rand.Read(bs)
	return bs
}

func (col *Column) setValueFunc() error {
	vf, ok := Registry[col.SQLType]
	if !ok {
		return fmt.Errorf("Unsupported type %v for column %v", col.SQLType, col.Name)
	}
	col.value = vf
	return nil
}

// Table model
type Table struct {
	Name    string
	Amount  int
	Columns []*Column
}

func (t *Table) row() []interface{} {
	row := make([]interface{}, len(t.Columns))

	for i, col := range t.Columns {
		row[i] = col.value(col)
	}

	return row
}

const (
	insert = "insert into %s (%s) values (%s);"
)

func (t *Table) insertQuery() string {
	cols, args := make([]string, len(t.Columns)), make([]string, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = c.Name
		args[i] = fmt.Sprintf("$%d", i+1) // $1
	}

	query := fmt.Sprintf(
		insert, t.Name,
		strings.Join(cols, ", "),
		strings.Join(args, ", "), // $1, $2, $N
	)
	log.Println(query)
	return query
}

func (t *Table) prepareColumns() error {
	for _, col := range t.Columns {
		if err := col.setValueFunc(); err != nil {
			return err
		}

		if col.WordListID != "" {
			wl, ok := wordMap[col.WordListID]
			if !ok {
				return fmt.Errorf("Wordlist %s for column %s not defined", col.WordListID, col.Name)
			}
			col.wordList = wl
		}

		if col.Seed != 0 {
			col.rand = rand.New(rand.NewSource(col.Seed))
		}
	}

	return nil
}

func (t *Table) insert(ctx context.Context, tx *sql.Tx) error {
	if err := t.prepareColumns(); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, t.insertQuery())
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < t.Amount; i++ {
		if _, err := stmt.ExecContext(ctx, t.row()...); err != nil {
			return fmt.Errorf("table.generate: %w", err)
		}
	}

	return nil
}

// DriverName is a supported sql driver
type DriverName string

const (
	// Sqlite driver name
	Sqlite DriverName = "sqlite3"
)

// Schema holds all data generation parameters.
type Schema struct {
	Driver         DriverName
	DataSourceName string
	WordLists      map[string]string
	Tables         []Table
}

func loadSchema(filename string) (*Schema, error) {
	js, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	schema := new(Schema)
	return schema, json.Unmarshal(js, schema)
}

var db *sql.DB

func main() {

}
