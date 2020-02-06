// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/ericlagergren/decimal"
	_ "github.com/lib/pq"
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
		"bool":      genBool,
		"decimal":   genDecimal,
		"double":    genFloat64,
		"real":      genFloat32,
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

	// Min and Max values for all numeric types (inclusive).
	// Or Min and Max amount of words or characters.
	Min, Max int64

	// Scale for decimal numbers
	Scale int

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
			v = col.rand.Int31n(max + 1)
		}

		switch {
		case col.Null && v == 0:
			return nil
		case v >= min:
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
			v = col.rand.Int63n(col.Max + 1)
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
	n := genInt64(col)
	if n == nil {
		return nil
	}

	words := make([]string, n.(int64))
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

func genBool(col *Column) interface{} {
	if col.rand == nil {
		col.n++
		return 1 == col.n%2
	}
	return col.rand.Int63n(2) == 1
}

func genDecimal(col *Column) interface{} {
	i := genInt64(col)
	if i == nil {
		return i
	}
	return decimal.New(i.(int64), col.Scale).String()
}

func genFloat64(col *Column) interface{} {
	i := genInt64(col)
	if i == nil {
		return i
	}
	f, _ := decimal.New(i.(int64), col.Scale).Float64()
	return f
}

func genFloat32(col *Column) interface{} {
	f := genFloat64(col)
	if f == nil {
		return nil
	}
	return float32(f.(float64))
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
			col.rand = rand.New(rand.NewSource(col.Seed))
		} else if col.Seed != 0 {
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
			return fmt.Errorf("table.insert on %s, row %d: %w", t.Name, i, err)
		}
	}

	return nil
}

// DriverName is a supported sql driver
type DriverName string

const (
	// Sqlite driver name
	Sqlite DriverName = "sqlite3"
	// Postgres driver name
	Postgres DriverName = "postgres"
)

// Schema holds all data generation parameters.
type Schema struct {
	Driver         DriverName
	DataSourceName string
	MaxDuration    int
	WordLists      map[string]string
	Tables         []Table
}

func (s *Schema) connect() (*sql.DB, error) {
	return sql.Open(string(s.Driver), s.DataSourceName)
}

func loadList(fileName string) ([]string, error) {
	fh, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("loadList: %w", err)
	}
	defer fh.Close()

	var list []string
	for sc := bufio.NewScanner(fh); sc.Scan(); {
		list = append(list, sc.Text())
	}
	return list, nil
}

func loadSchema(filename string) (*Schema, error) {
	js, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	schema := new(Schema)
	if err = json.Unmarshal(js, schema); err != nil {
		return nil, fmt.Errorf("loadSchema: %w", err)
	}

	wordMap = make(map[string][]string)
	for k, v := range schema.WordLists {
		if wordMap[k], err = loadList(v); err != nil {
			return nil, fmt.Errorf("loadSchema: %w", err)
		}
	}

	return schema, nil
}

var (
	schemaFile string
)

func run() int {
	s, err := loadSchema(schemaFile)
	if err != nil {
		log.Printf("Run: %v", err)
		return 2
	}
	db, err := s.connect()
	if err != nil {
		log.Printf("Run: %v", err)
		return 2
	}
	defer db.Close()

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if s.MaxDuration == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(
			context.Background(),
			time.Duration(s.MaxDuration)*time.Second,
		)
	}
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Run: %v", err)
		return 2
	}
	defer tx.Rollback()

	for _, t := range s.Tables {
		if err = t.insert(ctx, tx); err != nil {
			log.Printf("Run: %v", err)
			return 2
		}
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Run: %v", err)
		return 2
	}

	return 0
}

func main() {
	flag.StringVar(&schemaFile, "schema", "example.json", "json file with schema definition")
	flag.Parse()
	os.Exit(run())
}
