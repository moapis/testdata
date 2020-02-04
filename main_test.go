// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
)

var (
	mock sqlmock.Sqlmock
	db   *sql.DB
)

func init() {
	wordMap = map[string][]string{
		"some": {"hello", "worlds", "spanac", "foo", "bar"},
	}

	qm := sqlmock.QueryMatcherFunc(func(a, b string) error {
		if strings.Trim(a, " ") != strings.Trim(b, " ") {
			return fmt.Errorf(`actual sql: "%s" does not equal to expected "%s"`, a, b)
		}
		return nil
	})

	var err error
	if db, mock, err = sqlmock.New(sqlmock.QueryMatcherOption(qm)); err != nil {
		log.Panic(err)
	}
}

func Test_genInt32(t *testing.T) {
	tests := []struct {
		name string
		col  *Column
		want interface{}
	}{
		{
			"Serial",
			&Column{
				n: 22,
			},
			int32(23),
		},
		{
			"Null",
			&Column{
				Null: true,
				Min:  0,
				Max:  1,
				rand: rand.New(rand.NewSource(9)),
			},
			nil,
		},
		{
			"No max",
			&Column{
				Null: true,
				Min:  0,
				Max:  0,
				rand: rand.New(rand.NewSource(2)),
			},
			int32(359266786),
		},
		{
			"High minimal",
			&Column{
				Null: true,
				Min:  359266786,
				Max:  0,
				rand: rand.New(rand.NewSource(2)),
			},
			int32(569199786),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genInt32(tt.col); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genInt32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_genInt64(t *testing.T) {
	tests := []struct {
		name string
		col  *Column
		want interface{}
	}{
		{
			"Serial",
			&Column{
				n: 22,
			},
			int64(23),
		},
		{
			"Null",
			&Column{
				Null: true,
				Min:  0,
				Max:  1,
				rand: rand.New(rand.NewSource(9)),
			},
			nil,
		},
		{
			"No max",
			&Column{
				Null: true,
				Min:  0,
				Max:  0,
				rand: rand.New(rand.NewSource(2)),
			},
			int64(1543039099823358511),
		},
		{
			"High minimal",
			&Column{
				Null: true,
				Min:  1543039099823358512,
				Max:  0,
				rand: rand.New(rand.NewSource(2)),
			},
			int64(2444694468985893231),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genInt64(tt.col); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_genTime(t *testing.T) {
	tests := []struct {
		name string
		col  *Column
		want interface{}
	}{
		{
			"Null",
			&Column{
				Null: true,
				Min:  0,
				Max:  1,
				rand: rand.New(rand.NewSource(9)),
			},
			nil,
		},
		{
			"Time interval",
			&Column{
				Min:  15000,
				Max:  15001,
				rand: rand.New(rand.NewSource(9)),
			},
			time.Unix(15000, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genTime(tt.col); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_genText(t *testing.T) {
	tests := []struct {
		name string
		col  *Column
		want interface{}
	}{
		{
			"Null",
			&Column{
				Null: true,
				Min:  0,
				Max:  1,
				rand: rand.New(rand.NewSource(9)),
			},
			nil,
		},
		{
			"3 words",
			&Column{
				Min:      3,
				Max:      4,
				rand:     rand.New(rand.NewSource(333)),
				wordList: wordMap["some"],
			},
			"worlds spanac foo",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genText(tt.col); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_genChar(t *testing.T) {
	tests := []struct {
		name string
		col  *Column
		want interface{}
	}{
		{
			"Null",
			&Column{
				Null: true,
				Min:  0,
				Max:  1,
				rand: rand.New(rand.NewSource(9)),
			},
			nil,
		},
		{
			"20 chars",
			&Column{
				Min:  20,
				Max:  21,
				rand: rand.New(rand.NewSource(333)),
			},
			"8XSb2gYJvhsbuh0F5zDS",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genChar(tt.col); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genChar() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_genByteA(t *testing.T) {
	tests := []struct {
		name string
		col  *Column
		want interface{}
	}{
		{
			"Null",
			&Column{
				Null: true,
				Min:  0,
				Max:  1,
				rand: rand.New(rand.NewSource(9)),
			},
			nil,
		},
		{
			"5 bytes",
			&Column{
				Min:  5,
				Max:  6,
				rand: rand.New(rand.NewSource(333)),
			},
			[]byte{77, 226, 244, 227, 188},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genByteA(tt.col); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genByteA() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestColumn_setValueFunc(t *testing.T) {
	tests := []struct {
		name    string
		SQLType string
		wantErr bool
	}{
		{
			"Empty type",
			"",
			true,
		},
		{
			"int type",
			"int",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Column{
				SQLType: tt.SQLType,
			}
			if err := col.setValueFunc(); (err != nil) != tt.wantErr {
				t.Errorf("Column.setValueFunc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTable_row(t *testing.T) {
	tests := []struct {
		name    string
		Columns []*Column
		want    []interface{}
	}{
		{
			"Some columns",
			[]*Column{
				{
					n:     22,
					value: genInt32,
				},
				{
					Null:  true,
					Min:   0,
					Max:   0,
					rand:  rand.New(rand.NewSource(2)),
					value: genInt64,
				},
			},
			[]interface{}{
				int32(23),
				int64(1543039099823358511),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &Table{
				Columns: tt.Columns,
			}
			if got := tb.row(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Table.row() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTable_insertQuery(t *testing.T) {
	type fields struct {
		Name    string
		Columns []*Column
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"Query",
			fields{
				"schema.table",
				[]*Column{
					{Name: "one"},
					{Name: "two"},
					{Name: "three"},
				},
			},
			"insert into schema.table (one, two, three) values ($1, $2, $3);",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &Table{
				Name:    tt.fields.Name,
				Columns: tt.fields.Columns,
			}
			if got := tb.insertQuery(); got != tt.want {
				t.Errorf("Table.insertQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func (a ValueFunc) eq(b ValueFunc) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a != nil && b == nil:
		return false
	case a == nil && b != nil:
		return false
	}

	ca := &Column{
		Max:      5,
		wordList: wordMap["some"],
		rand:     rand.New(rand.NewSource(2)),
	}
	cb := &Column{
		Max:      5,
		wordList: wordMap["some"],
		rand:     rand.New(rand.NewSource(2)),
	}
	return reflect.DeepEqual(a(ca), b(cb))
}

func TestTable_prepareColumns(t *testing.T) {
	tests := []struct {
		name    string
		Columns []*Column
		want    []*Column
		wantErr bool
	}{
		{
			"ValueFunc error",
			[]*Column{{SQLType: "foo"}},
			[]*Column{{SQLType: "foo"}},
			true,
		},
		{
			"WorList ID error",
			[]*Column{{
				SQLType:    "int",
				WordListID: "foo",
			}},
			[]*Column{{
				SQLType:    "int",
				WordListID: "foo",
				value:      genInt32,
			}},
			true,
		},
		{
			"Succes, without seed",
			[]*Column{{
				SQLType:    "int",
				WordListID: "some",
			}},
			[]*Column{{
				SQLType:    "int",
				WordListID: "some",
				value:      genInt32,
				wordList:   wordMap["some"],
				rand:       rand.New(rand.NewSource(0)),
			}},
			false,
		},
		{
			"Succes, with seed",
			[]*Column{{
				SQLType:    "int",
				WordListID: "some",
				Seed:       999,
			}},
			[]*Column{{
				SQLType:    "int",
				WordListID: "some",
				value:      genInt32,
				wordList:   wordMap["some"],
				Seed:       999,
				rand:       rand.New(rand.NewSource(999)),
			}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &Table{
				Columns: tt.Columns,
			}
			if err := tb.prepareColumns(); (err != nil) != tt.wantErr {
				t.Fatalf("Table.prepareColumns() error = %v, wantErr %v", err, tt.wantErr)
			}

			if len(tt.want) != len(tb.Columns) {
				t.Fatalf("Table.prepareColumns() columns = \n%v\nwant\n%v", tb.Columns, tt.want)
			}

			for i, col := range tb.Columns {
				if !tt.want[i].value.eq(col.value) {
					t.Errorf("Table.prepareColumns() columns = \n%v\nwant\n%v", col, tt.want[i])
				}

				// Functions are only deeply equal if nil
				col.value, tt.want[i].value = nil, nil
				if !reflect.DeepEqual(tt.want[i], col) {
					t.Errorf("Table.prepareColumns() columns = \n%v\nwant\n%v", col, tt.want[i])
				}
			}
		})
	}
}

func TestTable_insert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tb := &Table{
		Name:   "test_table",
		Amount: 3,
		Columns: []*Column{
			{
				Name:    "first",
				SQLType: "foo",
			},
		},
	}

	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		mock.ExpectRollback()
		t.Logf("Rollback: %v", tx.Rollback())
	}()

	t.Log("SQLType error")
	if err = tb.insert(ctx, tx); err == nil {
		t.Errorf("Table.insert() error = %v, wantErr %v", err, true)
	}

	tb.Columns = []*Column{
		{
			Name:    "first",
			SQLType: "int",
		},
		{
			Name:    "second",
			SQLType: "bigint",
		},
	}

	query := "insert into test_table (first, second) values ($1, $2);"
	t.Log("Expected succes")
	ep := mock.ExpectPrepare(query)
	for i := 1; i <= tb.Amount; i++ {
		ep.ExpectExec().WithArgs(i, i).WillReturnResult(sqlmock.NewResult(0, 1))
	}

	if err = tb.insert(ctx, tx); err != nil {
		t.Fatal(err)
	}

	t.Log("Prepare error")
	mock.ExpectPrepare(query).WillReturnError(errors.New("TE"))

	if err = tb.insert(ctx, tx); err == nil || err.Error() != "TE" {
		t.Errorf("Table.insert() error = %v, wantErr %v", err, "TE")
	}

	t.Log("Exec error")
	ep = mock.ExpectPrepare(query)
	ep.ExpectExec().WithArgs(4, 4).WillReturnError(errors.New("TE2"))

	if err = tb.insert(ctx, tx); err == nil || err.Error() != "table.insert on test_table, row 0: TE2" {
		t.Errorf("Table.insert() error = %v, wantErr %v", err, "table.insert on test_table, row 0: TE2")
	}
}

func TestMock(t *testing.T) {
	qm := sqlmock.QueryMatcherFunc(func(a, b string) error {
		if strings.Trim(a, " ") != strings.Trim(b, " ") {
			return fmt.Errorf(`actual sql: "%s" does not equal to expected "%s"`, a, b)
		}
		return nil
	})

	query := "insert into test_table (first, second) values ($1, $2)"
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(qm))
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec(query).WillReturnResult(driver.ResultNoRows)
	if _, err := db.Exec(query); err != nil {
		t.Error(err)
	}
	mock.ExpectClose()
	db.Close()

}

func Test_loadList(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		want     []string
		wantErr  bool
	}{
		{
			"Success",
			"tests/short.txt",
			[]string{"Lorem", "ipsum", "dolor"},
			false,
		},
		{
			"Error",
			"foo",
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadList(tt.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadList() = %v, want %v", got, tt.want)
			}
		})
	}
}

var exampleSchema = &Schema{
	Driver:         Sqlite,
	DataSourceName: "file:test.db",
	WordLists: map[string]string{
		"lorem": "tests/lorem.txt",
	},
	Tables: []Table{
		{
			Name:   "categories",
			Amount: 4,
			Columns: []*Column{
				{
					Name:    "id",
					SQLType: "int",
				},
				{
					Name:       "label",
					SQLType:    "text",
					WordListID: "lorem",
					Seed:       1,
					Min:        1,
					Max:        2,
				},
			},
		},
		{
			Name:   "articles",
			Amount: 1000,
			Columns: []*Column{
				{
					Name:    "id",
					SQLType: "bigint",
				},
				{
					Name:    "created",
					SQLType: "timestamp",
					Seed:    3,
					Min:     1580000000,
					Max:     1580604800,
				},
				{
					Name:       "title",
					SQLType:    "text",
					WordListID: "lorem",
					Seed:       4,
					Min:        2,
					Max:        10,
				},
				{
					Name:       "description",
					SQLType:    "text",
					Null:       true,
					WordListID: "lorem",
					Max:        200,
				},
				{
					Name:    "category_id",
					SQLType: "int",
					Seed:    5,
					Min:     1,
					Max:     4,
				},
			},
		},
	},
}

func TestLoadSchema(t *testing.T) {
	js, err := json.MarshalIndent(exampleSchema, "", "   ")
	if err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile("example.json", js, 0644); err != nil {
		t.Fatal(err)
	}

	schema, err := loadSchema("example.json")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(exampleSchema, schema) {
		t.Errorf("loadSchema() schema = \n%v\nwant\n%v", schema, exampleSchema)
	}

	if _, err = loadSchema("foo"); err == nil {
		t.Errorf("loadSchema() error = %v, wantErr %v", err, true)
	}
	if _, err = loadSchema("tests/invalid.json"); err == nil {
		t.Errorf("loadSchema() error = %v, wantErr %v", err, true)
	}
	if _, err = loadSchema("tests/invalid_wordlist.json"); err == nil {
		t.Errorf("loadSchema() error = %v, wantErr %v", err, true)
	}
}

func TestUnitRun(t *testing.T) {
	schemaFile = "foo"
	if ret := run(); ret != 2 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 2)
	}

	schemaFile = "tests/invalid_driver.json"
	if ret := run(); ret != 2 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 2)
	}

	schemaFile = "tests/tx_fail.json"
	if ret := run(); ret != 2 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 2)
	}

	schemaFile = "tests/tx_fail2.json"
	if ret := run(); ret != 2 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 2)
	}
}
