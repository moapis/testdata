// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

// +build integration

package main

import (
	"database/sql"
	"os"
	"testing"
)

const (
	createCategoriesQuery = `create table categories (
		id int,
		label varchar(50)
	);`
	createArticlesQuery = `create table articles (
		id bigint,
		created timestamp,
		title varchar(50),
		description varchar(2000),
		category_id int references categories (id)
	);`
)

func prepSqliteTest() error {
	os.Remove("test.db")

	db, err := sql.Open(string(exampleSchema.Driver), exampleSchema.DataSourceName)
	if err != nil {
		return err
	}

	if _, err := db.Exec(createCategoriesQuery); err != nil {
		return err
	}
	if _, err := db.Exec(createArticlesQuery); err != nil {
		return err
	}
	return nil
}

func TestRunSqlite(t *testing.T) {
	if err := prepSqliteTest(); err != nil {
		t.Fatal(err)
	}

	schemaFile = "example.json"
	if ret := run(); ret != 0 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 0)
	}
}
