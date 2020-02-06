// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"os"
	"testing"
)

const (
	dropCategoriesQuery   = "drop table if exists categories cascade;"
	createCategoriesQuery = `create table categories (
		id int primary key,
		label varchar(100)
	);`
	dropArticlesQuery   = "drop table if exists articles cascade;"
	createArticlesQuery = `create table articles (
		id bigint primary key,
		created timestamp,
		title varchar(100),
		description varchar(2000),
		category_id int references categories (id),
		published bool not null,
		price decimal null
	);`
)

func prepSqliteTest() error {
	os.Remove("test.db")

	db, err := exampleSchemas["sqlite"].connect()
	if err != nil {
		return err
	}
	defer db.Close()

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

	schemaFile = "example_sqlite.json"
	if ret := run(); ret != 0 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 0)
	}
}

func prepPqTest() error {
	db, err := exampleSchemas["pq"].connect()
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(dropArticlesQuery); err != nil {
		return err
	}
	if _, err := db.Exec(dropCategoriesQuery); err != nil {
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

func TestRunPq(t *testing.T) {
	if err := prepPqTest(); err != nil {
		t.Fatal(err)
	}

	schemaFile = "example_pq.json"
	if ret := run(); ret != 0 {
		t.Errorf("run() ret = %v, wantRet %v", ret, 0)
	}
}
