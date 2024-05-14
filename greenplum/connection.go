// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

type Connection struct {
	URI string
	DB  *sql.DB `json:"-"`
	Pool *pgxpool.Pool `json:"-"`
	NumConns int32
}

func NewConnection(connURI string, numConns int32) (*Connection, error) {
	
	config, err := pgxpool.ParseConfig(connURI)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(context.TODO(), config)
	if err != nil {
		return nil, err
	}

	db := stdlib.OpenDBFromPool(pool)

	return &Connection{
		URI: connURI,
		DB: db,
		Pool: pool,
		NumConns: numConns,
	}, nil
}

func (c *Connection) Close() error {
	if c.Pool != nil {
		c.Pool.Close()
		c.Pool = nil
		return c.DB.Close()
	}
	return nil
}

func URI(options ...Option) string {
	o := newOptionList(options...)
	return fmt.Sprintf("postgresql://localhost:%d/%s?search_path=%s", o.port, o.database, o.searchPath)
}

type Option func(*optionList)

// Port defaults to coordinator port
func Port(port int) Option {
	return func(options *optionList) {
		options.port = port
	}
}

// Database defaults to template1
func Database(database string) Option {
	return func(options *optionList) {
		options.database = database
	}
}

func UtilityMode() Option {
	return func(options *optionList) {
		options.utilityMode = true
	}
}

func AllowSystemTableMods() Option {
	return func(options *optionList) {
		options.allowSystemTableMods = true
	}
}

func NumConns(num int32) Option {
	return func(options *optionList) {
		options.numConns = num
	}
}

func SearchPath(path string) Option {
	return func(options *optionList) {
		options.searchPath = path
	}
}

type optionList struct {
	port                 int
	database             string
	utilityMode          bool
	allowSystemTableMods bool
	numConns             int32
	searchPath           string
}

func newOptionList(opts ...Option) *optionList {
	o := new(optionList)
	for _, option := range opts {
		option(o)
	}
	return o
}
