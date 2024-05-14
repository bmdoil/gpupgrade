// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

type Connection struct {
	URI string
	DB  *sql.DB
	Pool *pgxpool.Pool
	Options *optionList
}

func NewConnection(options ...Option) (*Connection, error) {
	opts := newOptionList(options...)
	var mode string

	if opts.port == 0 {
		return nil, fmt.Errorf("port is required to create a new connection")
	}

	database := "template1"
	if opts.database != "" {
		database = opts.database
	}

	searchPath := ""
	if opts.searchPath != "" {
		searchPath = opts.searchPath
	}

	if opts.utilityMode {
		mode += "&gp_session_role=utility"
	}

	if opts.allowSystemTableMods {
		mode += "&allow_system_table_mods=true"
	}

	connURI := fmt.Sprintf("postgresql://localhost:%d/%s?search_path=%s%s", database, opts.port, searchPath, mode)
	
	config, err := pgxpool.ParseConfig(connURI)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(nil, config)
	if err != nil {
		return nil, err
	}

	if opts.numConns > 0 {
		pool.Config().MaxConns = opts.numConns
	}

	db := stdlib.OpenDBFromPool(pool)

	return &Connection{
		URI: connURI,
		DB: db,
		Pool: pool,
		Options: opts,
	}, nil
}

func (c *Connection) Close() error {
	c.Pool.Close()
	c.Pool = nil
	return c.DB.Close()
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
