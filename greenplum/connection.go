// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"context"
	"fmt"
	"log"

	"github.com/greenplum-db/gpupgrade/utils/errorlist"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func (c *Cluster) Connection(options ...Option) string {
	opts := newOptionList(options...)

	port := c.CoordinatorPort()
	if opts.port > 0 {
		port = opts.port
	}

	database := "template1"
	if opts.database != "" {
		database = opts.database
	}

	connURI := fmt.Sprintf("postgresql://localhost:%d/%s?search_path=", port, database)

	if opts.utilityMode {
		mode := "&gp_role=utility"
		if c.Version.Major < 7 {
			mode = "&gp_session_role=utility"
		}

		connURI += mode
	}

	if opts.allowSystemTableMods {
		connURI += "&allow_system_table_mods=true"
	}

	log.Printf("connecting to %s cluster with: %q", c.Destination, connURI)
	return connURI
}

func NewPool(options ...Option) (*pgxpool.Pool, error) {
	opts := newOptionList(options...)

	if opts.port <= 0 {
		return nil, fmt.Errorf("port must be set")
	}

	database := "template1"
	if opts.database != "" {
		database = opts.database
	}

	connURI := fmt.Sprintf("postgresql://localhost:%d/%s", opts.port, database)

	if opts.utilityMode {
		connURI += "&gp_session_role=utility"
	}

	config, err := pgxpool.ParseConfig(connURI)
	if err != nil {
		return nil, err
	}

	config.MaxConns = 1
	if opts.numConns > 0 {
		config.MaxConns = int32(opts.numConns)
	}

	db := stdlib.OpenDB(*config.ConnConfig)
	defer func() {
		if cErr := db.Close(); cErr != nil {
			err = errorlist.Append(err, cErr)
		}
	}()

	version, err := Version(db)
	if err != nil {
		return nil, err
	}

	if opts.allowSystemTableMods {
		var val string
		if version.Major < 6 {
			val = "dml"
		} else {
			val = "on"
		}
		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, fmt.Sprintf("SET allow_system_table_mods = %s", val))
			return err
		}
		if err != nil {
			return nil, err
		}

	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	// Acquire a connection to ensure they can be established
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	conn.Release()

	return pool, nil
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

func NumConns(numConns int) Option {
	return func(options *optionList) {
		options.numConns = numConns
	}
}

type optionList struct {
	port                 int
	database             string
	utilityMode          bool
	allowSystemTableMods bool
	numConns             int
}

func newOptionList(opts ...Option) *optionList {
	o := new(optionList)
	for _, option := range opts {
		option(o)
	}
	return o
}
