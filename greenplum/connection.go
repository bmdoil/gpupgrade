// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/blang/semver/v4"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

var NewPoolerFunc = NewPooler

func SetNewPoolerFunc(command func(...Option) (Pooler, error)) {
	NewPoolerFunc = command
}

func ResetNewPoolerFunc() {
	NewPoolerFunc = NewPooler
}

type Pooler interface {
	Exec(sql string, args ...any) error
	Select(dest any, sql string, args ...any) error
	Close()
	ConnString() string
	Jobs() int
	Database() string
	Version() semver.Version
}

type Pool struct {
	*pgxpool.Pool
	database   string
	version    semver.Version
	jobs       int
	connString string
}

func NewPooler(options ...Option) (Pooler, error) {
	setGucsQuery := ""
	opts := newOptionList(options...)

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
	if opts.jobs > 0 {
		config.MaxConns = int32(opts.jobs)
	}

	// Open a transient connection to determine the version of the database
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
		if version.Major < 6 {
			setGucsQuery += "SET allow_system_table_mods=dml;\n"
		} else {
			setGucsQuery += "SET allow_system_table_mods=on;\n"
		}
	}

	// Add any GUC values for connections in the pool
	for _, guc := range opts.gucs {
		setGucsQuery += guc + "\n"
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, setGucsQuery)
		return err
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

	return &Pool{Pool: pool, database: database, version: version, jobs: int(config.MaxConns), connString: pool.Config().ConnString()}, nil
}

func (p *Pool) Exec(query string, args ...any) error {
	var err error
	if p.Pool == nil {
		return fmt.Errorf("pool is nil")
	}
	_, err = p.Pool.Exec(context.Background(), query, args...)
	return err
}

func (p *Pool) Select(dest any, query string, args ...any) error {
	rows, err := p.Query(context.Background(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	destVal := reflect.ValueOf(dest)
	if destVal.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to a slice")
	}
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer")
	}
	destVal = destVal.Elem()
	if destVal.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice")
	}
	destElems := destVal.Type().Elem()
	for rows.Next() {
		elem := reflect.New(destElems).Interface()
		err = rows.Scan(elem)
		if err != nil {
			return err
		}
		destVal.Set(reflect.Append(destVal, reflect.ValueOf(elem).Elem()))
	}

	return nil
}

func (p *Pool) Database() string {
	return p.database
}

func (p *Pool) Version() semver.Version {
	return p.version
}

func (p *Pool) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}

func (p *Pool) Jobs() int {
	return int(p.Config().MaxConns)
}

func (p *Pool) ConnString() string {
	return p.connString
}

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

func Jobs(jobs int) Option {
	return func(options *optionList) {
		options.jobs = jobs
	}
}

func Gucs(gucs []string) Option {
	return func(options *optionList) {
		options.gucs = gucs
	}
}

type optionList struct {
	port                 int
	database             string
	utilityMode          bool
	allowSystemTableMods bool
	jobs                 int
	gucs                 []string
}

func newOptionList(opts ...Option) *optionList {
	o := new(optionList)
	for _, option := range opts {
		option(o)
	}
	return o
}
