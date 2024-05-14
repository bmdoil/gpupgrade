// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

var NewConnectionFunc = NewConnection

// XXX: for internal testing only
func SetNewConnectionFunction(connectionFunc func(connURI string, numConns int32) (*Connection, error)) {
	NewConnectionFunc = connectionFunc
}

// XXX: for internal testing only
func ResetNewConnectionFunction() {
	NewConnectionFunc = NewConnection
}

func MustCreateMockCluster(t *testing.T, segments SegConfigs) (*Cluster, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("couldn't create sqlmock: %v", err)
	}

	cluster, err := NewCluster(segments)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	conn := &Connection{DB: db}
	SetNewConnectionFunction(func(connURI string, numConns int32) (*Connection, error) {
		return &Connection{URI: connURI, DB: db, Pool: nil, NumConns: numConns}, nil
	})

	cluster.Connection = conn

	return &cluster, mock
}

func MustCreateCluster(t *testing.T, segments SegConfigs) *Cluster {
	t.Helper()

	cluster, err := NewCluster(segments)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	return &cluster
}