// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/blang/semver/v4"

	"github.com/greenplum-db/gpupgrade/config"
	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/upgrade"
)

func TestAssignDataDirsAndPorts(t *testing.T) {
	var upgradeID string

	expectedDataDir := func(sourceDir string) string {
		return upgrade.TempDataDir(sourceDir, "seg", upgradeID)
	}

	cases := []struct {
		name string

		cluster  *greenplum.Cluster
		ports    []int
		expected *greenplum.Cluster
	}{{
		name: "sorts and deduplicates provided port range",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
		}),
		ports: []int{10, 9, 10, 9, 10, 8},
		expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 8},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 9},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: greenplum.PrimaryRole, Port: 10},
		}),
	},
		{
			name: "gives coordinator its own port regardless of host layout",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
				{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
				{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast3/seg3", Role: greenplum.PrimaryRole},
			}),
			ports: []int{50432, 50433, 50434, 50435, 50436, 50437, 50438, 50439, 50440},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 50432},
				{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 50433},
				{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: greenplum.PrimaryRole, Port: 50434},
				{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast3/seg3"), Role: greenplum.PrimaryRole, Port: 50435},
			}),
		}, {
			name: "when using default ports, it sets up mirrors as expected in the InitializeConfig",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
				{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
				{ContentID: 0, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1/seg1", Role: greenplum.MirrorRole},
				{ContentID: 1, DbID: 5, Hostname: "sdw1", DataDir: "/data/dbfast_mirror2/seg2", Role: greenplum.MirrorRole},
			}),
			ports: []int{50432, 50433, 50434, 50435, 50436, 50437, 50438, 50439, 50440},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 50432},
				{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 50433},
				{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: greenplum.PrimaryRole, Port: 50434},
				{ContentID: 0, DbID: 4, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast_mirror1/seg1"), Role: greenplum.MirrorRole, Port: 50435},
				{ContentID: 1, DbID: 5, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast_mirror2/seg2"), Role: greenplum.MirrorRole, Port: 50436},
			}),
		}, {
			name: "provides a standby port",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: "/data/standby", Role: greenplum.MirrorRole},
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			}),
			ports: []int{50432, 50433, 50434, 50435, 50436, 50437, 50438, 50439, 50440},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 50432},
				{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: expectedDataDir("/data/standby"), Role: greenplum.MirrorRole, Port: 50433},
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 50434},
			}),
		}, {
			name: "deals with coordinator and standby on the same host",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: greenplum.MirrorRole},
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			}),
			ports: []int{50432, 50433, 50434, 50435, 50436, 50437, 50438, 50439, 50440},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 50432},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: greenplum.MirrorRole, Port: 50433},
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 50434},
			}),
		}, {
			name: "deals with coordinator and standby on the same host as other segments",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: greenplum.MirrorRole},
				{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			}),
			ports: []int{50432, 50433, 50434, 50435, 50436, 50437, 50438, 50439, 50440},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 50432},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: greenplum.MirrorRole, Port: 50433},
				{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 50434},
			}),
		}, {
			name: "assigns provided ports to the standby",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: greenplum.MirrorRole},
				{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			}),
			ports: []int{1, 2, 3},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 1},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: greenplum.MirrorRole, Port: 2},
				{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 3},
			}),
		}, {
			name: "assigns provided ports to cluster with standby and multiple primaries and multiple mirrors",
			cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: greenplum.MirrorRole},
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
				{ContentID: 1, DbID: 4, Hostname: "sdw2", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
				{ContentID: 2, DbID: 5, Hostname: "sdw3", DataDir: "/data/dbfast3/seg3", Role: greenplum.PrimaryRole},
				{ContentID: 0, DbID: 6, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg1", Role: greenplum.MirrorRole},
				{ContentID: 1, DbID: 7, Hostname: "sdw3", DataDir: "/data/dbfast_mirror2/seg2", Role: greenplum.MirrorRole},
				{ContentID: 2, DbID: 8, Hostname: "sdw1", DataDir: "/data/dbfast_mirror3/seg3", Role: greenplum.MirrorRole},
			}),
			ports: []int{1, 2, 3, 4, 5},
			expected: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: greenplum.PrimaryRole, Port: 1},
				{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: greenplum.MirrorRole, Port: 2},
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: greenplum.PrimaryRole, Port: 3},
				{ContentID: 1, DbID: 4, Hostname: "sdw2", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: greenplum.PrimaryRole, Port: 3},
				{ContentID: 2, DbID: 5, Hostname: "sdw3", DataDir: expectedDataDir("/data/dbfast3/seg3"), Role: greenplum.PrimaryRole, Port: 3},
				{ContentID: 0, DbID: 6, Hostname: "sdw2", DataDir: expectedDataDir("/data/dbfast_mirror1/seg1"), Role: greenplum.MirrorRole, Port: 4},
				{ContentID: 1, DbID: 7, Hostname: "sdw3", DataDir: expectedDataDir("/data/dbfast_mirror2/seg2"), Role: greenplum.MirrorRole, Port: 4},
				{ContentID: 2, DbID: 8, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast_mirror3/seg3"), Role: greenplum.MirrorRole, Port: 4},
			}),
		}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, err := config.GenerateIntermediateCluster(c.cluster, c.ports, upgradeID, semver.Version{}, "")
			if err != nil {
				t.Errorf("returned error %+v", err)
			}

			c.expected.Destination = idl.ClusterDestination_intermediate
			if !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("GenerateIntermediateCluster(<cluster>, %v)=%v, want %v", c.ports, actual, c.expected)
			}
		})
	}

	errCases := []struct {
		name string

		cluster *greenplum.Cluster
		ports   []int
	}{{
		name: "errors when not given enough ports (single host)",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
		}),
		ports: []int{15433},
	}, {
		name: "errors when not given enough ports (multiple hosts)",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
		}),
		ports: []int{15433, 25432},
	}, {
		name: "errors when not given enough unique ports",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
		}),
		ports: []int{15433, 15433, 15433},
	}, {
		name: "errors when not given enough unique ports with a standby",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole},
			{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
		}),
		ports: []int{15433, 15434},
	}, {
		name: "errors when not given enough unique ports with a standby on a different host",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
		}),
		ports: []int{15433, 15434},
	}, {
		name: "errors when there are not enough ports for the mirrors",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1/seg1", Role: greenplum.MirrorRole},
		}),
		ports: []int{15433, 15434},
	}, {
		// regression case
		name: "doesn't panic when not given enough unique ports with a standby on a different host",
		cluster: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole},
			{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
		}),
		ports: []int{15433},
	}}

	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := config.GenerateIntermediateCluster(c.cluster, c.ports, "0", semver.Version{}, "")
			if err == nil {
				t.Errorf("GenerateIntermediateCluster(<cluster>, %v) returned nil, want error", c.ports)
			}
		})
	}
}

func TestEnsureTempPortRangeDoesNotOverlapWithSourceClusterPorts(t *testing.T) {
	source := greenplum.MustCreateCluster(t, greenplum.SegConfigs{
		{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 15432},
		{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 16432},
		{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 25432},
		{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 25435},
	})

	intermediate := greenplum.MustCreateCluster(t, greenplum.SegConfigs{
		{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 6000},
		{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 6001},
		{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 6002},
		{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 6005},
	})

	t.Run("EnsureTempPortRangeDoesNotOverlapWithSourceClusterPorts succeeds", func(t *testing.T) {
		err := config.EnsureTempPortRangeDoesNotOverlapWithSourceClusterPorts(source, intermediate)
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}
	})

	t.Run("allow the same port on different hosts", func(t *testing.T) {
		intermediate.Mirrors[-1] = greenplum.SegConfig{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: source.CoordinatorPort()}

		err := config.EnsureTempPortRangeDoesNotOverlapWithSourceClusterPorts(source, intermediate)
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}
	})

	errCases := []struct {
		name            string
		source          *greenplum.Cluster
		intermediate    *greenplum.Cluster
		conflictingPort int
	}{{
		name: "errors when source coordinator port overlaps with intermediate target cluster ports",
		source: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 15432},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 16432},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 25432},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 25435},
		}),
		intermediate: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 15432},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 6001},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 6002},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 6005},
		}),
		conflictingPort: 15432,
	}, {
		name: "errors when source standby port overlaps with intermediate target cluster ports",
		source: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 15432},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 16432},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 25432},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 25435},
		}),
		intermediate: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 6000},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 16432},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 6002},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 6005},
		}),
		conflictingPort: 16432,
	}, {
		name: "errors when source primary port overlaps with intermediate target cluster ports",
		source: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 15432},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 16432},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 25432},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 25435},
		}),
		intermediate: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 6000},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 6001},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 25432},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 6005},
		}),
		conflictingPort: 25432,
	}, {
		name: "errors when source mirror port overlaps with intermediate target cluster ports",
		source: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 15432},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 16432},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 25432},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 25435},
		}),
		intermediate: greenplum.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: greenplum.PrimaryRole, Port: 6000},
			{ContentID: -1, DbID: 8, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: greenplum.MirrorRole, Port: 6001},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: greenplum.PrimaryRole, Port: 6002},
			{ContentID: 0, DbID: 5, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: greenplum.MirrorRole, Port: 25435},
		}),
		conflictingPort: 25435,
	}}

	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			err := config.EnsureTempPortRangeDoesNotOverlapWithSourceClusterPorts(c.source, c.intermediate)
			var invalidPortErr *config.InvalidTempPortRangeError
			if !errors.As(err, &invalidPortErr) {
				t.Fatalf("got %T, want %T", err, invalidPortErr)
			}

			if invalidPortErr.ConflictingPort != c.conflictingPort {
				t.Errorf("got conflicting port %d, want %d", invalidPortErr.ConflictingPort, c.conflictingPort)
			}
		})
	}
}
