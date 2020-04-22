# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

# log() prints its arguments to the TAP stream. Newlines are supported (each
# line will be correctly escaped in TAP).
log() {
    while read -r line; do
        echo "# $line" 1>&3
    done <<< "$*"
}

# fail() is meant to be called from BATS tests. It will fail the current test
# after printing its arguments to the TAP stream.
fail() {
    log "$@"
    false
}

# abort() is meant to be called from BATS tests. It will exit the process after
# printing its arguments to the TAP stream.
abort() {
    log "fatal: $*"
    exit 1
}

# skip_if_no_gpdb() will skip a test if a cluster's environment is not set up.
skip_if_no_gpdb() {
    [ -n "${GPHOME}" ] || skip "this test requires an active GPDB cluster (set GPHOME)"
    [ -n "${PGPORT}" ] || skip "this test requires an active GPDB cluster (set PGPORT)"
}

# start_source_cluster() ensures that database is up before returning
start_source_cluster() {
    "${GPHOME}"/bin/pg_isready -q || "${GPHOME}"/bin/gpstart -a
}

# Calls gpdeletesystem on the cluster pointed to by the given master data
# directory.
delete_cluster() {
    local masterdir="$1"

    # NOTE: the target master datadir now looks something like this: qddir/demoDataDir.k9KuElo8HT8.-1

    # Sanity check.
    if [[ $masterdir != *qddir/demoDataDir*\.*\.-1* ]]; then
        abort "cowardly refusing to delete $masterdir which does not look like an upgraded demo data directory"
    fi

    # Look up the master port (fourth line of the postmaster PID file).
    local port=$(awk 'NR == 4 { print $0 }' < "$masterdir/postmaster.pid")

    local gpdeletesystem="$GPHOME"/bin/gpdeletesystem

    # XXX gpdeletesystem returns 1 if there are warnings. There are always
    # warnings. So we ignore the exit code...
    yes | PGPORT="$port" "$gpdeletesystem" -fd "$masterdir" || true

    # XXX The master datadir copy moves the datadirs to .old instead of
    # removing them. This causes gpupgrade to fail when copying the master
    # data directory to segments with "file exists".
    delete_target_datadirs "${masterdir}"
}

delete_finalized_cluster() {
    local masterdir="$1"

    # Sanity check.
    local id=$(gpupgrade config show --id)
    local old_qddir_path=$(dirname $masterdir)"/demoDataDir.${id}.-1.old"
    if [[ ! -d "$old_qddir_path" ]]; then
        abort "cowardly refusing to delete $masterdir. Expected $old_qddir_path to exist."
    fi

    # Look up the master port (fourth line of the postmaster PID file).
    local port=$(awk 'NR == 4 { print $0 }' < "$masterdir/postmaster.pid")

    local gpdeletesystem="$GPHOME"/bin/gpdeletesystem

    # XXX gpdeletesystem returns 1 if there are warnings. There are always
    # warnings. So we ignore the exit code...
    yes | PGPORT="$port" "$gpdeletesystem" -fd "$masterdir" || true

    # put source directories back into place
    local datadirs=$(dirname "$(dirname "$masterdir")")
    for archive in $(find "${datadirs}" -name "*${id}*.old"); do
        # The following sed matches archived data directories and returns the
        # path of the original directory. For example,
        #   /dbfast_mirror2/demoDataDir.BY6l9U0LfX8.1.old -> /dbfast_mirror2/demoDataDir1
        #   /datadirs/standby.BY6l9U0LfX8.old -> /datadirs/standby
        local original=$(sed -E 's/\.'"${id}"'(\.([-0-9]+))?\.old/\2/' <<< "$archive")
        rm -rf "${original}"
        mv "$archive" "$original"
    done
}

delete_target_datadirs() {
    local masterdir="$1"
    local datadir=$(dirname "$(dirname "$masterdir")")

    rm -rf "${datadir}"/*/demoDataDir.*.[0-9]
}

# require_gnu_stat tries to find a GNU stat program. If one is found, it will be
# assigned to the STAT global variable; otherwise the current test is skipped.
require_gnu_stat() {
    if command -v gstat > /dev/null; then
        STAT=gstat
    elif command -v stat > /dev/null; then
        STAT=stat
    else
        skip "GNU stat is required for this test"
    fi

    # Check to make sure what we have is really GNU.
    local version=$($STAT --version || true)
    [[ $version = *"GNU coreutils"* ]] || skip "GNU stat is required for this test"
}

process_is_running() {
    ps -ef | grep -wGc "$1"
}

# Takes an original datadir and echoes the expected temporary datadir containing
# the upgradeID.
#
# NOTE for devs: this is just for getting the expected data directories, which
# is an implementation detail. If you want the actual location of the new master
# data directory after an initialization, you can just ask the hub with
#
#    gpupgrade config show --target-datadir
#
expected_target_datadir() {
    local dir=$1
    local parentDir=$(dirname "${dir}")
    local baseDir=$(basename "${dir}")
    local suffix="${baseDir#demoDataDir}"

    local upgradeID
    upgradeID=$(gpupgrade config show --id)

    # Sanity check.
    [ -n "$parentDir" ]

    if [ "${baseDir}" == "standby" ]; then
        echo "${parentDir}/${baseDir}.${upgradeID}"
        return
    fi

    echo "${parentDir}/demoDataDir.${upgradeID}.${suffix}"
}

# archive_dir echoes the expected archive directory given an original data
# directory.
archive_dir() {
    local dir=$1
    echo "$(expected_target_datadir "$dir")".old
}
