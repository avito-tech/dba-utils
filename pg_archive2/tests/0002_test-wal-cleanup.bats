#!/usr/bin/env bats
# -*- mode: sh; -*-

load vars
load functions

_wals_list_before="000000010000000000000001
000000010000000000000002
000000010000000000000003
000000010000000000000004
000000010000000000000005
000000010000000000000006
000000010000000000000007"

_wals_list_after="000000010000000000000005
000000010000000000000006
000000010000000000000007"

_init_backup ()
{
    source /app/base-backup_2

    # overwrite vars
    load vars
    load functions

    _init_backup_task

    # create test backup, logs in /var/tmp/base-backup_test.log
    sub_main () {
        ( main ) > /dev/null
    }
    # subfunction for correct error report in bats
    sub_main

    # create more old files
    cp -u /tmp/pg_xlog/0000* /archive/logs.complete/
}

_cleanup_backup ()
{
    _cleanup_backup_task

    rm -f /archive/SUCCESS
    rm -f /archive/logs.complete/*
    rm -rf /archive/data.master.0
}

timeout_always_mock ()
{
    local res
    local mock_timeout=$((4 * 60 * 60))

    # echo "*** DBG 1: ${1}" >&2
    # for ((i = 20; i < 30; i++)); do
    #     echo "*** DBG $i: ${!i}" >&2
    # done

    if [[ $1 = ${mock_timeout} && ${26} == *"/pg_archivecleanup '/archive/logs.complete/' "* ]]; then
        echo "*** DBG timeout emulate long ssh call: $@" >&3
        command timeout 1 sleep 3
        res=$?
    else
        command timeout "$@"
        res=$?
    fi

    return $res
}

timeout_lock_mock ()
{
    local res
    local mock_timeout=$((4 * 60 * 60))
    local lock_startup_timeout=10

    if [[ $1 = ${mock_timeout} && ${26} == *"/pg_archivecleanup '/archive/logs.complete/' "* ]]; then
        if (( timeout_lock_mock_calls == 0 )); then
            echo "*** DBG timeout emulate long ssh call: $@" >&3
            command timeout 1 sleep 3
            res=$?
        elif (( timeout_lock_mock_calls == 1 )); then
            echo "*** DBG timeout emulate remote lock: $@" >&3

            coproc ssh {
                ssh test-archive03 \
                    flock -n /archive/wal-cleanup.lock \
                    bash -c "'echo locked; read -rt ${lock_startup_timeout};'"
            }
            # wait for flock started
            read -rt "${lock_startup_timeout}" -u "${ssh[0]}" line
            if [[ $line != 'locked' ]]; then
                echo "timeout_lock_mock: cannot create remote lock" >&3
                exit 255
            fi

            shift
            command timeout 10 "$@"
            res=$?

            echo done >&${ssh[1]} || true # ignore timeout
            wait $ssh_PID                 # no quote, ignore if pid empty
            jobs >&3
        fi
        (( timeout_lock_mock_calls++ ))
    else
        command timeout "$@"
        res=$?
    fi

    return $res
}

timeout_neterror_mock ()
{
    local res
    local mock_timeout=$((4 * 60 * 60))

    if [[ $1 = ${mock_timeout} && ${26} == *"/pg_archivecleanup '/archive/logs.complete/' "* ]]; then
        # timeout_neterror_mock pass to wal-cleanup via export
        # that's why the functions from functions.bash is not available here
        _argv_replace ()
        {
            local old=$1; shift
            local new=$1; shift
            local v

            for v in "$@"; do
                if [[ $v = "$old" ]]; then
                    v=$new
                fi
                # echo eat args like -n
                printf "%s\n" "$v"
            done
        }

        # remove timeout
        shift
        # replace hostname
        set -- $(_argv_replace test-archive03 test-UNREACHABLE-archive03 "$@")
        echo "*** DBG timeout emulate ssh network error: $@" >&3

        command timeout 10 "$@"
        res=$?
    else
        command timeout "$@"
        res=$?
    fi

    return $res
}

setup ()
{
    case "$BATS_TEST_DESCRIPTION" in
        "wal-cleanup")
            _init_backup

            rsync -av /archive/logs.complete/ test-archive03:/archive/logs.complete/ >&3
            rsync -av /archive/data.master.0 test-archive03:/archive/ >&3

            cat > /archive/data.master.0/backup_label <<EOF
START WAL LOCATION: 0/5000028 (file 000000010000000000000005)
CHECKPOINT LOCATION: 0/5000060
BACKUP METHOD: streamed
BACKUP FROM: master
START TIME: 2019-04-20 01:26:44 UTC
LABEL: pg_basebackup base backup
EOF
            ssh test-archive03 'cat > /archive/data.master.0/backup_label' <<EOF
START WAL LOCATION: 0/6000028 (file 000000010000000000000006)
CHECKPOINT LOCATION: 0/6000060
BACKUP METHOD: streamed
BACKUP FROM: master
START TIME: 2019-04-20 01:26:44 UTC
LABEL: pg_basebackup base backup
EOF
            ;;
        "wal-cleanup, remote timeout")
            _init_backup

            # mock timeout command
            # _copy_function timeout timeout_orig
            _copy_function timeout_always_mock timeout
            export -f timeout
            ;;
        "wal-cleanup, remote timeout, lock")
            _init_backup

            timeout_lock_mock_calls=0
            _copy_function timeout_lock_mock timeout
            export -f timeout
            ;;
        "wal-cleanup, remote network error")
            _init_backup

            _copy_function timeout_neterror_mock timeout
            export -f timeout
            ;;
        "wal-cleanup, local locked")
            ;;
        "wal-cleanup, local lock access error")
            ;;
    esac
}

teardown ()
{
    case "$BATS_TEST_DESCRIPTION" in
        "wal-cleanup")
            _cleanup_backup

            ssh test-archive03 rm -f /archive/logs.complete/*
            ssh test-archive03 rm -rf /archive/data.master.0
            ;;
        "wal-cleanup, remote timeout")
            _cleanup_backup

            # restore timeout command
            # _copy_function timeout_orig timeout
            # declare -f +x timeout
            unset -f timeout
            ;;
        "wal-cleanup, remote timeout, lock")
            _cleanup_backup

            unset -f timeout
            ;;
        "wal-cleanup, remote network error")
            _cleanup_backup
            ;;
        "wal-cleanup, local locked")
            ;;
        "wal-cleanup, local lock access error")
            ;;
    esac
}


@test "wal-cleanup" {
    # sanity check
    out=$(ls /archive/logs.complete/ | head -7); echo "$out" >&2
    [[ $out = "$_wals_list_before" ]]
    out=$(ssh test-archive03 ls /archive/logs.complete/ | head -7); echo "$out" >&2
    [[ $out = "$_wals_list_before" ]]

    cd /archive/
    run /app/wal-cleanup_2 data.master.0/ logs.complete/ test-archive03

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    [[ ${lines[-1]} = *'done wal-cleanup: '\''data.master.0/'\'' '\''logs.complete/'\' ]]
    out=$(ls /archive/logs.complete/ | head -3); echo "$out" >&2
    [[ $out = "$_wals_list_after" ]]
    out=$(ssh test-archive03 ls /archive/logs.complete/ | head -3); echo "$out" >&2
    [[ $out = "$_wals_list_after" ]]
}

@test "wal-cleanup, remote timeout" {
    cd /archive/
    run /app/wal-cleanup_2 data.master.0/ logs.complete/ test-archive03

    echo "$output" >&2
    echo "*** DBG ${lines[-4]}" >&2
    echo "*** DBG ${lines[-2]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    [[ ${lines[-4]} = *'REMOTE pg_archivecleanup timeout, retrying (1 of 1)' ]]
    [[ ${lines[-2]} = *'REMOTE pg_archivecleanup timeout, abort' ]]
    [[ ${lines[-1]} = *'done wal-cleanup: '\''data.master.0/'\'' '\''logs.complete/'\' ]]
}

@test "wal-cleanup, remote timeout, lock" {
    cd /archive/
    run /app/wal-cleanup_2 data.master.0/ logs.complete/ test-archive03

    echo "$output" >&2
    echo "*** DBG ${lines[-3]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    [[ ${lines[-3]} = *'REMOTE pg_archivecleanup timeout, retrying (1 of 1)' ]]
    [[ ${lines[-1]} = *'REMOTE pg_archivecleanup still running, abort' ]]
}

@test "wal-cleanup, remote network error" {
    cd /archive/
    run /app/wal-cleanup_2 data.master.0/ logs.complete/ test-archive03

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    [[ ${lines[-1]} = *'error with REMOTE pg_archivecleanup, abort' ]]
}

@test "wal-cleanup, local locked" {
    local lock_startup_timeout=10

    coproc lock {
        flock -n /archive/wal-cleanup.lock \
              bash -c "echo locked; read -rt ${lock_startup_timeout};"
    }
    # wait for flock started
    read -rt "${lock_startup_timeout}" -u "${lock[0]}" line
    if [[ $line != 'locked' ]]; then
        echo "'wal-cleanup, local locked': cannot start lock" >&2
        exit 255
    fi

    cd /archive/
    run /app/wal-cleanup_2 data.master.0/ logs.complete/ test-archive03

    echo done >&${lock[1]} || true # ignore timeout
    wait $lock_PID                 # no quote, ignore if pid empty
    jobs >&3

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    [[ ${lines[-1]} = *'lock file '\''/archive/wal-cleanup.lock'\'' from '*' locked, abort' ]]
}

@test "wal-cleanup, local lock access error" {
    cd /archive/
    run /app/wal-cleanup_2 data.master.0/ /tmp/ test-archive03

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    [[ ${lines[-1]} = *' /wal-cleanup.lock: Permission denied' ]]
}
