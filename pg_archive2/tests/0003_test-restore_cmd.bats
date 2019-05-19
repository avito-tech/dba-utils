#!/usr/bin/env bats
# -*- mode: sh; -*-

load vars
load functions

_init ()
{
    local h

    pbzip2 < /tmp/pg_xlog/000000010000000000000001 > /tmp/000000010000000000000001

    for h in test-archive02 test-archive03; do
        scp /tmp/000000010000000000000001 "$h":/archive/wals/000000010000000000000001
    done

    stat -c %s /tmp/000000010000000000000001
    rm /tmp/000000010000000000000001
}

_cleanup ()
{
    ssh test-archive02 rm -f /archive/wals/000000010000000000000001
    ssh test-archive03 rm -f /archive/wals/000000010000000000000001
    rm -f /lib/init/rw/pg_recv_sb/ssh-errors_*
    rm -f /tmp/000000010000000000000001
}

setup ()
{
    case "$BATS_TEST_DESCRIPTION" in
        "restore_cmd")
            _init
            ;;
        "restore_cmd, test-archive02 not exist")
            _init
            ssh test-archive02 rm -f /archive/wals/000000010000000000000001
            ;;
        "restore_cmd, test-archive02 corrupt")
            _init
            ssh test-archive02 dd conv=nocreat,notrunc bs=1 seek=1024 count=42 status=none \
                if=/dev/urandom of=/archive/wals/000000010000000000000001
            ;;
        "restore_cmd, test-archive02 unreachable, test-archive03 corrupt")
            _init
            ssh test-archive03 dd conv=nocreat,notrunc bs=1 seek=10240 count=42 status=none \
                if=/dev/urandom of=/archive/wals/000000010000000000000001
            ;;
        "restore_cmd, both corrupt")
            local size=$(_init)

            ssh test-archive02 truncate -s-42 /archive/wals/000000010000000000000001
            ssh test-archive03 dd conv=nocreat,notrunc bs=1 seek=$((size - 4242)) count=42 status=none \
                if=/dev/urandom of=/archive/wals/000000010000000000000001
            ;;
    esac
}

teardown ()
{
    case "$BATS_TEST_DESCRIPTION" in
        "restore_cmd") ;&
        "restore_cmd, test-archive02 not exist") ;&
        "restore_cmd, test-archive02 corrupt") ;&
        "restore_cmd, test-archive02 unreachable, test-archive03 corrupt") ;&
        "restore_cmd, both corrupt")
            _cleanup
            ;;
    esac
}


@test "restore_cmd" {
    run /app/restore_cmd_2 'test-archive02 test-archive03' /archive/wals 000000010000000000000001 /tmp/000000010000000000000001

    echo "$output" >&2

    [[ $status -eq 0 ]]
    [[ ! $output ]]
    test -f /tmp/000000010000000000000001
}

@test "restore_cmd, test-archive02 not exist" {
    run /app/restore_cmd_2 'test-archive02 test-archive03' /archive/wals 000000010000000000000001 /tmp/000000010000000000000001

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    test -f /tmp/000000010000000000000001
    [[ ${lines[-1]} = "WARNING: can't find wal '000000010000000000000001' at host 'test-archive02'" ]]
}

@test "restore_cmd, test-archive02 corrupt" {
    run /app/restore_cmd_2 'test-archive02 test-archive03' /archive/wals 000000010000000000000001 /tmp/000000010000000000000001

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    test -f /tmp/000000010000000000000001
    [[ ${lines[-1]} = "WARNING: decompress error from host test-archive02, trying other" ]]
}

@test "restore_cmd, test-archive02 unreachable, test-archive03 corrupt" {
    run /app/restore_cmd_2 'test-UNREACHABLE-archive02 test-archive03' /archive/wals 000000010000000000000001 /tmp/000000010000000000000001

    echo "$output" >&2
    echo "*** DBG ${lines[-5]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -ne 0 ]]
    test ! -f /tmp/000000010000000000000001
    [[ ${lines[-5]} = 'WARNING: decompress error from host test-archive03, trying other' ]]
    [[ ${lines[-1]} = "ERROR: can't fetch wal from all hosts: test-UNREACHABLE-archive02 test-archive03" ]]
}

@test "restore_cmd, both corrupt" {
    run /app/restore_cmd_2 'test-archive02 test-archive03' /archive/wals 000000010000000000000001 /tmp/000000010000000000000001

    echo "$output" >&2
    echo "*** DBG ${lines[-4]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -ne 0 ]]
    test ! -f /tmp/000000010000000000000001
    [[ ${lines[-4]} = 'WARNING: decompress error from host test-archive02, trying other' ]]
    [[ ${lines[-1]} = 'ERROR: cannot unpack wal from all hosts' ]]
}
