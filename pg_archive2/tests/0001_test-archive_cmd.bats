#!/usr/bin/env bats
# -*- mode: sh; -*-

load vars
load functions

_archive_cleanup ()
{
    ssh test-archive02 rm -f /archive/wals/000000010000000000000001
    ssh test-archive03 rm -f /archive/wals/000000010000000000000001
    ssh test-archive02 rm -fr /lib/init/rw/pg_recv_sb/master5/
    ssh test-archive02 rm -f /archive/LASTLOG
    ssh test-archive03 rm -f /archive/LASTLOG
    rm -f /var/lib/postgresql/LASTLOG
    rm -f /tmp/send_wal_errors_cnt*
}

stat_wrong_size_mock ()
{
    if [[ $3 = '/tmp/pg_xlog/000000010000000000000001' ]]; then
        echo "*** DBG stat_wrong_size_mock: $@" >&3
        echo '2019-04-23 22:10:11.673849012 +0300|4096'
        return 0
    else
        command stat "$@"
    fi
}

stat_size_lost_mock ()
{
    if [[ $3 = '/tmp/pg_xlog/000000010000000000000001' ]]; then
        echo "*** DBG stat_size_lost_mock: $@" >&3
        echo '2019-04-23 22:10:11.673849012 +0300'
        return 0
    else
        command stat "$@"
    fi
}

# use /archive/wals for this tests, do not destroy /archive/logs.complete/
# it will be used in other tests
setup ()
{
    case "$BATS_TEST_DESCRIPTION" in
        "archive_cmd")
            # refresh IP in known_hosts, this remove future "Permanently added the ECDSA host key for IP address"
            # from test output
            ssh test-archive02 true > /dev/null 2>&1
            ssh test-archive02 ssh test-archive03 true > /dev/null 2>&1
            ssh test-archive03 true > /dev/null 2>&1
            # cleanup test stand
            _archive_cleanup
            ;;
        "archive_cmd, second archive unreachable")
            ;;
        "archive_cmd, skip already sent")
            ;;
        "archive_cmd, duplicate, same md5")
            ;;
        "archive_cmd, duplicate, different md5")
            mkdir /tmp/archive_status/
            dd conv=swab status=none if=/tmp/pg_xlog/000000010000000000000001 of=/tmp/000000010000000000000001
            md5sum /tmp/pg_xlog/000000010000000000000001 /tmp/000000010000000000000001 \
                | sed \
                      -e 's@/tmp/pg_xlog/000000010000000000000001@/tmp/unpacked@' \
                      -e 's@/tmp/000000010000000000000001@/lib/init/rw/pg_recv_sb/master5/000000010000000000000001.bad_md5@' \
                      > /tmp/md5
            ;;
        "archive_cmd, wrong size")
            _copy_function stat_wrong_size_mock stat
            export -f stat
            ;;
        "archive_cmd, size lost")
            _copy_function stat_size_lost_mock stat
            export -f stat
            ;;
        "archive_cmd, size zero")
            mkdir /tmp/archive_status/
            touch /tmp/000000010000000000000001
            ;;
    esac
}

teardown ()
{
    case "$BATS_TEST_DESCRIPTION" in
        "archive_cmd")
            _archive_cleanup
            ;;
        "archive_cmd, second archive unreachable")
            _archive_cleanup
            ;;
        "archive_cmd, skip already sent")
            _archive_cleanup
            ;;
        "archive_cmd, duplicate, same md5")
            _archive_cleanup
            ;;
        "archive_cmd, duplicate, different md5")
            _archive_cleanup
            rm -f /tmp/000000010000000000000001
            rmdir /tmp/archive_status/
            rm -f /tmp/md5
            ssh test-archive02 rm -f /tmp/unpacked
            ;;
        "archive_cmd, wrong size")
            _archive_cleanup
            unset -f stat
            ;;
        "archive_cmd, size lost")
            _archive_cleanup
            unset -f stat
            ;;
        "archive_cmd, size zero")
            _archive_cleanup
            rm -f /tmp/000000010000000000000001
            rmdir /tmp/archive_status/
            ;;
    esac
}


@test "archive_cmd" {
    # /tmp/pg_xlog saved in Dockerfile
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5

    local local=$(< /var/lib/postgresql/LASTLOG)
    local remote1=$(ssh test-archive02 cat /archive/LASTLOG)
    local remote2=$(ssh test-archive03 cat /archive/LASTLOG)

    echo "$output" >&2

    echo "local" >&2
    tree -Dspug /archive/ >&2

    echo "test-archive02" >&2
    ssh test-archive02 tree -Dspug /archive/ >&2

    echo "test-archive03" >&2
    ssh test-archive03 tree -Dspug /archive/ >&2

    echo "local: $local" >&2
    echo "remote1: $remote1" >&2
    echo "remote2: $remote2" >&2

    [[ $status -eq 0 ]]
    [[ ! $output ]]

    ssh test-archive02 test -f /archive/wals/000000010000000000000001
    ssh test-archive03 test -f /archive/wals/000000010000000000000001

    [[ $local   = '000000010000000000000001' ]]
    [[ $remote1 = '000000010000000000000001' ]]
    [[ $remote2 = '000000010000000000000001' ]]
}

@test "archive_cmd, second archive unreachable" {
    run /app/archive_cmd_2 'test-archive02 test-UNREACHABLE-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    ssh test-archive02 test -f /archive/wals/000000010000000000000001
    [[ ${lines[-1]} = *' can'\''t sync file '\''000000010000000000000001'\'' to host '\''test-UNREACHABLE-archive03'\' ]]
}

@test "archive_cmd, skip already sent" {
    run /app/archive_cmd_2 'test-archive02 ""' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5
    run /app/archive_cmd_2 'test-archive02 ""' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    ssh test-archive02 test -f /archive/wals/000000010000000000000001
    [[ ${lines[-1]} = 'File '\''000000010000000000000001'\'' was already sent to archive. Skipping...' ]]
}

@test "archive_cmd, duplicate, same md5" {
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5
    # force duplicate send
    rm -f /var/lib/postgresql/LASTLOG
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 0 ]]
    ssh test-archive02 test -f /archive/wals/000000010000000000000001
    [[ ${lines[-1]} = 'WARN: /archive/wals/000000010000000000000001 already exist with same md5' ]]
}

@test "archive_cmd, duplicate, different md5" {
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5
    # force duplicate send
    rm -f /var/lib/postgresql/LASTLOG
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/000000010000000000000001 000000010000000000000001 master5

    ssh test-archive02 'pbzip2 -d < /archive/wals/000000010000000000000001 > /tmp/unpacked'

    echo "$output" >&2
    echo "*** DBG ${lines[-2]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    echo "WAL local:" >&2
    ls -l /tmp/pg_xlog/000000010000000000000001 >&2
    echo "WAL remote:" >&2
    ssh test-archive02 ls -l /archive/wals/000000010000000000000001 /tmp/unpacked >&2
    echo "md5 local:" >&2
    md5sum                                      \
        /tmp/pg_xlog/000000010000000000000001   \
        /tmp/000000010000000000000001 >&2
    echo "md5 remote:" >&2
    ssh test-archive02 md5sum                                           \
        /tmp/unpacked                                                   \
        /lib/init/rw/pg_recv_sb/master5/000000010000000000000001.bad_md5    \
        /archive/wals/000000010000000000000001 >&2

    [[ $status -eq 1 ]]
    ssh test-archive02 test -f /archive/wals/000000010000000000000001
    ssh test-archive02 test -f /lib/init/rw/pg_recv_sb/master5/000000010000000000000001.bad_md5
    ssh test-archive02 md5sum -c < /tmp/md5 >&2
    [[ ${lines[-2]} = 'ERROR: /archive/wals/000000010000000000000001 already exist with different md5' ]]
    [[ ${lines[-1]} = "ERROR: can't send '000000010000000000000001' to archive host 'test-archive02'. Exit code: '1'" ]]
}

@test "archive_cmd, wrong size" {
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5

    echo "$output" >&2
    echo "*** DBG ${lines[-2]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    ssh test-archive02 test -f /lib/init/rw/pg_recv_sb/master5/000000010000000000000001.new.bad
    ssh test-archive02 test ! -e /archive/wals/000000010000000000000001
    [[ ${lines[-2]} = 'ERROR: /lib/init/rw/pg_recv_sb/master5/000000010000000000000001.new.bad size 16777216, expected 4096 bytes, cat exit 0' ]]
    [[ ${lines[-1]} = "ERROR: can't send '000000010000000000000001' to archive host 'test-archive02'. Exit code: '1'" ]]
}

@test "archive_cmd, size lost" {
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/pg_xlog/000000010000000000000001 000000010000000000000001 master5

    echo "$output" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    ssh test-archive02 test ! -e /archive/wals/000000010000000000000001
    [[ ${lines[-1]} = "ERROR: can't send '000000010000000000000001' to archive host 'test-archive02'. Exit code: '1'" ]]
}

@test "archive_cmd, size zero" {
    run /app/archive_cmd_2 'test-archive02 test-archive03' /archive/wals /tmp/000000010000000000000001 000000010000000000000001 master5

    echo "$output" >&2
    echo "*** DBG ${lines[-2]}" >&2
    echo "*** DBG ${lines[-1]}" >&2

    [[ $status -eq 1 ]]
    ssh test-archive02 test ! -e /archive/wals/000000010000000000000001
    [[ ${lines[-2]} = 'ERROR: /lib/init/rw/pg_recv_sb/master5/000000010000000000000001.new.bad size 0, expected 0 bytes, cat exit 0' ]]
    [[ ${lines[-1]} = "ERROR: can't send '000000010000000000000001' to archive host 'test-archive02'. Exit code: '1'" ]]
}
