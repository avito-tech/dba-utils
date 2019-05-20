#!/bin/bash

_psql ()
{
    command psql -h ${BACKUP_DB_HOST} -X -At -Ppager=off -vON_ERROR_STOP=1 "$@"
}

_copy_function ()
{
    local from_name=$1
    local to_name=$2
    #
    # https://mharrison.org/post/bashfunctionoverride/
    #
    local orig_func=$(declare -f "$from_name")
    local newname_func="${to_name}${orig_func#$from_name}"
    eval "$newname_func"
}

_init_backup_task ()
{
    _psql -d test -f - <<'EOF'
insert into backups.hosts (host, cluster_name, archiver_name, keep_backups_cnt, periodicity_days, directory)
values ('test', 'master5', '{"test", "test-archive02"}', 2, 4,  '/archive/');
EOF
}

_cleanup_backup_task ()
{
    _psql -d test -f - <<'EOF'
truncate backups.hosts, backups.tasks restart identity;
EOF
}
