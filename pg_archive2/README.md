# General description

Archive is an inherent part of resilient PostgreSQL infrastructure.

This component is responsible for:

- sending, receiving, storing, and rotating the WAL files.
- backup tasks execution, queue with backup tasks, rotation of backups (validation of backup is not the part of Archive 2.0 and it is a standalone solution working on a different infrastructure).
- PITR, recovery from backup (indirectly)
- replication (As for Avito it is the majority of PostgreSQL clusters, apart from those using synchronous replication)

One of the special features/advantages of Archive 2.0 comparing to other archive solutions is that it stores WAL files on two archive-servers simultaneously.

The archive infrastructure continues working If one of two archive servers becomes unreachable/unavailable. The gaps in WAL files will be filled with the help of syncing-WAL procedure, that is executed after each backup.

Backups are made in turn on both archive servers with the help of pg_basebackup. Thus there is a window for PITR (point in time recovery), the size of the recovery interval is set in a cluster settings. 

Log-shipping replication (without streaming) guarantees that archive is always in future in relation to standby, it excludes loss of the data needed for PITR (gaps in WAL).

# Components

- **Two archive servers** store the WAL files and backups.
- **archive_cmd2** (archive command is set in postgresql.conf) transfers WAL and other files to either of the archive servers.
- **archive_remote_cmd_2** - program which is executed on archive servers side, it gets the WALs, compress it and transfers them to the reserve archive server.
- **restore_cmd_2** (recovery.conf) with its help standby gets WAL from archive servers.
- **backup_queue** - database that stores backup schedule, backup settings and statuses for all backup tasks.
- **base-backup_2** - program scheduled with cron (e.g. every 10 minutes). Checks if there is a backup task in the queue and executes it. It consists of: 
  - **base-backup_2** - backup itself;
  - **wal-cleanup_2** - cleaning of unnecessary WAL. The backup for them has been rotated (there is no backup to use these WAL files).
  - **wal-sync** - bidirectional synchronisation/merge of archive servers.
- **monitoring** - at least there should be monitoring of the backup queue and alerts for failed backup tasks.
 

# Examples

Example of the archive_command setting:

```sh
archive_command = '/usr/local/bin/archive_cmd_2 \'hostname_archive_1 hostname_archive_2\' /storage/archive_directory/ %p %f cluster_name' 
```

Parameters description:
```sh
 DST-HOSTNAMES              - two archive host names in single quotes
 DST-DIR                    - archive directory for WALs (ssh path)
 SRC-WAL-FILENAME-WITH-PATH - %p (file name with path)
 SRC-WAL-FILENAME           - %f (file name)
 CLUSTER-NAME               - unique cluster name
```
Pay attention to DST-DIR - it is the path on the archive server.


*archive_cmd_2* transfers WAL’s and other files to both of the archive hosts in the following way:
```sh
[ master: archive_cmd ] -> [ archive1: archive_remote_cmd ] -> [ archive2: scp ]
```
*archive_cmd_2* transfers to the first DST-HOSTNAME, and then to the second one with the help of archive_remote_cmd_2 on remote host.

*archive_remote_cmd*  writes WAL locally, compress it and tries to transfer the WAL to the SYNC-HOST.

If *archive_cmd_2* can’t transfer the WAL to the first host (from parameters), then after N retries it starts to transfer WAL only to the second host during N seconds (cooldown_time):
```sh
retry_count="6"     # see below
cooldown_time="600" # do not try to send file to $dst_host1 for a '$cooldown_time' seconds after '$retry_count' attempts
```

For /etc/postgresql-common/compress.mime.mgc   compress.mime is used where only compressing signatures are left in order to exclude false positive runs (on other file types).
