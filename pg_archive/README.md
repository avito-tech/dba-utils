General description
=================

* archive_cmd — executes on primary, archives and sends WAL with the help of ssh
* archive_remote_cmd — synchronously called on archive server from primary and receives WAL
* restore_cmd — executes on standby (recovery.conf) for receiving the WAL for replay

### Examples

```sh
archive_command = '/usr/local/bin/archive_cmd arch-host /mnt/nfs/wals_logs %p %f'
restore_command = '/usr/local/bin/restore_cmd /mnt/nfs/wals_logs %f %p'

# two-day (172800 seconds) delay of replaying the WAL from archive
restore_command = '/usr/local/bin/restore_cmd /mnt/nfs/wals_logs %f %p 172800'
```
