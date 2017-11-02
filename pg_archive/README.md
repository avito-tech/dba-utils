Архивирование WAL
=================

* archive_cmd — вызывается на мастере, архивирует и отправляет WAL через ssh
* archive_remote_cmd — вызывается синхронно с мастера на сервере архива и принимает WAL
* restore_cmd — вызывается на standby (recovery.conf) для получения WAL

### Примеры использования

```sh
archive_command = '/usr/local/bin/archive_cmd arch-host /mnt/nfs/wals_logs %p %f'
restore_command = '/usr/local/bin/restore_cmd /mnt/nfs/wals_logs %f %p'

# проигрывание из архива с задержкой в 2 дня (172800 секунд)
restore_command = '/usr/local/bin/restore_cmd /mnt/nfs/wals_logs %f %p 172800'
```
