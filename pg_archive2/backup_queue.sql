create table backups.hosts (
  id serial primary key,
  host text not null,
  port int default 5432,
  cluster_name text not null,
  archiver_name text[2] not null,
  bareos_on text default 'archive_1',
  keep_backups_cnt text not null,
  periodicity_days int not null,
  directory text not null,
  last_archiver text,
  last_backup_id int,
  last_backup_start_txtime timestamptz default 'epoch',
  constraint host unique(host),
  constraint cluster_name  unique(cluster_name)
 );
ALTER TABLE backups.hosts
  OWNER TO postgres;

---

create table backups.tasks (
  backup_id serial primary key,
  host text not null,
  archiver_name text not null,
  start_txtime timestamptz,
  end_txtime timestamptz,
  is_failed boolean default false not null
);
create index ON backups.tasks (start_txtime);
ALTER TABLE backups.tasks
  OWNER TO postgres;

-- insert into backups.hosts (host, cluster_name, archiver_name, bareos_on, keep_backups_cnt, periodicity_days, directory) values
--                           ('master7-sb', 'master7', '{"archive_1", "archive_2"}', 'archive_1', 2, 4,  '/archive_path7/');
-- insert into backups.hosts (host, cluster_name, archiver_name, bareos_on, keep_backups_cnt, periodicity_days, directory) values
--                           ('master2-sb', 'master2', '{"archive_1", "archive_2"}', 'archive_1', 2, 4, '/archive_path2/') ;
-- insert into backups.hosts (host, cluster_name, archiver_name, bareos_on, keep_backups_cnt, periodicity_days, directory) values
--                           ('master1-sb', 'master1', '{"archive_1", "archive_2"}', 'archive_1', 2, 8, '/archive_path/') ;
                           
---

CREATE OR REPLACE FUNCTION backups.get_next(i_archiver text, OUT o_backip_id int, OUT o_pghost text, OUT o_pgport int, OUT o_remote_archiver text,
                                            OUT o_keep_backups_cnt int, OUT o_backups_dir text, OUT o_bareos_on text)
 RETURNS SETOF record
 LANGUAGE plpgsql
 ROWS 1
AS $function$
     -- Очередь бэкапов для бэкапа postgres-серверов на два архив-сервера (архивера) по очереди.

     -- Процедура возвращает какой бэкап начать делать скрипту base-backup и помечает в тиблице tasks как начатый (end_txtime = NULL).
     -- Скрипт, делающий бэкап, должен пометить бэкап как успешный, выполнив select * backups.stop(backup_id), 
     -- где backip_id - возвращаемый из этой хранимки o_backip_id.
     -- periodicity_days - периодичность бэкапа в днях для конкретного (!) архивера

     -- Возвращает 0 строк если нет заданий в данный момент для текущего архивера.
     -- Бэкапы чередуются между двумя и только двумя архиверами.
     -- Если бэкап данного инстанса делался на 1-й архивер, то следующий бэкап пойдет на 2-й архивер (и наоборот) - если не было аварий
     -- Если на одном из архиверов бэкап зафэйлился, он не будет выполняться на этом архивере до тех пор, пока
     -- вручную не удалят строчку из backups.tasks с этим бэкапом в состоянии failed или хранимкой 'select * from backups.stop(backup_id)'
     -- при этом, бэкап на второй сервер будет происходить с указанной периодичностью.

     -- ОГРАНИЧЕНИЯ:
     -- Первый бэкап должен запуститься на первом архивере, только тогда второй архивер сможет начать работу


     -- ПРИМЕР добавления сервера в очередь бэкапа:
        -- insert into backups.hosts (host, cluster_name, archiver_name, keep_backups_cnt, periodicity_days, directory) values
        --                 ('master7-sb', 'master7', '{"archive_1", "archive_2"}', 2, 4,  '/archive_path7/');

     -- host             - standby, с которого бэкапим
     -- cluster_name     - имя кластера (например, m7)
     -- archiver_name    - массив из двух архиверов (на них делаем бэкап)
     -- keep_backups_cnt - сколько бэкапов хранить на одном архивере. Рекомендуемое значение 2
     -- periodicity_days - периодичность бэкапа в днях для одного (!) архивера. Рекомендуемое значение  - от 4 и кратное 2
     -- directory        - директория, где хранятся бэкапы. Должна быть создана заранее.

DECLARE
    BACKUP_START_TIME constant time := '03:07'; -- стартовать бэкапы не раньше этого времени суток
    v_host_r record;
    v_chosen_archiver text;
    v_is_found boolean := false;
    v_days_delimiter integer;
    v_expected_backup_date timestamptz;
begin
    -- Помечаем последние задания как зафэйленные, если между запусками backups.get_next() для текущего архивера не
    -- выполнялось backups.done()
    update
        backups.tasks t
    set
        is_failed = true
    from
        backups.hosts b
    where
        t.backup_id = b.last_backup_id
        and t.archiver_name = i_archiver
        and t.end_txtime is NULL
        and t.is_failed = false;
    if found then
        raise notice '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
        raise notice 'WARNING: one of backups marked as failed!';
        raise notice '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
    end if;
 
    -- очередь бэкапов
    for v_host_r in select * from backups.hosts order by last_backup_start_txtime desc
    loop
        -- вычислим следующий архивер (берем противоположный предыдущему)
        -- но если предыдущий архивер зафэйлился, то бэкап все равно делаем
        -- проверяем сломался ли бэкап для архивера противпололожному последнему:
        perform * from
            backups.tasks t 
        where
            t.archiver_name = coalesce((array_remove(v_host_r.archiver_name, v_host_r.last_archiver))[1], v_host_r.archiver_name[1])
            and t.host = v_host_r.host
            and t.is_failed = true
        order by
            start_txtime desc
        limit 1;
        -- если он сломался, то выбираем архивер из поля last_archiver (бэкапимся на тот же архивер, что и в прошлый раз)
        if found then
            v_chosen_archiver := v_host_r.last_archiver;
            v_days_delimiter := 1; -- бэкапим на этот архивер каждые periodicity_days / v_days_multiplier дней
        -- иначе выбираем противоположный тому, что указан в last_archiver (вычитание из массива)
        else
            v_chosen_archiver := coalesce((array_remove(v_host_r.archiver_name, v_host_r.last_archiver))[1], v_host_r.archiver_name[1]);
            v_days_delimiter := 2; -- чтобы бэкапиться на разные архиверы со сдвигом 
        end if;
        
        -- пропускаем, если аргумент хранимки не совпал с выбранным алгоритмом архивером
        if i_archiver != v_chosen_archiver then
            continue;
        end if;
        
        -- если в задачах нет такого backup_id, то выбираем этот хост
        if v_host_r.last_backup_id is NULL then
            v_is_found := true;
            exit;
        end if;
        perform * from backups.tasks t where t.backup_id = v_host_r.last_backup_id;
        if not found then
            -- TODO: подумать как защититься от удаления строки в tasks
            v_is_found := true;
            exit;
        end if;

        -- проверяем завершился ли хоть один бэкап для этого хоста на выбранный архивер ошибкой
        perform *
        from
            backups.tasks t
        where
            t.archiver_name = v_chosen_archiver
            and t.host = v_host_r.host
            and t.is_failed = true
        order by 
            start_txtime desc
        limit 1;
        -- пропускаем бэкап, пока ошибку не исправят и не удалят из tasks записи с ошибкой
        if found then
            continue;
        end if;

        -- чтобы бэкапы чередовались правильно, v_days_multiplier зависит от того сломался ли позапрошлый бэкап или нет
        -- например: если periodicity_days = 4, то, в целом, бэкапы будут идти каждые 2 дня, но на разные архиверы
        -- но для каждого архивера в отдельности - это означает "бэкап раз в 4 дня"

        -- проверяем прошло ли нужное число дней с момента предыдущего бэкапа (periodicity_days) на данный архивер
        v_expected_backup_date := v_host_r.last_backup_start_txtime::date + BACKUP_START_TIME + v_host_r.periodicity_days * '1 days'::interval / v_days_delimiter;
        -- raise notice 'DEBUG: v_expected_backup_date: %, v_days_delimiter: %', v_expected_backup_date, v_days_delimiter;
        perform *
        from
            backups.tasks t
        where
            t.backup_id = v_host_r.last_backup_id
            and v_expected_backup_date::timestamptz <= now()
        order by
            start_txtime desc
        limit 1;
        if found then
            v_is_found := true;
            exit;
        end if;
    end loop;

    -- выходим с ошибкой если в цикле не нашли подходящего кандидата для бэкапа
    if v_is_found = false then
        -- raise notice 'there is no tasks for ''%''', i_archiver;
        return;
    end if;

    o_pghost := v_host_r.host;
    o_pgport := v_host_r.port;
    o_keep_backups_cnt := v_host_r.keep_backups_cnt;
    o_backups_dir := v_host_r.directory;
    o_bareos_on := v_host_r.bareos_on;

    -- выбираем хост для зеркалирования (wal-sync, wal-cleanup)
    select (array_remove(v_host_r.archiver_name, v_chosen_archiver))[1] into o_remote_archiver from backups.hosts h;

    -- вставляем в tasks строчку о задании
    insert into backups.tasks
        (host, archiver_name, start_txtime, end_txtime)
    values
        (v_host_r.host, v_chosen_archiver, now(), NULL)
    returning backup_id into o_backip_id;

    -- обновим состояние в hosts
    update backups.hosts set last_archiver = v_chosen_archiver, last_backup_start_txtime = now(), last_backup_id = o_backip_id where host = v_host_r.host;

    return next;

end;
$function$;

alter function backups.get_next ( text) owner to postgres ;

---

CREATE OR REPLACE FUNCTION backups.stop(i_backup_id integer, OUT o_info boolean)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
     -- Помечает backup_id взятый из функции backups.i_archiver() как завершившийся успешно
DECLARE
begin
    o_info := false;
    update
        backups.tasks
    set
        is_failed = false,
        end_txtime = now()
    where
        backup_id = i_backup_id;
    if found then
        o_info := true;
    end if;

end;
$function$;

alter function backups.stop ( integer) owner to postgres ;

---
