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
     -- Backups queue for backup postgres servers to two archive servers in turn.

     -- Function returns which backup must be executed by base-backup and mark it in table tasks as in progress (end_txtime = NULL).
     -- Backup script must mark successful backup by executing select * backups.stop(backup_id),
     -- backip_id -  o_backip_id is an out parameter of that function.
     -- periodicity_days - periodicity(in days) of backup for specific(!) archive

     -- Returns 0 strings if there is no backup tasks for specific archive
     -- Backups are alternating between 2 archive servers.
     -- If previous backup was made to 1st archive server, then next will be made by 2nd archive server(and vice versa) if there is no crashes
     -- If the backup is failed on specific archive server, it will not being retried till
     -- the row with failed status would be deleted(manually or by 'select * from backups.stop(backup_id)') from  backups.tasks
     -- Meanwhile on second archive backups operations will continue being successfully executed

     -- RESTRICTIONS:
     -- First backup must be run on the 1st archive server (second archive starts work only after 1st one)


     -- EXAMPLE of adding new cluster to backup queu:
        -- insert into backups.hosts (host, cluster_name, archiver_name, keep_backups_cnt, periodicity_days, directory) values
        --                 ('master7-sb', 'master7', '{"archive_1", "archive_2"}', 2, 4,  '/archive_path7/');

     -- host             - standby, from which backups will be taken
     -- cluster_name     - cluster name (e.g. master7)
     -- archiver_name    - array with two archive servers (destination of backup)
     -- keep_backups_cnt - the number of backups to keep on one server (recommended value 2)
     -- periodicity_days - schedule for one(!) archive server (recommended value 4 or more and it must be multiple of 2)
     -- directory        - destination

DECLARE
    BACKUP_START_TIME constant time := '03:07'; -- don't start backup befor this time
    v_host_r record;
    v_chosen_archiver text;
    v_is_found boolean := false;
    v_days_delimiter integer;
    v_expected_backup_date timestamptz;
begin
    -- Mark backup tasks as failed if there was no backups.done() call between backups.get_next() for specific archive server
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
 
    -- backup queue
    for v_host_r in select * from backups.hosts order by last_backup_start_txtime desc
    loop
        -- try to derive next archive server name (take the opposite one)
        -- if there was fail on previous one, baskup still needs to be done
        -- check existence failed backup task on opposite archive:
        perform * from
            backups.tasks t 
        where
            t.archiver_name = coalesce((array_remove(v_host_r.archiver_name, v_host_r.last_archiver))[1], v_host_r.archiver_name[1])
            and t.host = v_host_r.host
            and t.is_failed = true
        order by
            start_txtime desc
        limit 1;
        -- if failed then continue execute backup tasks on previous one archive server
        if found then
            v_chosen_archiver := v_host_r.last_archiver;
            v_days_delimiter := 1;
        else
            v_chosen_archiver := coalesce((array_remove(v_host_r.archiver_name, v_host_r.last_archiver))[1], v_host_r.archiver_name[1]);
            v_days_delimiter := 2;
        end if;
        
        -- skip if input parameter does not match with reusult of function
        if i_archiver != v_chosen_archiver then
            continue;
        end if;
        
        -- if there is now such backup_id in tasksk, then chose that host
        if v_host_r.last_backup_id is NULL then
            v_is_found := true;
            exit;
        end if;
        perform * from backups.tasks t where t.backup_id = v_host_r.last_backup_id;
        if not found then
            -- TODO: cope with deleting of row in tasks
            v_is_found := true;
            exit;
        end if;

        -- check for errors in backup tasks for chosen archive server
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
        -- skip backup execution till error will be fixed and record with error will be removed from tasks
        if found then
            continue;
        end if;

        -- v_days_multiplier dependency on before last backup task fail is done to alternate archives in right way
        -- e.g.: if periodicity_days = 4, then backup will be made each 2 days on different archive servers
        -- while on each archive server backup task is executed each 4 days

        -- check if periodicity_days are passed since last backup on that archive server
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

    -- exit with error if don't find appropriate backup candidate
    if v_is_found = false then
        -- raise notice 'there is no tasks for ''%''', i_archiver;
        return;
    end if;

    o_pghost := v_host_r.host;
    o_pgport := v_host_r.port;
    o_keep_backups_cnt := v_host_r.keep_backups_cnt;
    o_backups_dir := v_host_r.directory;
    o_bareos_on := v_host_r.bareos_on;

    -- choose host for syncing (wal-sync, wal-cleanup)
    select (array_remove(v_host_r.archiver_name, v_chosen_archiver))[1] into o_remote_archiver from backups.hosts h;

    -- add record with backup task
    insert into backups.tasks
        (host, archiver_name, start_txtime, end_txtime)
    values
        (v_host_r.host, v_chosen_archiver, now(), NULL)
    returning backup_id into o_backip_id;

    -- update hosts status
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
     -- mark backup_id (which was get by backups.i_archiver()) as successful
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
