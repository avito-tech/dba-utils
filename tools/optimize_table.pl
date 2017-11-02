#!/usr/bin/perl
# -*- indent-tabs-mode: nil -*-
#

use strict;
use warnings;
use DBI;
#use Data::Dump qw(dump);

$| = 1; # autoflush

my $DBPORT = 5432;
my $DBUSER = 'postgres';
my $BULKVACUUM = 4000; # pages
#
my $PAGESTUPLES = 8192 / 26; # 26 байт - системные атрибуты всегда есть, даже в пустой таблице без полей
my $BULKPAGES = 3;

sub usage();
sub vacuum();
sub wait_txid($);
sub xmin();
sub get_txid();
sub pages_count();
sub max_page();
sub die_with_vacuum($);
sub gen_ctids($);
sub move($$);

usage if $#ARGV != 3;

my $DBHOST = $ARGV[0];
my $DBNAME = $ARGV[1];
my $DBTABLE = $ARGV[2];
my $XFIELD = $ARGV[3];

#DBI->trace('SQL'); # DBD

my $dbh = DBI->connect("dbi:Pg:host='$DBHOST';port=$DBPORT;dbname=$DBNAME;", $DBUSER, '',
                       {AutoCommit => 1, RaiseError => 1}); # , pg_server_prepare => 0

# пометить мёртвые как свободные
vacuum();

my $p_cnt = pages_count();
my $file_pages = pages_count();
my $total = 0;
my $i_total = 0;
my $i_BULKVACUUM = $BULKVACUUM / $BULKPAGES;
my $txid = 0;
while (1)
{
    print "page $p_cnt of $file_pages ($total) - ";

    $dbh->begin_work();
    # отключить все триггеры на сжимаемой таблице
    $dbh->do("set local session_replication_role to 'replica'");
    $txid = get_txid();
    my $r = move($p_cnt, gen_ctids($p_cnt));
    print "\n";
    if ($r < 0) {
        # сдвинуть не получилось, "забудем и вернём назад" попавших возможно на уже очищенные страницы
        $dbh->rollback();
    } else {
        $dbh->commit();
    }

    $total += $BULKPAGES;
    $i_total++;

    # print "no rows\n" if $r == 0;
    die_with_vacuum("no space left in table $DBTABLE, exit") if $r < 0; # плюс отрезать свободный конец файла таблицы если он пуст

    $p_cnt = $p_cnt - $BULKPAGES;

    # 
    # Убирать этот постоянный VACUUM отсюда и делатьего его один раз в конце нельзя!
    # Иначе при усечении файла будет очень долгая блокировка. PostgreSQL просматривает
    # отрезаемую часть файла с конца к максимальному найденному номеру не пустой страницы
    # находясь в эксклюзивной блокировке на сжимаемой таблице.
    #
    if ($i_total % $i_BULKVACUUM == 0)
    {
        wait_txid($txid);
        vacuum();
    }
}

$dbh->disconnect;

sub usage()
{
    # xfield - любая простая колонка, не TOAST и не varlen
    print "usage: dbhost dbname table xfield\n";
    exit 1;
}

sub wait_txid($)
{
    my ($txid) = @_;
    print "waiting for transactions with xmin < $txid\n";
    while(xmin() < $txid)
    {
        print ".";
        sleep 1;
    }
    print "\n"
}

sub vacuum()
{
    print "vacuum verbose $DBTABLE\n";
    $dbh->do("set vacuum_cost_delay to 0");
    $dbh->do("vacuum verbose $DBTABLE");
}

sub xmin()
{
    my $row = $dbh->selectrow_hashref(qq{
      select txid_snapshot_xmin(txid_current_snapshot()) as xmin
    });
    return $row->{'xmin'};
}

sub analyze()
{
    print "analyze verbose $DBTABLE\n";
    $dbh->do("set vacuum_cost_delay to 0");
    $dbh->do("analyze verbose $DBTABLE");
}

sub move($$)
{
    my ($page, $ptrs) = @_;
    my $min_page = $page - $BULKPAGES + 1;

    my $sth = $dbh->prepare(qq{
      update $DBTABLE set $XFIELD = $XFIELD
      where ctid = any (?)
      returning ctid as ptr
    });

    while ($#{$ptrs} >= 0)
    {
        my $rows = $dbh->selectcol_arrayref($sth, undef, $ptrs);
#       dump($rows);
        print $sth->rows,'.';
        return 0 if $#{$rows} < 0; # -1 - запрос не обновил ни одной строки, все обновляемые страницы уже пусты?

        # выкинуть из списка все tid у которых страница меньше исходной
        # если массив tid пуст - успех
        # если есть хотя бы один tid у которого страница больше исходной - неудача, кончились дырки в файле
        $ptrs = [];
        foreach my $tid (@$rows)
        {
            my $p = $1 if $tid =~ /^\((\d+),\d+\)$/;
            next if $p < $min_page;
            return -1 if $p > $page;
            # страница всё ещё та же, пробуем снова обновить этот tid
            push @$ptrs, "'$tid'";
        }
    }

    return 1;
}

sub get_txid()
{
    my $row = $dbh->selectrow_hashref(qq{
      select txid_current() as txid
    });
    return $row->{'txid'};
}

sub pages_count()
{
    my $rows = $dbh->selectrow_hashref(qq{
      select relpages as pages
      from pg_class
      where oid = '${DBTABLE}'::regclass
    });
    return $rows->{'pages'};
}

sub max_page()
{
    my $row = $dbh->selectrow_hashref(qq{
      select max(ctid) as ptr
      from $DBTABLE
    });
    my $page = 0;
    if ($row->{'ptr'} and $row->{'ptr'} =~ /^\((\d+),\d+\)$/) {
        $page = $1;
    }
    return $page;
}

sub die_with_vacuum($)
{
    vacuum();
    analyze();
    die @_;
}

sub gen_ctids($)
{
    my ($p_cnt) = @_;
    if ($p_cnt < 4) {
        $dbh->commit(); # gen_ctids run in transaction
        die_with_vacuum("cannot vacuum less then 4 pages");
    }
    my @ptrs = map {
        my $a = $_;
        map { "'($a,$_)'" } 1 .. $PAGESTUPLES;
    } $p_cnt-$BULKPAGES+1 .. $p_cnt;
    return \@ptrs;
}
