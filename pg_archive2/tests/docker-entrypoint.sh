#!/bin/bash
#
# use 'docker logs test-pg-archive' for debug this script
#
TIMEOUT=10

gosu ()
{
    /usr/sbin/gosu "$@"
}

set -x

if (( $(id -u) != 0 )); then
    gosu root bash -l $0 "$@"
else
    service ssh start &
    exit # root
fi

echo "wait ssh start ($TIMEOUT)"
for ((i = $TIMEOUT; i > 0; i--)); do
    gosu root pidof /usr/sbin/sshd && break
    sleep 1
done
(( i )) || { echo "timeout"; exit 1; }

# ssh-keyscan localhost test-archive02 test-archive03 > ~/.ssh/known_hosts 2> /dev/null
for name in localhost test test-archive02 test-archive03; do
    for key in /etc/ssh/*.pub; do
        echo "$name $(< "$key")" >> ~/.ssh/known_hosts
    done
done

if [[ $1 = 'test' ]]; then
    pg_ctl start -w -o '-h 0.0.0.0 --fsync=off'
fi
echo "= docker-entrypoint.sh started ="

sleep inf
