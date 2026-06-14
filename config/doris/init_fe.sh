#!/usr/bin/env sh
set -eu

until mysql -h doris-fe -P 9030 -u root -e "SHOW FRONTENDS;" >/dev/null 2>&1; do
  sleep 2
done

mysql -h doris-fe -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND 'doris-be:9050';" >/dev/null 2>&1 || true

echo "Doris backend registration attempted."
