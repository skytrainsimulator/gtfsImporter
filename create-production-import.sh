#!/bin/bash
set -o errexit

dumpTable() {
  echo "Dumping $1"
  pg_dump --data-only --file "production-import/$1.sql" --table "gtfs.$1"
}

rm -rf production-import/
mkdir production-import
dumpTable "agency"
dumpTable "calendar"
dumpTable "calendar_dates"
dumpTable "feed_info"
dumpTable "frequencies"
dumpTable "routes"
dumpTable "shapes"
dumpTable "stop_times"
dumpTable "stops"
dumpTable "transfers"
dumpTable "trips"
