#!/bin/bash
set -o errexit

time npm exec -- gtfs-to-sql --require-dependencies --ignore-unsupported --schema gtfs -- work/gtfs/*.txt | sponge > work/import.sql
time psql -b -f import.sql
