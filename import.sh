#!/bin/bash
set -o errexit

# As of October 12, 2024, Translink's GTFS feeds include spaces in the stop times, something gtfs-via-postgres
# does not support due to an overly strict regex.
sed -i 's/ //g' ./work/gtfs/stop_times.txt
npm exec -- gtfs-to-sql --require-dependencies --ignore-unsupported --schema gtfs_import --stats-by-route-date view --stats-by-agency-route-stop-hour view --stats-active-trips-by-hour view -- work/gtfs/*.txt | sponge > work/import.sql
# gtfs-via-postgres doesn't have an option to not wrap the output in a transaction
# Normally that's sane, but in this case the script is being ran in another transaction.
sed -i '/^BEGIN;\|^COMMIT;\|^END;/d' ./work/import.sql
psql -b -f import.sql
