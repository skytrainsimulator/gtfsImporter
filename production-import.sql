\set ON_ERROR_STOP on
BEGIN;

ALTER TABLE gis.node_stop_positions
DROP CONSTRAINT node_stop_positions_gtfs_id;

TRUNCATE
    gtfs.feed_info,
    gtfs.agency,
    gtfs.calendar,
    gtfs.calendar_dates,
    gtfs.shapes,
    gtfs.stops,
    gtfs.routes,
    gtfs.trips,
    gtfs.stop_times,
    gtfs.frequencies,
    gtfs.transfers;

\i production-import/feed_info.sql
\i production-import/agency.sql
\i production-import/calendar.sql
\i production-import/calendar_dates.sql
\i production-import/shapes.sql
\i production-import/stops.sql
\i production-import/routes.sql
\i production-import/trips.sql
\i production-import/stop_times.sql
\i production-import/frequencies.sql
\i production-import/transfers.sql

ALTER TABLE gis.node_stop_positions
ADD CONSTRAINT node_stop_positions_gtfs_id
FOREIGN KEY (gtfs_id) REFERENCES gtfs.stops (stop_id);

REFRESH MATERIALIZED VIEW gtfs.service_days;
COMMIT;
