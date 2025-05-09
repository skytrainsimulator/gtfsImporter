\set ON_ERROR_STOP on

BEGIN;

DROP CAST IF EXISTS (gtfs_import.availability AS gtfs.availability);
DROP CAST IF EXISTS (gtfs_import.bikes_allowance AS gtfs.bikes_allowance);
DROP CAST IF EXISTS (gtfs_import.exact_times_v AS gtfs.exact_times_v);
DROP CAST IF EXISTS (gtfs_import.exception_type_v AS gtfs.exception_type_v);
DROP CAST IF EXISTS (gtfs_import.location_type_val AS gtfs.location_type_val);
DROP CAST IF EXISTS (gtfs_import.pickup_drop_off_type AS gtfs.pickup_drop_off_type);
DROP CAST IF EXISTS (gtfs_import.route_type_val AS gtfs.route_type_val);
DROP CAST IF EXISTS (gtfs_import.timepoint_v AS gtfs.timepoint_v);
DROP CAST IF EXISTS (gtfs_import.transfer_type_v AS gtfs.transfer_type_v);
DROP CAST IF EXISTS (gtfs_import.wheelchair_accessibility AS gtfs.wheelchair_accessibility);
DROP CAST IF EXISTS (gtfs_import.wheelchair_boarding_val AS gtfs.wheelchair_boarding_val);
DROP SCHEMA IF EXISTS gtfs_import CASCADE;
CREATE SCHEMA gtfs_import;

\i work/import.sql

-- As of 2024/11/24, the dataset uses the incorrect stop ID for the Canada Line single-platform stations
-- (The station is used instead of the platform)
WITH broken_stop_ids AS ( VALUES
    ('11294', '11295'), -- Richmond-Brighouse Station / Platform 1
    ('11300', '11301') -- YVR-Airport Station / Platform 1
)
UPDATE gtfs_import.stop_times
SET stop_id = broken_stop_ids.column2
FROM broken_stop_ids
WHERE stop_id = broken_stop_ids.column1;

CREATE CAST (gtfs_import.availability AS gtfs.availability) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.bikes_allowance AS gtfs.bikes_allowance) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.exact_times_v AS gtfs.exact_times_v) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.exception_type_v AS gtfs.exception_type_v) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.location_type_val AS gtfs.location_type_val) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.pickup_drop_off_type AS gtfs.pickup_drop_off_type) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.route_type_val AS gtfs.route_type_val) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.timepoint_v AS gtfs.timepoint_v) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.transfer_type_v AS gtfs.transfer_type_v) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.wheelchair_accessibility AS gtfs.wheelchair_accessibility) WITH INOUT AS IMPLICIT;
CREATE CAST (gtfs_import.wheelchair_boarding_val AS gtfs.wheelchair_boarding_val) WITH INOUT AS IMPLICIT;

-- Copy order:
-- shapes
-- calendar
-- calendar_dates
-- agency
-- routes
-- trips
-- frequencies
-- stops
-- stop_times
-- feed_info
-- transfers

ALTER TABLE gtfs.stops DROP CONSTRAINT stops_parent_station_fkey;

DELETE FROM gtfs.transfers WHERE 1 > 0;
DELETE FROM gtfs.feed_info WHERE 1 > 0;
DELETE FROM gtfs.stop_times WHERE 1 > 0;
DELETE FROM gtfs.stops WHERE 1 > 0;
DELETE FROM gtfs.frequencies WHERE 1 > 0;
DELETE FROM gtfs.trips WHERE 1 > 0;
DELETE FROM gtfs.routes WHERE 1 > 0;
DELETE FROM gtfs.agency WHERE 1 > 0;
DELETE FROM gtfs.calendar_dates WHERE 1 > 0;
DELETE FROM gtfs.calendar WHERE 1 > 0;
DELETE FROM gtfs.shapes WHERE 1 > 0;

INSERT INTO gtfs.shapes (id, shape_id, shape_pt_sequence, shape_pt_loc, shape_dist_traveled)
SELECT id, shape_id, shape_pt_sequence, shape_pt_loc, shape_dist_traveled
FROM gtfs_import.shapes;

INSERT INTO gtfs.calendar (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date
FROM gtfs_import.calendar;

INSERT INTO gtfs.calendar_dates (service_id, date, exception_type)
SELECT service_id, date, exception_type
FROM gtfs_import.calendar_dates;

INSERT INTO gtfs.agency (agency_id, agency_name, agency_url, agency_timezone, agency_lang, agency_phone, agency_fare_url, agency_email)
SELECT agency_id, agency_name, agency_url, agency_timezone, agency_lang, agency_phone, agency_fare_url, agency_email
FROM gtfs_import.agency;

INSERT INTO gtfs.routes (route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color, route_sort_order)
SELECT route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color, route_sort_order
FROM gtfs_import.routes;

INSERT INTO gtfs.trips (trip_id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible, bikes_allowed)
SELECT trip_id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible, bikes_allowed
FROM gtfs_import.trips;

INSERT INTO gtfs.frequencies (frequencies_row, trip_id, start_time, end_time, headway_secs, exact_times)
SELECT frequencies_row, trip_id, start_time, end_time, headway_secs, exact_times
FROM gtfs_import.frequencies;

INSERT INTO gtfs.stops (stop_id, stop_code, stop_name, stop_desc, stop_loc, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code)
SELECT stop_id, stop_code, stop_name, stop_desc, stop_loc, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code
FROM gtfs_import.stops;

INSERT INTO gtfs.stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_sequence_consec, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, timepoint, trip_start_time)
SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_sequence_consec, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, timepoint, trip_start_time
FROM gtfs_import.stop_times;

INSERT INTO gtfs.feed_info (feed_publisher_name, feed_publisher_url, feed_lang, default_lang, feed_start_date, feed_end_date, feed_version, feed_contact_email, feed_contact_url)
SELECT feed_publisher_name, feed_publisher_url, feed_lang, default_lang, feed_start_date, feed_end_date, feed_version, feed_contact_email, feed_contact_url
FROM gtfs_import.feed_info;

INSERT INTO gtfs.transfers (id, from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id, to_trip_id)
SELECT id, from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id, to_trip_id
FROM gtfs_import.transfers;

ALTER TABLE "gtfs".stops
ADD CONSTRAINT stops_parent_station_fkey
FOREIGN KEY (parent_station) REFERENCES "gtfs".stops;

REFRESH MATERIALIZED VIEW gtfs.service_days;
REFRESH MATERIALIZED VIEW gtfs.feed_time_frame;

REFRESH MATERIALIZED VIEW gtfs_extra.bounds;
REFRESH MATERIALIZED VIEW gtfs_extra.stop_routes;
REFRESH MATERIALIZED VIEW gtfs_extra.route_stops;
REFRESH MATERIALIZED VIEW gtfs_extra.all_routes;
REFRESH MATERIALIZED VIEW gtfs_extra.all_stops;
REFRESH MATERIALIZED VIEW gtfs_extra.all_stops_by_route;
REFRESH MATERIALIZED VIEW gtfs_extra.grouped_stops_by_typology;
REFRESH MATERIALIZED VIEW gtfs_extra.deduplicated_grouped_stops_by_typology;
REFRESH MATERIALIZED VIEW gtfs_extra.all_trips_by_shape;
REFRESH MATERIALIZED VIEW gtfs_extra.all_routes_shape;
REFRESH MATERIALIZED VIEW gtfs_extra.all_trips;
REFRESH MATERIALIZED VIEW gtfs_extra.trip_blocks;

DROP CAST IF EXISTS (gtfs_import.availability AS gtfs.availability);
DROP CAST IF EXISTS (gtfs_import.bikes_allowance AS gtfs.bikes_allowance);
DROP CAST IF EXISTS (gtfs_import.exact_times_v AS gtfs.exact_times_v);
DROP CAST IF EXISTS (gtfs_import.exception_type_v AS gtfs.exception_type_v);
DROP CAST IF EXISTS (gtfs_import.location_type_val AS gtfs.location_type_val);
DROP CAST IF EXISTS (gtfs_import.pickup_drop_off_type AS gtfs.pickup_drop_off_type);
DROP CAST IF EXISTS (gtfs_import.route_type_val AS gtfs.route_type_val);
DROP CAST IF EXISTS (gtfs_import.timepoint_v AS gtfs.timepoint_v);
DROP CAST IF EXISTS (gtfs_import.transfer_type_v AS gtfs.transfer_type_v);
DROP CAST IF EXISTS (gtfs_import.wheelchair_accessibility AS gtfs.wheelchair_accessibility);
DROP CAST IF EXISTS (gtfs_import.wheelchair_boarding_val AS gtfs.wheelchair_boarding_val);
DROP SCHEMA gtfs_import CASCADE;

COMMIT;
