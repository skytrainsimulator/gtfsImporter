BEGIN;

DROP SCHEMA IF EXISTS gtfs CASCADE;
CREATE SCHEMA gtfs;

\i work/import.sql

-- The foreign keys gtfs-via-postgresql generates aren't really optimized for what I'm doing
-- Make them cascading so that I can easily modify the dataset as needed to optimize it.

ALTER TABLE gtfs.frequencies
    DROP CONSTRAINT frequencies_trip_id_fkey;
ALTER TABLE gtfs.frequencies
    ADD FOREIGN KEY (trip_id) REFERENCES gtfs.trips
        ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE gtfs.stop_times
    DROP CONSTRAINT stop_times_trip_id_fkey;
ALTER TABLE gtfs.stop_times
    ADD FOREIGN KEY (trip_id) REFERENCES gtfs.trips
        ON UPDATE CASCADE ON DELETE CASCADE;

-- The transfers table has 2 of the keys targeting the wrong column, let's also fix that

ALTER TABLE gtfs.transfers
    DROP CONSTRAINT transfers_from_route_id_fkey;
ALTER TABLE gtfs.transfers
    ADD FOREIGN KEY (from_route_id) REFERENCES gtfs.routes
        ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE gtfs.transfers
    DROP CONSTRAINT transfers_from_route_id_fkey1;
ALTER TABLE gtfs.transfers
    ADD FOREIGN KEY (to_route_id) REFERENCES gtfs.routes
        ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE gtfs.transfers
    DROP CONSTRAINT transfers_from_trip_id_fkey;
ALTER TABLE gtfs.transfers
    ADD FOREIGN KEY (from_trip_id) REFERENCES gtfs.trips
        ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE gtfs.transfers
    DROP CONSTRAINT transfers_from_trip_id_fkey1;
ALTER TABLE gtfs.transfers
    ADD FOREIGN KEY (to_trip_id) REFERENCES gtfs.trips
        ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE gtfs.trips
    DROP CONSTRAINT trips_route_id_fkey;
ALTER TABLE gtfs.trips
    ADD FOREIGN KEY (route_id) REFERENCES gtfs.routes
        ON UPDATE CASCADE ON DELETE CASCADE;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT true FROM gtfs.routes WHERE route_id = '30052' AND route_long_name = 'Millennium Line') THEN
            RAISE WARNING 'Millennium Line route not found. Did the route_long_name change?';
        END IF;
        IF NOT EXISTS (SELECT true FROM gtfs.routes WHERE route_id = '30053' AND route_long_name = 'Expo Line') THEN
            RAISE WARNING 'Expo Line route not found. Did the route_long_name change?';
        END IF;
        IF NOT EXISTS (SELECT true FROM gtfs.routes WHERE route_id = '13686' AND route_long_name = 'Canada Line') THEN
            RAISE WARNING 'Canada route not found. Did the route_long_name change?';
        END IF;
    END
$$ LANGUAGE plpgsql;

-- As of 2024/08/26, the dataset does not include hold time at stations.
-- Based off personal observation, the given timestamp appears to be the departure time.
UPDATE gtfs.stop_times st
SET arrival_time = st.arrival_time - INTERVAL '30 seconds'
FROM (SELECT t.trip_id FROM gtfs.trips t WHERE t.route_id = '30053' OR t.route_id = '30052' OR t.route_id = '13686') t
WHERE st.arrival_time = st.departure_time AND t.trip_id = st.trip_id;

-- BRAID SINGLE-TRACK FIXES
-- As of 2024/02/25, Expo Line is single-tracking between the Sapperton crossover & Lougheed Town Center
-- Expected timeline of 2 years

-- Outbound trains hold at LH Platform 2 for 3 minutes to sequence correctly
UPDATE gtfs.stop_times st
SET arrival_time = st.arrival_time - INTERVAL '2 minutes 30 seconds'
FROM (SELECT t.trip_id FROM gtfs.trips t WHERE t.route_id = '30053') t
WHERE st.arrival_time = st.departure_time AND t.trip_id = st.trip_id AND st.stop_id = '8746';

COMMIT;
