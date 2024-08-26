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

COMMIT;
