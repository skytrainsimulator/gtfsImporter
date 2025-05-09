CREATE SCHEMA IF NOT EXISTS tl_map_extras;

CREATE TABLE IF NOT EXISTS tl_map_extras.raw_station_exchange_locations (
    exchange_name TEXT NOT NULL PRIMARY KEY,
    exchange_type TEXT NOT NULL,
    year_open INT,
    bus_exchange_on TEXT NOT NULL,
    exchange_longitude DOUBLE PRECISION NOT NULL,
    exchange_latitude DOUBLE PRECISION NOT NULL,
    bike_park BOOLEAN NOT NULL,
    bike_lock BOOLEAN NOT NULL,
    line_connections TEXT NOT NULL,
    stop_connections TEXT NOT NULL,
    municipality TEXT NOT NULL,
    sub_region TEXT NOT NULL,
    tspr_sub_region TEXT NOT NULL,
    region_district TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.raw_lines (
    line TEXT NOT NULL PRIMARY KEY,
    tspr_sub_region TEXT NOT NULL,
    typology TEXT NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.raw_stops (
    stop_number INT NOT NULL PRIMARY KEY,
    municipality TEXT NOT NULL,
    sub_region TEXT NOT NULL,
    tspr_sub_region TEXT NOT NULL,
    region_district TEXT NOT NULL
);



CREATE TABLE IF NOT EXISTS tl_map_extras.municipalities (
    name TEXT NOT NULL PRIMARY KEY,
    notes TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.sub_regions (
    name TEXT NOT NULL PRIMARY KEY,
    notes TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.region_district (
    name TEXT NOT NULL PRIMARY KEY,
    notes TEXT DEFAULT NULL
);

CREATE TYPE tl_map_extras.exchange_type AS ENUM ('station', 'exchange', 'loop');

CREATE TYPE tl_map_extras.bus_exchange_on AS ENUM ('on-street', 'off-street');

CREATE TABLE IF NOT EXISTS tl_map_extras.station_exchange_locations (
    exchange_name TEXT NOT NULL PRIMARY KEY,
    exchange_type tl_map_extras.exchange_type NOT NULL,
    bus_exchange_on tl_map_extras.bus_exchange_on NOT NULL,
    bike_park BOOLEAN NOT NULL,
    bike_lock BOOLEAN NOT NULL,
    municipality TEXT NOT NULL REFERENCES tl_map_extras.municipalities (name),
    sub_region TEXT NOT NULL REFERENCES tl_map_extras.sub_regions (name),
    region_district TEXT NOT NULL REFERENCES tl_map_extras.region_district (name),
    geometry geometry(POINT, 3857) NOT NULL,
    tspr_sub_region TEXT DEFAULT NULL,
    year_open INT DEFAULT NULL,
    notes TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.station_exchange_stop_connections (
    exchange_name TEXT NOT NULL REFERENCES tl_map_extras.station_exchange_locations (exchange_name),
    stop_id TEXT NOT NULL REFERENCES gtfs.stops (stop_id),
    notes TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.station_exchange_route_connections (
    exchange_name TEXT NOT NULL REFERENCES tl_map_extras.station_exchange_locations (exchange_name),
    route_id TEXT NOT NULL REFERENCES gtfs.routes (route_id),
    notes TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS tl_map_extras.stops (
    stop_id TEXT NOT NULL PRIMARY KEY REFERENCES gtfs.stops (stop_id),
    municipality TEXT NOT NULL REFERENCES tl_map_extras.municipalities,
    sub_region TEXT NOT NULL REFERENCES tl_map_extras.sub_regions,
    region_district TEXT NOT NULL REFERENCES tl_map_extras.region_district,
    tspr_sub_region TEXT NOT NULL
);

CREATE TYPE tl_map_extras.route_typology AS ENUM (
    'rapid', 'rapid-bus', 'b-line', 'all-day-frequent', 'peak-frequent', 'standard', 'basic',
    'peak-only-limited', 'special', 'commuter-rail', 'seabus', 'night-bus'
    );

CREATE TYPE tl_map_extras.route_category AS ENUM (
    'skytrain-expo', 'skytrain-millennium', 'skytrain-canada', 'rapid-bus', 'b-line', 'all-day-frequent',
    'peak-frequent', 'standard', 'basic', 'limited', 'west-coast-express', 'seabus', 'night-bus'
    );

CREATE TABLE IF NOT EXISTS tl_map_extras.routes (
    route_id TEXT NOT NULL PRIMARY KEY REFERENCES gtfs.routes (route_id),
    typology tl_map_extras.route_typology NOT NULL,
    category tl_map_extras.route_category NOT NULL,
    tspr_sub_region TEXT NOT NULL
);
