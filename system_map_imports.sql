INSERT INTO tl_map_extras.station_exchange_locations (
    exchange_name, exchange_type, bus_exchange_on, bike_park, bike_lock, municipality,
    sub_region, region_district, geometry, tspr_sub_region, year_open, notes
)
SELECT
    exchange_name,
    lower(exchange_type)::tl_map_extras.exchange_type,
    lower(bus_exchange_on)::tl_map_extras.bus_exchange_on,
    bike_park,
    bike_lock,
    municipality,
    sub_region,
    region_district,
    st_transform(st_point(exchange_longitude, exchange_latitude, srid := 4326), 3857),
    tspr_sub_region,
    year_open,
    NULL
FROM tl_map_extras.raw_station_exchange_locations;

WITH stops AS (
    SELECT
        exchange_name,
        unnest(string_to_array(stop_connections, '; ')) AS tl_stop_id
    FROM tl_map_extras.raw_station_exchange_locations
)
INSERT INTO tl_map_extras.station_exchange_stop_connections (exchange_name, stop_id, notes)
SELECT
    rs.exchange_name,
    s.stop_id,
    NULL
FROM stops rs
JOIN gtfs.stops s ON rs.tl_stop_id = s.stop_code;

WITH routes AS (
    SELECT
        exchange_name,
        unnest(string_to_array(line_connections, '; ')) AS tl_route_id
    FROM tl_map_extras.raw_station_exchange_locations
)
INSERT INTO tl_map_extras.station_exchange_route_connections (exchange_name, route_id, notes)
SELECT
    rr.exchange_name,
    r.route_id,
    NULL
FROM routes rr
JOIN gtfs.routes r ON
    rr.tl_route_id = r.route_short_name OR
    (rr.tl_route_id = 'SB' AND r.route_long_name = 'SeaBus') OR
    (rr.tl_route_id = 'ML' AND r.route_long_name = 'Millennium Line') OR
    (rr.tl_route_id = 'EL' AND r.route_long_name = 'Expo Line') OR
    (rr.tl_route_id = 'CL' AND r.route_long_name = 'Canada Line');

INSERT INTO tl_map_extras.stops (stop_id, municipality, sub_region, region_district, tspr_sub_region)
SELECT
    s.stop_id,
    rs.municipality,
    rs.sub_region,
    rs.region_district,
    rs.tspr_sub_region
FROM tl_map_extras.raw_stops rs
JOIN gtfs.stops s ON rs.stop_number::text = s.stop_code;

INSERT INTO tl_map_extras.routes (route_id, typology, category, tspr_sub_region)
SELECT
    r.route_id,
    lower(
            replace(replace(replace(replace(rl.typology, 'NightBus', 'night-bus'), 'RapidBus', 'rapid-bus'), ' - ', '-'), ' ', '-')
    )::tl_map_extras.route_typology,
    lower(
            replace(replace(replace(replace(replace(replace(rl.category, 'NightBus', 'night-bus'), 'RapidBus', 'rapid-bus'), ' Line)', ''), '(', ''), ' - ', '-'), ' ', '-')
    )::tl_map_extras.route_category,
    rl.tspr_sub_region
FROM tl_map_extras.raw_lines rl
JOIN gtfs.routes r ON
    rl.line = r.route_short_name OR
    (rl.line = 'SB' AND r.route_long_name = 'SeaBus') OR
    (rl.line = 'ML' AND r.route_long_name = 'Millennium Line') OR
    (rl.line = 'EL' AND r.route_long_name = 'Expo Line') OR
    (rl.line = 'CL' AND r.route_long_name = 'Canada Line');
