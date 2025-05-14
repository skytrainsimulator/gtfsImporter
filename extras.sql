CREATE SCHEMA IF NOT EXISTS gtfs_extra;

CREATE MATERIALIZED VIEW gtfs_extra.bounds AS
SELECT
    st_transform(st_envelope(st_buffer(st_union(array_agg(st_transform(stop_loc::geometry, 3857))), 1000)), 4326) AS bounds, 1 AS id
FROM gtfs.stops;

CREATE MATERIALIZED VIEW gtfs_extra.stop_routes AS
SELECT DISTINCT
    st.stop_id,
    array_agg(DISTINCT t.route_id) AS route_ids
FROM gtfs.stop_times st
JOIN gtfs.trips t USING (trip_id)
GROUP BY st.stop_id;
CREATE UNIQUE INDEX stop_routes_stop_id_index ON gtfs_extra.stop_routes (stop_id);

CREATE MATERIALIZED VIEW gtfs_extra.route_stops AS
SELECT DISTINCT
    t.route_id,
    array_agg(DISTINCT st.stop_id) AS stop_ids
FROM gtfs.stop_times st
JOIN gtfs.trips t USING (trip_id)
GROUP BY t.route_id;
CREATE UNIQUE INDEX route_stops_route_id_index ON gtfs_extra.route_stops (route_id);

CREATE MATERIALIZED VIEW gtfs_extra.all_routes AS
WITH route_exchanges AS (
    SELECT
        route_id,
        array_agg(DISTINCT exchange_name ORDER BY exchange_name) AS route_served_exchanges
    FROM tl_map_extras.station_exchange_route_connections serc
    LEFT JOIN tl_map_extras.station_exchange_locations sel USING (exchange_name)
    GROUP BY route_id
)
SELECT
    r.*,
    (
        route_short_name LIKE '%R%' OR
        r.route_long_name = 'Millennium Line' OR
        r.route_long_name = 'Expo Line' OR
        r.route_long_name = 'Canada Line' OR
        route_short_name = '099' OR
        route_short_name = 'WCE' OR
        route_long_name = 'SeaBus'
    ) AS route_is_significant,
    CASE WHEN (
            route_short_name LIKE '%R%' OR
            r.route_long_name = 'Millennium Line' OR
            r.route_long_name = 'Expo Line' OR
            r.route_long_name = 'Canada Line' OR
            route_short_name = '099' OR
            route_short_name = 'WCE' OR
            route_long_name = 'SeaBus'
        )
    THEN 800 ELSE 400 END AS route_stop_coverage_radius,
    coalesce(
        CASE
            WHEN r.route_long_name = 'Millennium Line' THEN 'ML'
            WHEN r.route_long_name = 'Expo Line' THEN 'EL'
            WHEN r.route_long_name = 'Canada Line' THEN 'CL'
            WHEN r.route_long_name = 'SeaBus' THEN 'SB'
        END,
        r.route_short_name,
        r.route_long_name
    ) AS route_display,
    typology AS route_extras_typology,
    category AS route_extras_category,
    er.tspr_sub_region AS route_extras_tspr_sub_region,
    route_served_exchanges,
    array_to_string(route_served_exchanges, ', ') AS route_served_exchanges_text
FROM gtfs.routes r
LEFT JOIN tl_map_extras.routes er USING (route_id)
LEFT JOIN route_exchanges USING (route_id);
CREATE UNIQUE INDEX all_routes_route_id_index ON gtfs_extra.all_routes (route_id);

CREATE MATERIALIZED VIEW gtfs_extra.all_stops AS
WITH compiled_stop_routes AS (
    SELECT
        s.stop_id,
        sr.route_ids AS stop_route_ids,
        array_agg(ar.route_display ORDER BY ar.route_display) AS stop_route_names,
        array_to_string(array_agg(ar.route_display ORDER BY ar.route_display), ', ') AS stop_route_names_text,
        bool_or(ar.route_is_significant) AS stop_any_route_is_significant,
        max(ar.route_stop_coverage_radius) AS stop_coverage_radius
    FROM gtfs.stops s
    JOIN gtfs_extra.stop_routes sr USING (stop_id)
    JOIN gtfs_extra.all_routes ar ON ar.route_id = ANY (sr.route_ids)
    GROUP BY s.stop_id, sr.route_ids
)
SELECT
    s.*,
    municipality AS stop_extras_municipality,
    sub_region AS stop_extras_sub_region,
    region_district AS stop_extras_region_district,
    tspr_sub_region AS stop_extras_tspr_sub_region,
    stop_route_ids, stop_route_names, stop_route_names_text, stop_coverage_radius,
    st_transform(st_buffer(st_transform(stop_loc::geometry, 3857), stop_coverage_radius), 4326) AS stop_coverage,
    st_transform(st_buffer(st_transform(stop_loc::geometry, 3857), 400), 4326) AS stop_400m_radius
FROM compiled_stop_routes csr
JOIN gtfs.stops s USING (stop_id)
LEFT JOIN tl_map_extras.stops es USING (stop_id);
CREATE UNIQUE INDEX all_stops_stop_id_index ON gtfs_extra.all_stops (stop_id);

CREATE MATERIALIZED VIEW gtfs_extra.all_stops_by_route AS
SELECT
    s.*,
    st_transform(st_buffer(st_transform(stop_loc::geometry, 3857), ar.route_stop_coverage_radius), 4326) AS stop_route_coverage,
    ar.*,
    '[' || ar.route_display || '] ' || s.stop_name AS stop_route_name,
    s.stop_id || '-' || ar.route_id AS qgis_id
FROM gtfs_extra.all_stops s
JOIN gtfs_extra.all_routes ar ON ar.route_id = ANY (s.stop_route_ids);

CREATE MATERIALIZED VIEW gtfs_extra.grouped_stops_by_typology AS
SELECT
    asbr.route_extras_typology,
    st_union(array_agg(stop_route_coverage)) AS stop_route_coverage,
    st_union(array_agg(stop_400m_radius)) AS stop_400m_coverage
FROM gtfs_extra.all_stops_by_route asbr
GROUP BY asbr.route_extras_typology;

CREATE MATERIALIZED VIEW gtfs_extra.deduplicated_grouped_stops_by_typology AS
WITH tmp AS (
    SELECT
        route_extras_typology,
        stop_route_coverage,
        st_union(stop_route_coverage) OVER w AS already_covered_stop_route_coverage,
        stop_400m_coverage,
        st_union(stop_400m_coverage) OVER w AS already_covered_stop_400m_coverage
    FROM gtfs_extra.grouped_stops_by_typology
    WINDOW w AS (ORDER BY route_extras_typology ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW)
)
SELECT
    route_extras_typology,
    st_difference(stop_route_coverage, coalesce(already_covered_stop_route_coverage, 'srid=4326;POLYGON EMPTY'::geometry)) AS stop_route_coverage,
    st_difference(stop_400m_coverage, coalesce(already_covered_stop_400m_coverage, 'srid=4326;POLYGON EMPTY'::geometry)) AS stop_400m_coverage
FROM tmp;

CREATE MATERIALIZED VIEW gtfs_extra.all_trips_by_shape AS
WITH tmp AS (
    SELECT
        array_agg(DISTINCT t.trip_headsign) AS trip_headsigns,
        array_agg(DISTINCT t.service_id) AS trip_service_ids,
        t.route_id,
        t.shape_id
    FROM gtfs.trips t
    GROUP BY t.shape_id, t.route_id
)
SELECT
    sa.shape,
    trip_headsigns,
    array_to_string(trip_headsigns, ', ') AS trip_headsigns_text,
    trip_service_ids,
    array_to_string(trip_service_ids, ', ') AS trip_service_ids_text,
    ar.*,
    tmp.shape_id,
    tmp.shape_id || '-' || tmp.route_id AS qgis_id
FROM tmp
JOIN gtfs.shapes_aggregated sa USING (shape_id)
JOIN gtfs_extra.all_routes ar USING (route_id);

CREATE MATERIALIZED VIEW gtfs_extra.all_routes_shape AS
WITH unionized AS (
    SELECT
        atbs.route_id,
        st_union(array_agg(atbs.shape)) AS shapes_unioned,
        array_agg(atbs.shape_id) AS shape_ids
    FROM gtfs_extra.all_trips_by_shape atbs
    GROUP BY atbs.route_id
)
SELECT
    ar.*,
    shapes_unioned,
    st_linemerge(shapes_unioned) AS shape_merged,
    shape_ids
FROM unionized
JOIN gtfs_extra.all_routes ar USING (route_id);

CREATE MATERIALIZED VIEW gtfs_extra.all_trips AS
WITH trip_times AS (
    SELECT
        trip_id,
        min(arrival_time) AS trip_start_time,
        max(departure_time) AS trip_end_time,
        count(trip_id) AS trip_num_stops
    FROM gtfs.stop_times
    GROUP BY trip_id
)
SELECT
    trips.*,
    trip_start_time,trip_end_time,
    trip_end_time - trip_start_time AS trip_length,
    trip_num_stops
FROM gtfs.trips
JOIN trip_times USING (trip_id);
CREATE UNIQUE INDEX all_trips_trip_index ON gtfs_extra.all_trips (trip_id);

CREATE MATERIALIZED VIEW gtfs_extra.trip_blocks AS
WITH tmp AS (
    SELECT
        block_id,
        service_id,
        min(trip_start_time) AS block_start_time,
        max(trip_end_time) AS block_end_time,
        sum(trip_num_stops) AS block_num_stops,
        array_agg(trip_id ORDER BY trip_start_time) AS trips,
        array_agg(route_id ORDER BY trip_start_time) AS routes,
        array_agg(route_display ORDER BY trip_start_time) AS routes_display,
        array_agg(trip_headsign ORDER BY trip_start_time) AS headsigns
    FROM gtfs_extra.all_trips
    LEFT JOIN gtfs_extra.all_routes USING (route_id)
    GROUP BY block_id, service_id
)
SELECT
    block_id,
    service_id,
    block_start_time,
    block_end_time,
    block_end_time - block_start_time AS block_length,
    block_num_stops,
    trips,
    array_to_string(trips, ', ') AS trips_text,
    routes,
    array_to_string(routes, ', ') AS routes_text,
    routes_display,
    array_to_string(routes_display, ', ') AS routes_display_text,
    headsigns,
    array_to_string(headsigns, ', ') AS headsigns_text
FROM tmp;
CREATE INDEX trip_blocks_block_index ON gtfs_extra.trip_blocks (block_id);
CREATE INDEX trip_blocks_service_index ON gtfs_extra.trip_blocks (service_id);
CREATE UNIQUE INDEX trip_blocks_block_service_index ON gtfs_extra.trip_blocks (block_id, service_id);
