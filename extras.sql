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

CREATE MATERIALIZED VIEW gtfs_extra.service_blocks AS
WITH service_dates AS (
    SELECT
        date::date, array_agg(service_id ORDER BY service_id) AS service_ids
    FROM gtfs.service_days
    GROUP BY date
), blocks AS (
    SELECT
        block_id, array_agg(DISTINCT service_id ORDER BY service_id) AS service_ids
    FROM gtfs.trips
    GROUP BY block_id
), distinct_block_patterns AS (
    SELECT DISTINCT
        block_id,
        array(select unnest(blocks.service_ids)intersect
              select unnest(service_dates.service_ids) ORDER BY 1) AS service_id_intersect
    FROM service_dates
             JOIN blocks ON service_dates.service_ids && blocks.service_ids
)
SELECT
    CASE WHEN count(block_id) OVER (PARTITION BY block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 1 THEN block_id
         ELSE block_id || '-' || array_to_string(service_id_intersect, '-') END AS complete_block_id,
    block_id AS raw_block_id,
    service_id_intersect AS service_ids
FROM distinct_block_patterns;

CREATE MATERIALIZED VIEW gtfs_extra.date_services AS
SELECT
    date::date,
    array_agg(DISTINCT service_id ORDER BY service_id) AS service_ids
FROM gtfs.service_days
GROUP BY date;

CREATE MATERIALIZED VIEW gtfs_extra.date_blocks AS
SELECT DISTINCT
    date,
    first_value(complete_block_id) OVER (PARTITION BY date, raw_block_id ORDER BY array_length(ds.service_ids, 1) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS complete_block_id,
    raw_block_id
FROM gtfs_extra.date_services ds
JOIN gtfs_extra.service_blocks sb ON ds.service_ids @> sb.service_ids;

CREATE MATERIALIZED VIEW gtfs_extra.all_routes AS
WITH route_exchanges AS (
    SELECT
        route_id,
        array_agg(DISTINCT exchange_name ORDER BY exchange_name) FILTER ( WHERE exchange_name IS NOT NULL ) AS route_served_exchanges
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
        array_agg(DISTINCT ar.route_display ORDER BY ar.route_display) AS stop_route_names,
        array_to_string(array_agg(DISTINCT ar.route_display ORDER BY ar.route_display), ', ') AS stop_route_names_text,
        bool_or(ar.route_is_significant) AS stop_any_route_is_significant,
        max(ar.route_stop_coverage_radius) AS stop_coverage_radius,
        array_agg(DISTINCT exchange_name ORDER BY exchange_name) FILTER (WHERE exchange_name IS NOT NULL) AS stop_served_exchanges
    FROM gtfs.stops s
    JOIN gtfs_extra.stop_routes sr USING (stop_id)
    JOIN gtfs_extra.all_routes ar ON ar.route_id = ANY (sr.route_ids)
    LEFT JOIN tl_map_extras.station_exchange_stop_connections USING (stop_id)
    GROUP BY s.stop_id, sr.route_ids
)
SELECT
    s.*,
    municipality AS stop_extras_municipality,
    sub_region AS stop_extras_sub_region,
    region_district AS stop_extras_region_district,
    tspr_sub_region AS stop_extras_tspr_sub_region,
    stop_route_ids, stop_route_names, stop_route_names_text, stop_coverage_radius,
    stop_served_exchanges, array_to_string(stop_served_exchanges, ', ') AS stop_served_exchanges_text,
    st_transform(st_buffer(st_transform(stop_loc::geometry, 3857), stop_coverage_radius), 4326) AS stop_coverage,
    st_transform(st_buffer(st_transform(stop_loc::geometry, 3857), 400), 4326) AS stop_400m_radius
FROM compiled_stop_routes csr
JOIN gtfs.stops s USING (stop_id)
LEFT JOIN tl_map_extras.stops es USING (stop_id);
CREATE UNIQUE INDEX all_stops_stop_id_index ON gtfs_extra.all_stops (stop_id);
CREATE INDEX all_stops_loc ON gtfs_extra.all_stops USING GIST (stop_loc);

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
        count(trip_id) AS trip_num_stops,
        array_agg(stop_id ORDER BY stop_sequence) AS trip_stop_ids,
        array_agg(DISTINCT stop_id ORDER BY stop_id) AS trip_distinct_stop_ids,
        array_agg(DISTINCT exchange_name ORDER BY exchange_name) FILTER ( WHERE  exchange_name IS NOT NULL ) AS trip_served_exchanges
    FROM gtfs.stop_times
    LEFT JOIN tl_map_extras.station_exchange_stop_connections USING (stop_id)
    GROUP BY trip_id
), trip_agg AS (
    SELECT
        trip_id,
        array_agg(DISTINCT sb.complete_block_id ORDER BY sb.complete_block_id) AS complete_block_ids
    FROM gtfs.trips
    JOIN gtfs_extra.service_blocks sb ON trips.block_id = sb.raw_block_id AND trips.service_id = ANY (sb.service_ids)
    GROUP BY trip_id
)
SELECT
    trips.*,
    trip_start_time,trip_end_time,
    trip_end_time - trip_start_time AS trip_length,
    trip_num_stops,
    trip_stop_ids, array_to_string(trip_stop_ids, ', ') AS trip_stop_ids_text,
    trip_distinct_stop_ids, array_to_string(trip_distinct_stop_ids, ', ') AS trip_distinct_stop_ids_text,
    trip_served_exchanges, array_to_string(trip_served_exchanges, ', ') AS trip_served_exchanges_text,
    complete_block_ids, array_to_string(complete_block_ids, ', ') AS complete_block_ids_text,
    md5(encode((route_id || trip_headsign || array_to_string(trip_stop_ids, '/'))::bytea, 'base64')) AS pattern_id,
    shape
FROM gtfs.trips
JOIN trip_times USING (trip_id)
JOIN trip_agg USING (trip_id)
JOIN gtfs.shapes_aggregated USING (shape_id);
CREATE UNIQUE INDEX all_trips_trip_index ON gtfs_extra.all_trips (trip_id);
CREATE INDEX all_trips_block_id_index ON gtfs_extra.all_trips (block_id);
CREATE INDEX all_trips_service_id_index ON gtfs_extra.all_trips (service_id);
CREATE INDEX all_trips_shape ON gtfs_extra.all_trips USING GIST (shape);

CREATE MATERIALIZED VIEW gtfs_extra.trip_service_blocks AS
SELECT
    trip_id,
    unnest(complete_block_ids) AS complete_block_id
FROM gtfs_extra.all_trips;

CREATE MATERIALIZED VIEW gtfs_extra.trip_blocks AS
WITH ordered_trips AS (
    SELECT
        complete_block_id,
        array_agg(trip_id ORDER BY trip_start_time) AS trips
    FROM gtfs_extra.trip_service_blocks
    JOIN gtfs_extra.all_trips USING (trip_id)
    GROUP BY complete_block_id
), tmp AS (
    SELECT
        ordered_trips.complete_block_id,
        min(at.trip_start_time) AS block_start_time,
        max(trip_end_time) AS block_end_time,
        count(ut.trip_id) AS block_num_trips,
        sum(trip_num_stops) AS block_num_stops,
        sum(trip_length) AS block_in_service_time,
        trips,
        array_agg(route_id ORDER BY trip_index) AS routes,
        array_agg(DISTINCT route_id ORDER BY route_id) AS distinct_routes,
        array_agg(route_display ORDER BY trip_index) AS routes_display,
        array_agg(DISTINCT route_display ORDER BY route_display) AS distinct_routes_display,
        array_agg(trip_headsign ORDER BY trip_index) AS headsigns,
        array_agg(DISTINCT trip_headsign ORDER BY trip_headsign) AS distinct_headsigns,
        array_agg(pattern_id ORDER BY trip_index) AS block_pattern_ids,
        array_agg(DISTINCT pattern_id ORDER BY pattern_id) AS distinct_block_pattern_ids,
        st_union(array_agg(shape ORDER BY trip_index)) AS shape
    FROM ordered_trips
    CROSS JOIN unnest(trips) WITH ORDINALITY AS ut(trip_id, trip_index)
    JOIN gtfs_extra.all_trips at ON at.trip_id = ut.trip_id
    JOIN gtfs_extra.all_routes USING (route_id)
    GROUP BY ordered_trips.complete_block_id, trips
)
SELECT
    complete_block_id,
    raw_block_id,
    service_ids,
    block_start_time,
    block_end_time,
    block_end_time - block_start_time AS block_length,
    block_in_service_time,
    block_end_time - block_start_time - block_in_service_time AS block_not_in_service_time,
    block_num_stops,
    block_num_trips,
    trips,
    array_to_string(trips, ', ') AS trips_text,
    routes,
    array_to_string(routes, ', ') AS routes_text,
    distinct_routes,
    array_to_string(distinct_routes, ', ') AS distinct_routes_text,
    routes_display,
    array_to_string(routes_display, ', ') AS routes_display_text,
    distinct_routes_display,
    array_to_string(distinct_routes_display, ', ') AS distinct_routes_display_text,
    headsigns,
    array_to_string(headsigns, ', ') AS headsigns_text,
    distinct_headsigns,
    array_to_string(distinct_headsigns, ', ') AS distinct_headsigns_text,
    block_pattern_ids,
    array_to_string(block_pattern_ids, ', ') AS block_pattern_ids_text,
    distinct_block_pattern_ids,
    array_to_string(distinct_block_pattern_ids, ', ') AS distinct_block_pattern_ids_text,
    shape,
    st_linemerge(shape) AS shape_merged
FROM tmp
JOIN gtfs_extra.service_blocks USING (complete_block_id);
CREATE UNIQUE INDEX trip_blocks_block_index ON gtfs_extra.trip_blocks (complete_block_id);
CREATE INDEX trip_blocks_service_index ON gtfs_extra.trip_blocks (raw_block_id);
CREATE INDEX trip_blocks_shape ON gtfs_extra.trip_blocks USING GIST (shape);
CREATE INDEX trip_blocks_shape_merged ON gtfs_extra.trip_blocks USING GIST (shape_merged);
