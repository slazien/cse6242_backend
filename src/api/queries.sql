CREATE OR REPLACE FUNCTION api_get_pois_for_city(
    city_id int, poi_category character)
    RETURNS TABLE
            (
                id       int,
                h3id     char,
                name     varchar,
                lat      real,
                long     real,
                category varchar
            )
    LANGUAGE plpgsql
AS
$poiforcity$
BEGIN
    RETURN QUERY
        SELECT pois.poiid, pois.h3id, pois.name, pois.lat, pois.long, pois.category
        FROM pois
                 JOIN cityh3map ON cityh3map.h3id = pois.h3id
                 JOIN cities ON cities.cityid = cityh3map.cityid
        WHERE cities.cityID = city_id
          AND pois.category = poi_category;
END;
$poiforcity$;

-- DROP TABLE IF EXISTS h3demographics;

-- CREATE TABLE public.h3demographics
-- (
--     cityid bigint,
--     categorytype text,
--     groupname text,
--     h3id character(15),
--     population double precision,
--     id bigserial,
--     CONSTRAINT h3demographics_id PRIMARY KEY (id),
--     CONSTRAINT unique_key UNIQUE (cityid, categorytype, groupname, h3id)
-- );

-- CREATE INDEX IF NOT EXISTS h3id_cityid ON public.h3demographics (cityid, h3id);

-- CREATE INDEX IF NOT EXISTS h3demographics_h3index ON public.h3demographics (h3id);

CREATE OR REPLACE FUNCTION api_get_demographics_for_city(
    in_cityid integer, in_categorytype character)
    RETURNS TABLE
            (
                h3id       char,
                groupname  text,
                population float
            )
    LANGUAGE plpgsql
AS
$demographicsforcity$
BEGIN

    RETURN QUERY
        SELECT h3demographics.h3id, h3demographics.groupname, h3demographics.population
        from h3demographics
        WHERE cityid = in_cityid
          and categorytype = in_categorytype;

END;
$demographicsforcity$;

SELECT *
FROM api_get_demographics_for_city(1, 'Race');

CREATE OR REPLACE FUNCTION api_get_demographics_for_catchment(
    in_categorytype character,
    in_catchment_id integer
)
    RETURNS TABLE
            (
                groupname  text,
                population float
            )
    LANGUAGE plpgsql
AS
$demographicsforarea$
BEGIN

    RETURN QUERY
        SELECT catchment_stats.groupname, catchment_stats.population
        FROM catchment_stats
        WHERE categorytype = in_categorytype
          AND catchmentid = in_catchment_id;

END;
$demographicsforarea$;

SELECT *
FROM api_get_demographics_for_catchment('Race', 1);

-- ACCESSIBILITY INDEX TESTING

DROP TABLE IF EXISTS accessibility_stats;

CREATE TABLE public.accessibility_stats
(
    id            bigserial,
    cityid        bigint,
    categorytype  text,
    groupname     text,
    poi_category  varchar(50),
    timeofday     varchar(50),
    h3id          char(15),
    accessibility double precision,
    CONSTRAINT acc_stats_id PRIMARY KEY (id),
    CONSTRAINT acc_stats_unique_key UNIQUE (cityid, categorytype, groupname, poi_category, timeofday, h3id)
);

CREATE INDEX IF NOT EXISTS acc_stats_agg_index ON public.accessibility_stats (cityid, categorytype, poi_category, timeofday, groupname);
CREATE INDEX IF NOT EXISTS acc_stats_h3index ON public.accessibility_stats (h3id);

-- == ADD/REMOVE POI TESTING GROUND

WITH catchmentid_to_total_population AS (
    SELECT catchmentid, SUM(population) AS pop_in_catchment
    FROM catchment_stats
    WHERE categorytype = 'Race' -- we can use any category, the population totals are the same for each catchment regardless of category
    GROUP BY catchmentid
),
     step1 AS (
         SELECT pois.category as poi_category,
                catchments.timeofday,
                catchment_stats.catchmentid,
                c3m.h3id,
                catchment_stats.categorytype,
                catchment_stats.groupname,
                CASE
                    WHEN population = 0
                        THEN 0
                    ELSE 10000 / catchmentid_to_total_population.pop_in_catchment
                    END       AS ratio
         FROM pois
                  JOIN catchments
                       ON pois.h3id = catchments.originh3id
                  JOIN catchment_stats
                       ON catchments.catchmentid = catchment_stats.catchmentid
                  JOIN catchmentid_to_total_population
                       ON catchmentid_to_total_population.catchmentid = catchments.catchmentid
                  JOIN catchmenth3map c3m on catchment_stats.catchmentid = c3m.catchmentid
     )
SELECT h3id, sum(ratio)
FROM step1
group by h3id, categorytype, groupname, timeofday;

WITH catchmentid_to_total_population AS (
    SELECT catchmentid, SUM(population) AS pop_in_catchment
    FROM catchment_stats
    WHERE categorytype = 'Race' -- we can use any category, the population totals are the same for each catchment regardless of category
    GROUP BY catchmentid
),
     step1 AS (
         SELECT pois.category as poi_category,
                catchments.timeofday,
                catchment_stats.catchmentid,
                c3m.h3id,
                catchment_stats.categorytype,
                catchment_stats.groupname,
                CASE
                    WHEN population = 0
                        THEN 0
                    ELSE 1 / catchmentid_to_total_population.pop_in_catchment
                    END       AS ratio
         FROM pois
                  JOIN catchments
                       ON pois.h3id = catchments.originh3id
                  JOIN catchment_stats
                       ON catchments.catchmentid = catchment_stats.catchmentid
                  JOIN catchmentid_to_total_population
                       ON catchmentid_to_total_population.catchmentid = catchments.catchmentid
                  JOIN catchmenth3map c3m on catchment_stats.catchmentid = c3m.catchmentid
     )
SELECT h3id, sum(ratio)
FROM step1
WHERE catchmentid IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
GROUP BY h3id, categorytype, groupname, timeofday;

CREATE OR REPLACE FUNCTION api_add_remove_catchments(
    in_catchmentids integer[]
)
    RETURNS TABLE
            (
                out_h3id         character,
                out_categorytype text,
                out_groupname    text,
                out_timeofday    varchar,
                out_poicategory  varchar,
                sum_ratio        float
            )
    LANGUAGE plpgsql
AS
$addremovecatchments$
BEGIN
    RETURN QUERY
        WITH catchmentid_to_total_population AS (
            SELECT catchmentid, SUM(population) AS pop_in_catchment
            FROM catchment_stats
            WHERE categorytype = 'Race' -- we can use any category, the population totals are the same for each catchment regardless of category
            GROUP BY catchmentid
        ),
             step1 AS (
                 SELECT pois.category as poi_category,
                        catchments.timeofday,
                        catchment_stats.catchmentid,
                        c3m.h3id,
                        catchment_stats.categorytype,
                        catchment_stats.groupname,
                        CASE
                            WHEN population = 0
                                THEN 0
                            ELSE 10000 / catchmentid_to_total_population.pop_in_catchment
                            END       AS ratio
                 FROM pois
                          JOIN catchments
                               ON pois.h3id = catchments.originh3id
                          JOIN catchment_stats
                               ON catchments.catchmentid = catchment_stats.catchmentid
                          JOIN catchmentid_to_total_population
                               ON catchmentid_to_total_population.catchmentid = catchments.catchmentid
                          JOIN catchmenth3map c3m on catchment_stats.catchmentid = c3m.catchmentid
             )
        SELECT step1.h3id, categorytype, groupname, timeofday, poi_category, sum(ratio)
        FROM step1
                 JOIN unnest(in_catchmentids) m(id) ON step1.catchmentid = m.id
        GROUP BY h3id, categorytype, groupname, timeofday, poi_category;

END;
$addremovecatchments$;

SELECT *
FROM api_add_remove_catchments(array [1, 2, 3])
WHERE out_categorytype = 'Race';

-- ===

WITH catchmentid_to_total_population AS (
    SELECT catchmentid, SUM(population) AS pop_in_catchment
    FROM catchment_stats
    WHERE categorytype = 'Race' -- we can use any category, the population totals are the same for each catchment regardless of category
    GROUP BY catchmentid
),
     step1 AS (
         SELECT pois.category as poi_category,
                catchments.timeofday,
                catchment_stats.catchmentid,
                catchment_stats.categorytype,
                catchment_stats.groupname,
                CASE
                    WHEN population = 0
                        THEN 0
                    ELSE 10000 / catchmentid_to_total_population.pop_in_catchment
                    END       AS ratio
         FROM pois
                  JOIN catchments
                       ON pois.h3id = catchments.originh3id
                  JOIN catchment_stats
                       ON catchments.catchmentid = catchment_stats.catchmentid
                  JOIN catchmentid_to_total_population
                       ON catchmentid_to_total_population.catchmentid = catchments.catchmentid
     ),

--- Data collection for step 2. We need to:
     ---  Get all h3ids in a city and their associated population attributes
     --- For each h3id, get the catchment IDs that cover those cells
     --- Note that some cells may not have a single catchment area covering them
     --- But we still need to make sure they are captured for statistics purposes

     h3_coverage as (
         SELECT h3demographics.h3id,
                h3demographics.cityid,
                h3demographics.categorytype,
                h3demographics.groupname,
                catchmenth3map.catchmentid
         FROM h3demographics
                  LEFT JOIN catchmenth3map
                            ON catchmenth3map.h3id = h3demographics.h3id
     )
INSERT
INTO accessibility_stats (h3id, cityid, categorytype, groupname, poi_category, timeofday, accessibility)
SELECT h3_coverage.h3id,
       h3_coverage.cityid,
       h3_coverage.categorytype,
       h3_coverage.groupname,
       step1.poi_category,
       step1.timeofday,
       SUM(step1.ratio) as accessibility
FROM h3_coverage
         LEFT JOIN step1
                   ON
                       h3_coverage.catchmentid = step1.catchmentid
WHERE h3_coverage.categorytype = step1.categorytype
  AND h3_coverage.groupname = step1.groupname
GROUP BY h3_coverage.h3id,
         h3_coverage.cityid,
         h3_coverage.categorytype,
         h3_coverage.groupname,
         step1.poi_category,
         step1.timeofday;

SELECT h.groupname,
       h.categorytype,
       a.poi_category,
       10000 * SUM(a.accessibility * h.population) / SUM(h.population) as metric
FROM accessibility_stats as a
         JOIN h3demographics as h
              ON h.h3id = a.h3id
                  AND h.groupname = a.groupname
                  AND h.categorytype = a.categorytype
                  AND h.cityid = a.cityid
WHERE h.categorytype = 'Race'
  AND a.cityid = 1
GROUP BY a.cityid,
         h.groupname,
         h.categorytype,
         a.poi_category
ORDER BY poi_category, metric DESC;

-- This row count should not change depending on the number of supplied catchment IDs to add/remove
SELECT count(*)
FROM api_add_remove_catchments(array [1, 2, 3]) c
         RIGHT JOIN accessibility_stats a ON
            c.out_h3id = a.h3id AND
            c.out_categorytype = a.categorytype AND
            c.out_groupname = a.groupname AND
            c.out_poicategory = a.poi_category AND
            c.out_timeofday = a.timeofday
WHERE out_categorytype = 'Race';
-- ORDER BY poi_category, metric DESC;