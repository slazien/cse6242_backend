-- Accessibility index

CREATE OR REPLACE FUNCTION calculate_accessibility_index(
    in_cityname character, in_category character)
    RETURNS TABLE
            (
                h3id_out             varchar,
                groupname_out        text,
                poi_to_pop_ratio_out numeric
            )
    LANGUAGE plpgsql
AS
$procout$
BEGIN
    CREATE OR REPLACE FUNCTION get_h3ids_for_city(in_cityname character)
        RETURNS TABLE
                (
                    ids character
                )
        LANGUAGE plpgsql
    AS
    $funcout$
    DECLARE
        var_city_id integer;
    BEGIN
        -- get city id based on city name
        SELECT cityid INTO var_city_id FROM cities WHERE cityname = in_cityname;
        -- get h3 ids for that city
        RETURN QUERY
            SELECT h3id FROM cityh3map WHERE cityid = var_city_id;
    END;
    $funcout$;

    -- get h3 IDs for the selected city
    CREATE TEMP TABLE city_h3_ids ON COMMIT DROP AS
    SELECT *
    FROM get_h3ids_for_city(in_cityname);

    -- nothing fancy, count POIs for each catchment ID (numerator for accessibility index)
    CREATE TEMP TABLE catchment_to_poi_count ON COMMIT DROP AS
        -- CREATE OR REPLACE VIEW catchment_to_poi_count AS
        (
            SELECT c3m.catchmentid, COUNT(poiid) AS count_poi
            FROM pois
                     JOIN catchmenth3map c3m ON pois.h3id = c3m.h3id
-- make sure POIs are in a given city
            WHERE pois.h3id IN (SELECT ids FROM city_h3_ids)
            GROUP BY c3m.catchmentid
        );

    -- census block group ID to population per overlapping hex
-- when later mapping catchment ID to sum(population), we need to distribute the population of each catchment
-- by calculating (sum(population of catchment) / # of overlapping hexes) for that catchment ID
-- this will ensure that we're not over-counting the population for a given set of overlapping hexes
    CREATE TEMP TABLE census_to_pop_per_h3 ON COMMIT DROP AS
        -- CREATE OR REPLACE VIEW census_to_pop_per_h3 AS
        (
-- map census ID to overlapping hex count
            WITH census_to_hex_count AS (
                SELECT censusblockgroupid,
                       COUNT(h3id
                           ) AS count_h3
                FROM censush3map
                WHERE h3id IN (SELECT ids FROM city_h3_ids)
                GROUP BY censusblockgroupid
            ),
                 -- map census ID to population
                 census_to_pop AS (
                     SELECT censusblockgroupid,
                            demographics.groupname,
                            SUM(total
                                ) AS sum_pop
                     FROM demographics
                     WHERE categorytype = in_category
                     GROUP BY censusblockgroupid, demographics.groupname
                 )
-- mapping from census block ID to population per overlapping hex (distributed evenly: sum of pop / # of overlapping hex)
            SELECT cp.censusblockgroupid AS censusblockgroupid, groupname, sum_pop / count_h3 AS pop_in_hex
            FROM census_to_pop cp
                     JOIN census_to_hex_count ch ON cp.censusblockgroupid = ch.censusblockgroupid
        );

    -- map hex ID to population in that hex (as distributed uniformly in the previous step)
    CREATE TEMP TABLE hex_to_pop ON COMMIT DROP AS
        -- CREATE OR REPLACE VIEW hex_to_pop AS
        (
            SELECT h3id, groupname, pop_in_hex
            FROM census_to_pop_per_h3
                     JOIN censush3map c3m on census_to_pop_per_h3.censusblockgroupid = c3m.censusblockgroupid
        );

    -- map the catchment ID to the sum of populations from hexes overlapping with that catchment ID
    CREATE TEMP TABLE catchment_to_pop ON COMMIT DROP AS
        -- CREATE OR REPLACE VIEW catchment_to_pop AS
        (
            SELECT catchmentid, groupname, SUM(pop_in_hex) AS sum_pop
            FROM hex_to_pop
                     JOIN catchmenth3map c3m on hex_to_pop.h3id = c3m.h3id
            GROUP BY c3m.catchmentid, hex_to_pop.groupname
        );

    -- map catchment ID to POI/population ratio (accessibility index)
    CREATE TEMP TABLE catchment_to_acc_idx ON COMMIT DROP AS
        -- CREATE OR REPLACE VIEW catchment_to_accessibility_index AS
        (
            SELECT ctp.catchmentid AS catchmentid, groupname, count_poi / nullif(sum_pop, 0) AS poi_to_pop_ratio
            FROM catchment_to_poi_count
                     JOIN catchment_to_pop ctp on catchment_to_poi_count.catchmentid = ctp.catchmentid
        );

    -- for each h3id, sum accessibility index from all overlapping catchment areas
    CREATE TEMP TABLE hex_id_to_acc_idx ON COMMIT DROP AS
        (
            SELECT h3id,
                   groupname,
                   SUM(poi_to_pop_ratio) AS sum_accessibility_index
            FROM catchment_to_acc_idx caidx
                     JOIN catchmenth3map c on caidx.catchmentid = c.catchmentid
            GROUP BY h3id, groupname
        );

    RETURN QUERY
        SELECT * FROM hex_id_to_acc_idx;
END;
$procout$;

-- SELECT COUNT(*) FROM calculate_accessibility_index('Atlanta', 'Race');

-- H3 ID to catchment ID mapping for a given city

CREATE OR REPLACE FUNCTION get_h3_to_catchment_mapping(
    in_cityname character)
    RETURNS TABLE
            (
                h3id_out         varchar,
                catchment_id_out integer
            )
    LANGUAGE plpgsql
AS
$hextocatchment$
BEGIN
    -- Get city ID
    CREATE TEMP TABLE temp_cityid ON COMMIT DROP AS
        (
            SELECT cityid
            FROM cities
            WHERE cityname = in_cityname
        );

    -- Get H3 IDs for the city
    CREATE TEMP TABLE temp_h3_ids_for_city ON COMMIT DROP AS (
        SELECT h3id FROM cityh3map WHERE cityid IN (SELECT cityid FROM temp_cityid)
    );

    -- Get H3 to catchment ID mapping for the city
    DROP TABLE IF EXISTS temp_h3_to_catchment_id;
    CREATE TEMP TABLE temp_h3_to_catchment_id ON COMMIT DROP AS (
        SELECT h3id, catchmentid FROM catchmenth3map WHERE h3id IN (SELECT h3id FROM temp_h3_ids_for_city)
    );

    RETURN QUERY
        SELECT * FROM temp_h3_to_catchment_id;
END;
$hextocatchment$;

SELECT * FROM get_h3_to_catchment_mapping('Atlanta');

-- POI data for a given city

CREATE OR REPLACE FUNCTION get_pois_for_city(
    in_cityname character)
    RETURNS TABLE
            (
                h3id_out     char,
                name_out     varchar,
                category_out varchar
            )
    LANGUAGE plpgsql
AS
$poiforcity$
BEGIN
    -- Get city ID
    CREATE TEMP TABLE temp_cityid ON COMMIT DROP AS
        (
            SELECT cityid
            FROM cities
            WHERE cityname = in_cityname
        );

    -- Get H3 IDs for the city
    CREATE TEMP TABLE temp_h3_ids_for_city ON COMMIT DROP AS (
        SELECT h3id FROM cityh3map WHERE cityid IN (SELECT cityid FROM temp_cityid)
    );

    -- Get POIs for the city
    CREATE TEMP TABLE temp_poi_for_city ON COMMIT DROP AS (
        SELECT h3id, name, category FROM pois WHERE h3id IN (SELECT h3id FROM temp_h3_ids_for_city)
    );

    RETURN QUERY
        SELECT * FROM temp_poi_for_city;
END;
$poiforcity$;

SELECT *
FROM get_pois_for_city('Atlanta');

-- Demographics for a city

CREATE OR REPLACE FUNCTION get_demographics_for_city(
    in_cityname character, in_categorytype character)
    RETURNS TABLE
            (
                h3id_out      text,
                groupname_out text,
                total_out     bigint
            )
    LANGUAGE plpgsql
AS
$demographicsforcity$
BEGIN
    -- Get city ID
    CREATE TEMP TABLE temp_cityid ON COMMIT DROP AS
        (
            SELECT cityid
            FROM cities
            WHERE cityname = in_cityname
        );

    -- Get H3 IDs for the city
    CREATE TEMP TABLE temp_h3_ids_for_city ON COMMIT DROP AS (
        SELECT h3id FROM cityh3map WHERE cityid IN (SELECT cityid FROM temp_cityid)
    );

    -- Get census block group to H3 mapping for the city
    CREATE TEMP TABLE temp_census_to_h3 ON COMMIT DROP AS (
        SELECT * FROM censush3map WHERE h3id IN (SELECT h3id FROM temp_h3_ids_for_city)
    );

    -- Get H3 to demographics mapping for the city
    CREATE TEMP TABLE temp_demographics_for_city ON COMMIT DROP AS (
        SELECT h3id, groupname, total
        FROM demographics
                 JOIN temp_census_to_h3 c2h3 ON c2h3.censusblockgroupid = demographics.censusblockgroupid
        WHERE demographics.categorytype = in_categorytype
    );

    RETURN QUERY
        SELECT * FROM temp_demographics_for_city;
END;
$demographicsforcity$;

-- SELECT * FROM get_demographics_for_city('Atlanta', 'Race');

-- City list (for tooltip)
CREATE OR REPLACE FUNCTION get_citylist()
    RETURNS TABLE
            (
                cityid_out   integer,
                cityname_out varchar
            )
    LANGUAGE plpgsql
AS
$citylist$
BEGIN
    RETURN QUERY
        SELECT cityid, cityname FROM cities;
END;
$citylist$;

-- SELECT * FROM get_citylist();
