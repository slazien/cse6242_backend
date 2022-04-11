from sqlalchemy import Table, MetaData
from geoalchemy2.shape import from_shape, to_shape
from geoalchemy2.elements import WKBElement
import requests
import geopandas as gpd
import h3
from shapely.geometry import Polygon, MultiPolygon, mapping

class IsochroneService():

    def __init__(
        self, 
        times = None, 
        reference_date = '02-24-2022',
        walk_reluctance = 5,
        max_walk_distance = 1500,
        min_transfer_time = 120,
        mode = 'WALK,TRANSIT',
        h3_resolution = 9,
        otp_port = 8801,
        pg_conn = None
    ):
        if pg_conn is None:
            print("Postgress connection not provided, can only use compute_isochrone() function")
        
        if times is None:
            times = {'morning': "08:00am", 'afternoon': "2:00pm", 'evening': "8:00pm"}
        self.times = times
        self.ref_date = reference_date
        self.walk_reluctance = walk_reluctance
        self.min_transfer_time = min_transfer_time
        self.mode = mode
        self.max_walk_distance = max_walk_distance
        self.otp_port = otp_port
        self.pg_conn = pg_conn
        self.h3_resolution = h3_resolution
    

    def compute_isochrone(self, lat, lon, city='atlanta', time='morning', minutes=30):
        #documentation @ http://dev.opentripplanner.org/apidoc/1.5.0/resource_LIsochrone.html
        
        assert time in self.times, "Invalid time category. Provided {}, should be one of {}".format(time, ", ".join(self.times))
        time_spec = self.times[time]

        api_endpoint = 'http://localhost:{}/otp/routers/{}/isochrone'.format(self.otp_port, city)    

        response = requests.get(
            api_endpoint,
            params= {
                'fromPlace': '{},{}'.format(lat, lon), #lat long pair of starting point
                'mode': self.mode,
                'date': self.ref_date,
                'time': time_spec,
                'maxWalkDistance': self.max_walk_distance,
                'walkReluctance': self.walk_reluctance,
                'minTransferTime': self.min_transfer_time,
                'cutoffSec': minutes * 60
            },
        )

        no_route = 'org.opentripplanner.routing.error.VertexNotFoundException: vertices not found: [from] vertices not found: [from]'

        if response.status_code == 200:
            df = gpd.read_file(response.text)
            isochrone = df.loc[0, 'geometry']
            
        elif response.status_code == 500 and response.text == no_route:
            #return the shape of the h3 cell and its' neighbours
            h3_origin = h3.geo_to_h3(lat, lon, resolution=9)
            neighbours = h3.k_ring(h3_origin, 1)
            polygon = h3.h3_set_to_multi_polygon(neighbours, geo_json=True)
            isochrone =  MultiPolygon([Polygon(polygon[0][0])])
        
        return isochrone


    def get_isochrone(self, city_id, h3_id, time='morning', minutes=30):
        
        assert time in self.times, "Invalid time category. Provided {}, should be one of {}".format(time, ", ".join(self.times))        
        assert self.pg_conn is not None, "No Postgres connection available"

        #retrieve H3 index and city name of the given POI
        with self.pg_conn.connection.cursor() as cur:
            cur.execute("""
                SELECT cityName, catchments.originh3id, catchments.catchmentid, catchments.geometry FROM cities 
                    JOIN cityh3map ON cities.cityid = cityh3map.cityid                     
                    LEFT JOIN catchments ON cityh3map.h3id = catchments.originh3id
                    WHERE 
                        cities.cityid = %s and (catchments.originh3id = %s OR catchments.originh3id IS NULL) and
                        (timeofday = %s OR timeofday IS NULL) and (timedistance = %s OR timedistance IS NULL)
                """, (city_id, h3_id, time, minutes)
            )

            result = cur.fetchone()
                                
        #isochrone already in DB - return
        if result[1] is not None:                
            isochrone = to_shape(WKBElement(result[3]))
            catchment_id = result[2]
        
        #compute the isochrone,save to DB and return
        else:
            cityname = result[0].lower()                
            lat, lon = h3.h3_to_geo(h3_id)                
            isochrone = self.compute_isochrone(lat, lon, cityname, time, minutes)

            #save the isochrone to database
            metadata = MetaData(bind=self.pg_conn, schema='public')
            metadata.reflect(only=['catchments', 'catchmenth3map'])
            catchments = Table('catchments', metadata)
            
            vals = [ 
                {"timeofday": time,
                "timedistance" : minutes,
                "geometry": from_shape(isochrone),
                "originh3id":h3_id}
            ]
            res = self.pg_conn.execute(catchments.insert(), vals)
            catchment_id = res.inserted_primary_key[0]

            #find all h3 indices in the isochrone and save them to DB, too
            h3s = [h3.polyfill_geojson(mapping(polygon), res=self.h3_resolution) for polygon in isochrone.geoms]
            h3s = set().union(*h3s)
            

            catchment_map = Table('catchmenth3map', metadata)
            self.pg_conn.execute(
                catchment_map.insert(), 
                [{"catchmentid": catchment_id, "h3id" : h} for h in h3s]
            )
            #calculate catchment statistics and add them to the catchment_stats table and step1_stats table, too
            self.update_stats(catchment_id)                

        return isochrone, h3_id, catchment_id
    
    def update_stats(self, catchment_id):
        catchment_stats = """
        WITH all_h3_ids as (
            SELECT 		
                h3demographics.categorytype,
                h3demographics.h3id, 
                h3demographics.groupname, 
                h3demographics.population 
            FROM h3demographics
        ),

        catchmenth3_ids AS (
            SELECT 
                catchments.catchmentid,
                catchmenth3map.h3id
            FROM catchments
            JOIN catchmenth3map ON catchmenth3map.catchmentid = catchments.catchmentid WHERE catchments.catchmentid = %s
        )

        INSERT INTO catchment_stats (catchmentid, categorytype, groupname, population)
            SELECT c.catchmentid, h.categorytype, h.groupname, SUM(h.population)
                FROM all_h3_ids as h
                JOIN catchmenth3_ids as c ON c.h3id = h.h3id
                GROUP BY c.catchmentid, h.categorytype, h.groupname;

        WITH step1 AS (
         SELECT 
                c3m.h3id,                
                catchments.timeofday,
                catchments.catchmentid,
                catchment_stats.categorytype,		 
				CASE 
            		WHEN SUM(catchment_stats.population) = 0
            		THEN 0 
            		ELSE 10000 / SUM(catchment_stats.population) 
            		END AS ratio
         FROM catchments
		 JOIN catchment_stats
         	ON catchment_stats.catchmentid = catchments.catchmentid                
         JOIN catchmenth3map c3m 
		 	ON catchment_stats.catchmentid = c3m.catchmentid
        WHERE c3m.catchmentid = %s
		 GROUP BY 
		 	c3m.h3id,                
            catchments.timeofday,
            catchments.catchmentid,
            catchment_stats.categorytype
     )
	 
	 INSERT INTO step1_stats 
	 (h3id,		
			timeofday,
			categorytype,			
			catchmentid,
			ratio
	 )
	 	SELECT 
            h3id,		
			timeofday,
			categorytype,			
			catchmentid,
			ratio
		 FROM step1;
        """
        with self.pg_conn.connection.cursor() as cur:
            cur.execute(catchment_stats, (catchment_id, catchment_id))