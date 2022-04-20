
import psycopg2 as pg
from psycopg2.extras import DictCursor
import asyncio

from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
import sys
sys.path.append("../")


import isochrones as isc
from sqlalchemy import create_engine
from shapely.geometry import mapping


#please see @ https://github.com/tiangolo/fastapi/issues/1359#issuecomment-927789546 on what we are using to speed up FastAPI
from fastapi import FastAPI, Body, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel as PydanticBaseModel
import orjson
from bson import ObjectId

def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default, option=orjson.OPT_NON_STR_KEYS).decode()


class BaseModel(PydanticBaseModel):
    class Config:
        json_load = orjson.loads
        json_dumps = orjson_dumps
        json_encoders = {ObjectId: lambda x: str(x)}

class PydanticJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        if content is None:
            return b""
        if isinstance(content, bytes):
            return content
        if isinstance(content, BaseModel):
            return content.json(by_alias=True).encode(self.charset)        
        return content.encode(self.charset)

class backendApi:
    def __init__(self, db_params, otp_port = 8062) -> None:
        assert 'dbname' in db_params, "Database parameters do not include 'dbname'"
        assert 'user' in db_params, "Database parameters do not include 'user'"
        assert 'password' in db_params, "Database parameters do not include 'password'"
        assert 'host' in db_params, "Database parameters do not include 'host'"
        assert 'port' in db_params, "Database parameters do not include 'port'"
        self.db_params = db_params
        self.otp_port = otp_port

    
    def get_db_connection(self):
        return pg.connect(**self.db_params)

    def get_alchemy_engine(self):
        conn_string = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**self.db_params)
        return create_engine(conn_string, echo=False, future=True)

    def get_app(self):
        app = FastAPI()
        
        class City(BaseModel):
            name: str
            id: int            

        
        @app.get("/cities", response_model=List[City])
        async def cities():
            """Returns a list cities available in the database.
            """
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT cityID, cityname FROM cities")
                    data = cur.fetchall()
                    print(data)
            citiesList = [{
                "id": d[0], 
                "name": d[1],                 
                } for d in data]            
            return citiesList

        
        @app.get("/poi_categories", response_model=List[str])
        async def poi_categories():
            """Returns a list POI categories available in the database.
            """
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT category FROM pois")
                    data = cur.fetchall()
            categories = [d[0] for d in data]
            return categories

        @app.get("/times_of_day", response_model=List[str])
        async def times_of_day():
            """Returns a list of time of day selections (for catchment area calculations) available in the database.
            """
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT TImeOfDay FROM catchments")
                    data = cur.fetchall()              
            return [d[0] for d in data]

        @app.get("/demographics_categories", response_model=List[str])
        async def demographic_categories():
            """Returns a list of demographic segments available in the database.
            """
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT CategoryType FROM demographics")
                    data = cur.fetchall()              
            return [d[0] for d in data]

        class Configuration(BaseModel):
            cities: List[City]
            times_of_day: List[str]
            poi_categories: List[str]
            demographic_categories: List[str]

        @app.get("/configuration", response_model=Configuration)
        async def configuration():
            """Returns the global configuration object that contains possible values the user can choose from in the front-end.
            """
            res = await asyncio.gather(
                cities(),
                times_of_day(),
                poi_categories(),
                demographic_categories(),
            )

            labels = ['cities', 'times_of_day', 'poi_categories', 'demographic_categories']
            # res = [[{'id': 1, 'name': 'Atlanta'}], ['morning'], ['Schools and Kindergartners', 'Grocery stores and supermarkets', 'Cinemas and Theaters', 'Clinics and Hospitals', 'Restaurants', 'Vaccination centre'], ['Race', 'Age and Sex', 'Income', 'Origin', 'Vehicle Availability']]
            config_values = dict(zip(labels, res))
            return config_values

        class coordinates(BaseModel):
            lat: float
            long: float


        class POI(BaseModel):
            id: int
            name: str
            h3id: str
            category: str
            coords: coordinates

        class POIList(BaseModel):
            data: List[POI]

        @app.get("/pois/{city_id}/{poi_category}", response_model = POIList)
        async def get_pois_in_city(city_id: int, poi_category: str, native: bool = False, pois_to_exclude = []):
            """Get all POIs of a given category in a city.
            """
            with self.get_db_connection() as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    if pois_to_exclude:
                        sql = 'SELECT id, h3id, name, lat, long, category FROM api_get_pois_for_city(%s, %s) WHERE h3id NOT IN %s'                    
                        cur.execute(sql, (city_id, poi_category, tuple(pois_to_exclude)))
                    else:
                        sql = 'SELECT id, h3id, name, lat, long, category FROM api_get_pois_for_city(%s, %s)'                    
                        cur.execute(sql, (city_id, poi_category))
                    response = POIList.construct(data = [])
                    data = cur.fetchall()                  
                    for row in data:                        
                        obj = dict(row)             
                        #remap lat/long into coordinates
                        obj['coords'] = coordinates(lat = row['lat'], long=row['long'])
                        response.data.append(POI.construct(**obj))  
            
            if native:
                return response
            else:                       
                return PydanticJSONResponse(content=response)

        
        class H3Grid(BaseModel):
            h3id: str
            data: Optional[dict]
            total: float
            accessibility: float

        class H3List(BaseModel):
            data: List[H3Grid]        

        @app.get("/demographics/{city_id}/{demographics_category}/{poi_category}/{time_of_day}", response_model=H3List)
        async def get_city_demographics(city_id: int, demographics_category: str, poi_category: Optional[str], time_of_day: Optional[str], detailed: int = 0, native: bool = False):
            """ Returns requested demographic data for all H3 cells in the city. 
            Use detailed=False to retrieve totals per h3cell for initial drawing and detailed=True to get details by category for tooltips.
            """
            detailed = (detailed != 0)            
            with self.get_db_connection() as conn:                
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    if detailed:
                        # sql = 'SELECT h3id, groupname, population from api_get_demographics_for_city(%s, %s)'
                        sql = """
                        SELECT d.h3id AS h3id, d.groupname AS groupname, d.population AS population, a.accessibility AS accessibility
                        FROM api_get_demographics_for_city(%s, %s) as d
                        LEFT JOIN accessibility_stats a ON d.h3id = a.h3id
                        AND a.cityid = %s AND a.categorytype = %s AND a.timeofday = %s and a.poi_category = %s;
                        """
                        cur.execute(sql, (city_id, demographics_category, city_id, demographics_category, time_of_day, poi_category))
                    else:
                        sql = "SELECT h3id, 'total' as groupname, SUM(population) as population from api_get_demographics_for_city(%s, %s) GROUP BY h3id"
                        cur.execute(sql, (city_id, demographics_category))
                    response = {}
                    data = cur.fetchall()

                    for row in data:                        
                        #check if H3cell does not exist yet - create then
                        id = row['h3id']
                        if id not in response:
                            total = 0 if detailed else row['population']
                            data = {} if detailed else None
                            h3_obj = H3Grid.construct(h3id = id, data = data, total=total)                            
                            response[id] = h3_obj
                        
                        #add population data
                        if detailed:
                            groupname = row['groupname']
                            population = row['population']
                            accessibility = row["accessibility"]
                            response[id].data[groupname] = population
                            response[id].total += population
                            response[id].accessibility = accessibility
                    
                    response = H3List.construct(data = list(response.values()))
                        
            if native:
                return response
            else:                       
                return PydanticJSONResponse(content=response)

        
        class MultiPolygon(BaseModel):
            type: str
            coordinates: List[List[List[Tuple[float, float]]]]

        
        class CatchmentArea(BaseModel):
            origin_h3id: str            
            population_total: float            
            population_detail: dict
            geometry: MultiPolygon
            

        @app.get("/catchment/{city_id}/{h3_id}", response_model = CatchmentArea)
        async def get_catchment_details(city_id, h3_id, time_of_day, demographics_category):
            """ Returns catchment area geometry and associated population details"""
            with self.get_alchemy_engine().connect() as conn:
                service = isc.IsochroneService(otp_port=8062, pg_conn=self.get_alchemy_engine().connect()   )    
                isochrone, origin_h3id, catchment_id = service.get_isochrone(city_id = city_id, h3_id = h3_id, time=time_of_day)
                
                with conn.connection.cursor(cursor_factory=DictCursor) as cur:
                    sql = 'SELECT groupname, population FROM api_get_demographics_for_catchment(%s, %s)'            
                    cur.execute(sql, (demographics_category, catchment_id))
                    data = cur.fetchall()                    
                                
            population_details = {row['groupname']: row['population'] for row in data}
            population_total = sum(population_details.values())
            area = CatchmentArea.construct(
                geometry = mapping(isochrone),                
                origin_h3id = origin_h3id,
                population_total = population_total,
                population_detail = population_details,
            )
            return PydanticJSONResponse(content=area)

        class CityStats(BaseModel):
            index_total: float
            index_detail: Dict[str, float]


        @app.get("/city_stats/{city_id}", response_model = CityStats)
        async def get_city_accessibility_statistics(
            city_id, 
            demographics_category, 
            time_of_day, 
            poi_category, 
            pois_added = [],
            pois_removed = []
        ):  
            """Returns overall city accessibility statistics and a breakdown by demographics category"""          
            with self.get_db_connection() as conn:                
                with conn.cursor(cursor_factory=DictCursor) as cur:                    
                    sql = 'SELECT groupn, metric, population FROM api_get_city_stats(%s, %s, %s, %s, %s, %s)'                    
                    cur.execute(sql, 
                    (city_id, poi_category, time_of_day, demographics_category, pois_removed, pois_added))                    
                    data = cur.fetchall()
                    details = {d['groupn'] : d['metric'] for d in data}
                    total = sum(d['metric'] * d['population'] for d in data) / sum([d['population'] for d in data])

            
            return CityStats(index_total = total, index_detail = details)

        class ConfigSet(BaseModel):
            poi_category: str
            demographic_category: str
            time_of_day: str
            city_id: int

        class DataFields(str, Enum):
            pois = 'poi_category'
            demographics = 'demographic_category'
            poi_list = 'poi_list' 
            time_of_day = 'time_of_day'
            poi_add = 'poi_add'
            poi_remove = 'poi_remove'

        class UpdatedPois(BaseModel):
            added: Optional[List[str]]
            deleted: Optional[List[str]]
        
        class UpdatePack(BaseModel):
            changed: List[DataFields]
            config: ConfigSet
            poi_list: UpdatedPois

        class CityData(BaseModel):
            demographics: Optional[H3List]
            pois: Optional[POIList]
            stats: Optional[CityStats]
            lat: float
            long: float


        @app.post("/city_data/", response_model=CityData)
        async def get_city_data(update_pack: UpdatePack = Body(..., embed=False)):
            queries = {}
            city_id = update_pack.config.city_id

            if DataFields.demographics in update_pack.changed:
                queries['demographics'] = get_city_demographics(
                    city_id=city_id, 
                    demographics_category=update_pack.config.demographic_category, 
                    poi_category=update_pack.config.poi_category,
                    time_of_day=update_pack.config.time_of_day,
                    detailed=True,
                    native=True
                )
            
            if DataFields.pois in update_pack.changed or DataFields.poi_remove in update_pack.changed:
                queries['pois'] = get_pois_in_city(
                    city_id = city_id, 
                    poi_category= update_pack.config.poi_category,
                    native=True,
                    pois_to_exclude=update_pack.poi_list.deleted
                )
                        
            if update_pack.changed or update_pack.poi_list.added or update_pack.poi_list.deleted:
                queries['stats'] = get_city_accessibility_statistics(
                    city_id=city_id, 
                    demographics_category=update_pack.config.demographic_category, 
                    time_of_day = update_pack.config.time_of_day, 
                    poi_category = update_pack.config.poi_category,
                    pois_added=update_pack.poi_list.added,
                    pois_removed=update_pack.poi_list.deleted
                )

            #check if we have isochrones for all POIs that were added
            #issue requests to create them on the fly as needed
            if update_pack.poi_list.added:
                sql = """
                SELECT h3id FROM (SELECT unnest(%s) as h3id) as a 
                LEFT JOIN catchments ON h3id = originh3id  AND timeofday = %s
                WHERE catchmentid IS NULL"""
                with self.get_db_connection() as conn:                
                    with conn.cursor(cursor_factory=DictCursor) as cur:
                        cur.execute(sql, (update_pack.poi_list.added, update_pack.config.time_of_day))
                        new_pois = cur.fetchall()
                        new_catchs = []
                        
                        for p in new_pois:
                            new_catchs.append(
                                get_catchment_details(
                                    city_id = update_pack.config.city_id,
                                    h3_id= p[0],
                                    time_of_day= update_pack.config.time_of_day,
                                    demographics_category=update_pack.config.demographic_category
                                )
                            )
                        
                await asyncio.gather(*new_catchs)
                            
            results = await asyncio.gather(*queries.values())            
            dict_results = dict(zip(queries.keys(), results))

            with self.get_db_connection() as conn:                
                with conn.cursor(cursor_factory=DictCursor) as cur:                    
                    sql = 'SELECT boundingbox FROM cities WHERE cityid = %s'
                    cur.execute(sql, (city_id, ))
                    bbox = cur.fetchone()[0]
                    dict_results['long'] = (bbox[0] + bbox[2]) / 2
                    dict_results['lat'] = (bbox[1] + bbox[3]) / 2

            return PydanticJSONResponse(content=CityData.construct(** dict_results))
            
        return app