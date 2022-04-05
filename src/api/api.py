from fastapi import FastAPI
from sqlalchemy import create_engine
import psycopg2 as pg

class backendApi:
    def __init__(self, db_params) -> None:
        assert 'dbname' in db_params, "Database parameters do not include 'dbname'"
        assert 'user' in db_params, "Database parameters do not include 'user'"
        assert 'password' in db_params, "Database parameters do not include 'password'"
        assert 'host' in db_params, "Database parameters do not include 'host'"
        assert 'port' in db_params, "Database parameters do not include 'port'"
        self.db_params = db_params

    
    def get_db_connection(self):
        return pg.connect(**self.db_params)

    def get_app(self):
        app = FastAPI()
        
        @app.get("/cities")
        def read_cities():
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT cityID, cityname FROM cities")
                    data = cur.fetchall()
            citiesList = []
            for i in data:
                citiesList.append({"ID": i[0], "Name": i[1]})
            return citiesList


        @app.get("/pois")
        def read_POIs(city, type):
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    sql = "SELECT poiid, name, lat, long FROM cityh3map inner join (SELECT * FROM pois WHERE category = '" + type + "') pois ON cityh3map.h3id = pois.h3id WHERE cityh3map.cityid = " + str(city)
                    cur.execute(sql)
                    data = cur.fetchall()
            response = []
            for i in data:
                response.append({"ID": i[0], "Name": i[1], "Coordinates": {"lat": i[2], "long": i[3]}})
            return response
        
        @app.get("/demographics")
        def read_demographics(city, category):
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    sql = "SELECT demo.h3id, groupname, sum(total) FROM (SELECT * FROM censush3map inner join "
                    sql += "demographics ON censush3map.censusblockgroupid = demographics.censusblockgroupid) demo "
                    sql += "inner join cityh3map on cityh3map.h3id = demo.h3id WHERE cityh3map.cityid = "
                    sql += str(city) + " AND groupname = '" + category + "' GROUP BY demo.h3id, groupname"
                    cur.execute(sql)
                    data = cur.fetchall()
            dicData = {}
            for i in data:
                if(i[0] not in dicData):
                    dicData[i[0]] = []
                dicData[i[0]].append([i[1], i[2]])
            res = {"HexID": [], "Demographics": []}
            for hexID in dicData:
                res["HexID"].append(hexID)
                for j in dicData[hexID]:
                    res["Demographics"].append([{"Name": j[0], "Population": j[1]}])
            return res

        #To do
        @app.get("/catchment")
        async def read_catchment(city, poi, TimeofDay, distance, category):
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    sqlDemo = "SELECT demo.h3id, groupname, total FROM (SELECT * FROM censush3map inner join "
                    sqlDemo += "demographics ON censush3map.censusblockgroupid = demographics.censusblockgroupid) demo "
                    sqlDemo += "inner join cityh3map on cityh3map.h3id = demo.h3id WHERE cityh3map.cityid = "
                    sqlDemo += str(city) + " AND groupname = '" + category + "'"
                    sql = "(SELECT * FROM ((SELECT * FROM catchmenth3map inner join "
                    sql += "(SELECT catchmentid as catchmentid2, originh3id, timeofday, "
                    sql += "timedistance, geometry from catchments"
                    sql += " WHERE timeofday = '" + TimeofDay + "' AND timedistance = " + str(distance) + ") catchments"
                    sql += " ON catchments.catchmentid2 = catchmenth3map.catchmentid) catchment"
                    sql += " inner join (SELECT id as id2, cityid, h3id AS h3id2 from cityh3map"
                    sql += " WHERE cityid = " + str(city) + ") cityh3map on cityh3map.h3id2"
                    sql += " = catchment.h3id))"
                    sqlFinal = "SELECT catchment.catchmentid, catchment.geometry, groupname, total FROM (" + sql + ") catchment INNER JOIN ("
                    sqlFinal += sqlDemo + ") demo ON demo.h3id = catchment.h3id"
                    print(sqlFinal)
                    cur.execute(sqlFinal)
                    data = cur.fetchall()
            return data

        @app.get("/city_stats")
        async def read_city_stats(city, category, timeofDay, distance, type):
            return {"1": "Hello World"}

        return app