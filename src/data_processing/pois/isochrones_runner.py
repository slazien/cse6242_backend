from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Index
from geoalchemy2 import Geometry
import itertools as itt
from tqdm import tqdm

import sys
sys.path.append("../../")

import isochrones as isc
import configparser

config = configparser.ConfigParser()
config.read("../../../config/config.ini")    
db_params = config['DB']
opt_params = config['OTP']

conn_string = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**db_params)
engine = create_engine(conn_string, echo=False, future=True)


types = ['morning', 'afternoon', 'evening']
timedistances = [30]
cities = ['Atlanta', 'Dallas', 'Los Angeles', 'New York', 'Chicago']

with engine.connect() as conn:
    service = isc.IsochroneService(otp_port=opt_params['port'], pg_conn=conn, reference_date = opt_params['ref_date'], otp_host=opt_params['host'])

    for timedist, timeofday, city in tqdm(list(itt.product(timedistances, types, cities))):
        with conn.connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT pois.h3id, cities.cityid FROM cities 
                    JOIN cityh3map ON cityh3map.cityid = cities.cityid 
                    JOIN pois ON pois.h3id = cityh3map.h3id
                    LEFT JOIN catchments ON catchments.originh3id = pois.h3id AND catchments.timedistance = %s AND catchments.timeofday = %s
                    WHERE 
                        catchments.geometry IS NULL AND
                        cities.cityname = %s
                """,
                (timedist, timeofday, city)
            )
            res = cur.fetchall()

            for pid, cityid in tqdm(res):                
                service.get_isochrone(city_id = cityid, h3_id= pid, time = timeofday, minutes=timedist)                
                