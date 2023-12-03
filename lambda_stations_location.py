import json
import pandas as pd
import requests
import psycopg2
import os
import geopandas as gpd

ENDPOINT = "http://datalake01.cbahnwmpgx2t.us-east-1.rds.amazonaws.com"
DB_NAME = "datalake01"
USERNAME = "postgrebbex"
PASSWORD = "databasebab7!"

url_station_information = 'https://sharedmobility.ch/station_information.json'
url_city_info = 'https://www.ogd.stadt-zuerich.ch/wfs/geoportal/Statistische_Quartiere?service=WFS&version=1.1.0&request=GetFeature&outputFormat=GeoJSON&typename=adm_statistische_quartiere_v'


def get_city_info(lat, lon):
    try:
        params = {
            'service': 'WFS',
            'version': '1.1.0',
            'request': 'GetFeature',
            'outputFormat': 'GeoJSON',
            'typename': 'adm_statistische_quartiere_v',
            'CQL_FILTER': f'INTERSECTS(geom, POINT({lon} {lat}))'
        }
        r = requests.get(url_city_info, params=params)
        city_data = r.json()

        if 'features' in city_data and city_data['features']:
            properties = city_data['features'][0]['properties']
            qname = properties.get('qname')
            qnr = properties.get('qnr')
            return qname, qnr
    except Exception as e:
        print(f"Error fetching city information: {e}")
    return None, None


def lambda_handler(event, context):
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make a connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    # Auto commit
    conn.set_session(autocommit=True)

    # Get data on stations
    r = requests.get(url_station_information)
    data_stations = pd.DataFrame(r.json()['data']['stations'])

    print(data_stations.head(5))

    cur.execute("CREATE TABLE IF NOT EXISTS public.stations ("
                "station_id varchar, name varchar, lat double precision, lon double precision, "
                "provider_id varchar, qname varchar, qnr varchar);")

    try:
        for _, row in data_stations.iterrows():
            try:
                # Get city information based on latitude and longitude
                qname, qnr = get_city_info(row['lat'], row['lon'])

                cur.execute(f"INSERT INTO public.stations (station_id, name, lat, lon, provider_id, qname, qnr) "
                            f"VALUES ('{row['station_id']}', '{row['name']}', {row['lat']}, {row['lon']}, "
                            f"'{row['provider_id']}', '{qname}', '{qnr}');")
            except psycopg2.Error as e:
                continue
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)

    cur.close()
    conn.close()

    return "Execution successful"