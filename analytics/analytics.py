import asyncio
import pandas as pd
from os import environ
from time import sleep
from sqlalchemy import create_engine, VARCHAR, Integer, text
from datetime import datetime
from sqlalchemy.exc import OperationalError
from math import radians, degrees, sin, cos, asin, acos, sqrt
import ast

print('Waiting for the data generator...')
sleep(20)


while True:
    try:
        # Create a connection to the PostgreSQL database
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        print("Postgres Connection was Successful")

        # Create a connection to the MySQL database
        mysql_engine = create_engine(environ["MYSQL_CS"])
        print("MySQL Connection was Successful")

        with mysql_engine.connect() as mysql_conn:
            # Create the MySQL table if it doesn't exist
            mysql_conn.execute(text("""
                CREATE TABLE if not exists hourly_aggregations (
                device_id VARCHAR(255),
                hour DATETIME,
                max_temperature FLOAT,
                data_points INT,
                total_distance FLOAT
                );
            """))
            mysql_conn.commit()

        break
    except OperationalError:
        sleep(0.1)


print("ETL Process Started")

def generate_df(list_of_df):
    df = pd.concat(list_of_df)
    #converting int time to datetime object
    df['time'] = df['time'].apply(lambda x: datetime.fromtimestamp(int(x)))

    #extracting hour
    df['hour'] = pd.to_datetime(df['time']).dt.floor('H')

    #converting to dict
    df['location'] = df['location'].apply(ast.literal_eval)

    #aggregating the results
    final_df = df.groupby(['device_id','hour']).agg(data_points=('location','size'),max_temperature=('temperature','max'),total_distance=('location',distance_calculator)).reset_index()
    
    return final_df

#this will calculate the total distance between all points
def distance_calculator(locs):
    distance = [get_distance(loc1['longitude'],loc1['latitude'],loc2['longitude'],loc2['latitude']) for loc2 in locs for loc1 in locs if loc2 != loc1]
    return sum(set(distance))


#this will get the distance between 2 coordinates
def get_distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    vals = 6371 * (
        acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon1 - lon2))
    )
    return vals



def etl_process():
    df_list = []
    #df = None
    last_processed_timestamp = None
    with psql_engine.connect() as psql_conn, mysql_engine.connect() as mysql_conn:
        #This chunk of code is to be used later for incremental logic
        #get the last processed timestamp
        # sql = """
        #     SELECT COALESCE(MAX(hour), cast('1970-01-01 00:00:00' as datetime)) AS last_processed_timestamp
        #     FROM hourly_aggregations
        # """
        # results = mysql_conn.execute(text(sql))
        # mysql_conn.commit()
        # last_processed_timestamp = results.fetchone()[0]

        print('Last Process Timestamp', last_processed_timestamp)
        while True:
            try:
                # Build the SQL query to fetch new data
                sql = f"""
                    SELECT *
                    FROM devices;
                    
                """
                # I have plans to include incremental logic for it. This is the half baked code for it.
                # sql = f"""
                #     SELECT *
                #     FROM devices
                #     where time > cast('{last_processed_timestamp}' as datetime);
                # """
                query = pd.read_sql_query(text(sql), psql_conn)
                psql_conn.commit()
                # Store the query result in a pandas DataFrame
                temp_df = pd.DataFrame(query)

                
                df_list.append(temp_df)
                df_aggregated = generate_df(df_list)
                df_aggregated.to_sql('hourly_aggregations', mysql_conn, if_exists='append', index=False)
                mysql_conn.commit()
                #last_processed_timestamp = df['time'].max()
                break
            except Exception as e:
                print(e)
            # Sleep for a specific interval (e.g., 1 hour)
            #await asyncio.sleep(3600)


if __name__ == "__main__":  
    etl_process()


