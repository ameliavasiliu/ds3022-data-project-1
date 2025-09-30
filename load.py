import duckdb
import os
import logging
import time

# logging for personal use 
logging.basicConfig(
    level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

"""
0. Set up DB connection
1. Drop tables if they exist
2. create and append monthly parquet data for yellow and green cabs only
3. load vehicle emissions

"""

def load_parquet_files():

    con = None

    try:
        # 0. Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # 1. dropping tables if they exist so script can be rerun cleanly
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_all
        ;
            DROP TABLE IF EXISTS green_all
        ;
            DROP TABLE IF EXISTS vehicle_emissions
        ;
                    
        """)
        logger.info("Dropped table if exists")

        # 2. Loop through months of 2024 and load yellow + green taxi data in separate tables
        for taxi_type in ["yellow", "green"]:
            for year in range(2015, 2025):
                for month in range(1, 13):
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02}.parquet"
                    logger.info(f"Loading {taxi_type} data for {year}-{month:02} from {url}")

                    try:
                        if year == 2015 and month == 1:
                            con.execute(f"""
                            CREATE TABLE {taxi_type}_all AS
                            SELECT * FROM read_parquet('{url}');
                            """)
                            logger.info(f"Created table {taxi_type}_all with data from {year}-{month:02}")
                        else:
                            # appending to existing tables
                            con.execute(f"""
                                INSERT INTO {taxi_type}_all
                                SELECT * FROM read_parquet('{url}')
                            """)
                            logger.info(f"Appended data for {year}-{month:02} to {taxi_type}_all")
                    except Exception as e:
                        logger.warning("failed to load")
                    
                    time.sleep(30)
        

        # loading vehicle emissions CSV from local file with join
        csv_path = os.path.join('data', 'vehicle_emissions.csv')
        con.execute(f"""
        CREATE TABLE vehicle_emissions AS
        SELECT * FROM read_csv_auto('{csv_path}');
        """)
        logger.info("Loaded vehicle_emissions.csv into vehicle_emissions table")

        # running checks / summary queries
        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_all").fetchone()[0]
        print(f"yellow taxi count is {yellow_count}")
        logger.info(f"yellow taxi count is {yellow_count}")

        green_count = con.execute("SELECT COUNT(*) FROM green_all").fetchone()[0]
        print(f"green taxi count is {green_count}")
        logger.info(f"green taxi count is {green_count}")

        yellow_avg_distance = con.execute("SELECT AVG(trip_distance) FROM yellow_all").fetchone()[0]
        print(f"average yellow taxi trip distance is {yellow_avg_distance}")
        logger.info(f"average yellow taxi trip distance is {yellow_avg_distance}")

        green_avg_distance = con.execute("SELECT AVG(trip_distance) FROM green_all").fetchone()[0]
        print(f"average green taxi trip distance is {green_avg_distance}")
        logger.info(f"average green taxi trip distance is {green_avg_distance}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files() 
