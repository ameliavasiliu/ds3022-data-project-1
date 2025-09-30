import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)
"""
0. Connect to DuckDB
1. Drop cleaned tables if they exist
2. Remove any duplicates
3. Remove trips with 0 passengers
4. Remove trips with 0 miles
5. Remove trips with > 100 miles
6. Remove trips longer than 86400 seconds
7. Save cleaned tables
8. Check that tables are clean
"""

def clean_trip_data():
    con = None

    try:
        # 0. connect to the existing duckdb database
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)
        logger.info("Connected to DuckDB")

        # 1. drop cleaned tables if they exist
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_clean
        ;
            DROP TABLE IF EXISTS green_clean
        ;
                    
        """)
        logger.info("Dropped clean tables if they exist")

        # 2-7 applying cleaning to both taxi types using a for loop
        for taxi_type in ["yellow", "green"]:
            logger.info(f"Cleaning {taxi_type}_all table")

            # separating the taxi colors and their respective time stamp column names
            if taxi_type == "yellow":
                pickup_col = "tpep_pickup_datetime"
                dropoff_col = "tpep_dropoff_datetime"
            else:
                pickup_col = "lpep_pickup_datetime"
                dropoff_col = "lpep_dropoff_datetime"
            
            # cleaning the tables; selecting the trips only with the desired criteria
                # passenger count must be > 0
                # trip_distance must be <= 100 
                # interval must be less than one day 
             
            con.execute(f"""
                CREATE TABLE {taxi_type}_clean AS
                SELECT DISTINCT * 
                FROM {taxi_type}_all WHERE
                    passenger_count > 0 AND
                    trip_distance > 0 AND
                    trip_distance <= 100 AND
                    ({dropoff_col} - {pickup_col}) <= INTERVAL 1 DAY
                ;
            """)
            logger.info(f"Created {taxi_type}_clean with filtered data")

            # 8. verifying the tables are clean
                # checking number of rows
                # checking if there are any trips with 0 passengers
                # checking if the distance traveled is appropriate to conditions set above
                # checking that there are no records with duration over 1 day

            total = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_clean
                ;
                """).fetchone()[0]
            zero_pass = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_clean WHERE passenger_count = 0
                ;
                """).fetchone()[0]
            bad_dist = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_clean WHERE trip_distance <= 0 OR trip_distance > 100
                ;
                """).fetchone()[0]
            too_long = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_clean
                WHERE ({dropoff_col} - {pickup_col}) > INTERVAL 1 DAY
                ;
                """).fetchone()[0]
            
            # printing check statements
            print(f"\n{taxi_type}_clean")
            logger.info(f"\n{taxi_type}_clean")
            print(f"Total rows: {total}")
            logger.info(f"Total rows: {total}")
            print(f"Number of 0-passenger trips: {zero_pass}")
            logger.info(f"Number of 0-passenger trips: {zero_pass}")
            print(f"Valid distance range: {bad_dist}")
            logger.info(f"Valid distance range: {bad_dist}")
            print(f"Duration under 1 day: {too_long}")
            logger.info(f"Duration under 1 day: {too_long}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_trip_data()





