import duckdb
import logging
import pandas as pd
import matplotlib.pyplot as plt

# logging for personal use 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analyze.log'
)
logger = logging.getLogger(__name__)

"""
0. Connect to DuckDB
1. Finding the largest carbon producing trip for both yellow and green trips
2. Finding the most carbon heavy and carbon light hours of the day for both cab colors
3. Finding the most carbon heavy and carbon light days of the week for both colors
4. Finding the most carbon heavy and carbon light weeks for yellow and green taxis
5. Finding the most carbon heavy and carbon light months of the year for both colors of cabs
6. Generating a time series plot
"""

def analysis():

    try:
        # Connect to DuckDB
        con = duckdb.connect('emissions.duckdb')
        logger.info("Connected to DuckDB successfully")

        # 1. Largest CO2 trip (yellow)
        logger.info("Querying largest CO2-producing trip for yellow taxi.")
        q_yellow_max = """
            SELECT 'yellow' AS taxi_type, * 
            FROM yellow_transform
            ORDER BY trip_co2_kgs DESC
            LIMIT 1
        """
        yellow_max = con.execute(q_yellow_max).df()
            
            # finding the largest CO2 trip for green taxis now
        logger.info("Querying largest CO2-producing trip for green taxi.")
        q_green_max = """
            SELECT 'green' AS taxi_type, * 
            FROM green_transform
            ORDER BY trip_co2_kgs DESC
            LIMIT 1
        """
        green_max = con.execute(q_green_max).df()

        largest_trips = pd.concat([yellow_max, green_max], ignore_index=True)
        logger.info(f"Largest CO2 trips:\n{largest_trips}")
        print(f"largest CO2 trips:\n{largest_trips}")

        # helper function to get heaviest and lightest averages by time
        def get_heavy_light_avg(table_name, time_col, label):
            logger.info(f"Querying CO2 by {label} for {table_name}.")
            q = f"""
                SELECT {time_col}, AVG(trip_co2_kgs) AS avg_co2
                FROM {table_name}
                GROUP BY {time_col}
                ORDER BY avg_co2 DESC
            """
            df = con.execute(q).df()
            heaviest = df.iloc[0]
            lightest = df.iloc[-1]
            return heaviest, lightest

        # 2. finding CO2 emissions by hour (heaviest and lightest)
        logger.info("Querying CO2 by hour of day.")
        yellow_heavy_hour, yellow_light_hour = get_heavy_light_avg('yellow_transform', 'hour_of_day', 'hour_of_day (yellow)')
        green_heavy_hour, green_light_hour = get_heavy_light_avg('green_transform', 'hour_of_day', 'hour_of_day (green)')
        logger.info(f"Yellow taxi - Heaviest hour: {yellow_heavy_hour.to_dict()}, Lightest hour: {yellow_light_hour.to_dict()}")
        logger.info(f"Green taxi - Heaviest hour: {green_heavy_hour.to_dict()}, Lightest hour: {green_light_hour.to_dict()}")
        print(f"Yellow taxi - Heaviest hour: {yellow_heavy_hour.to_dict()}, Lightest hour: {yellow_light_hour.to_dict()}")
        print(f"Green taxi - Heaviest hour: {green_heavy_hour.to_dict()}, Lightest hour: {green_light_hour.to_dict()}")

        # 3. finding CO2 emissions by day of week heaviest + lightest 
        logger.info("Querying CO2 by day of week.")
        yellow_heavy_day, yellow_light_day = get_heavy_light_avg('yellow_transform', 'day_of_week', 'day_of_week (yellow)')
        green_heavy_day, green_light_day = get_heavy_light_avg('green_transform', 'day_of_week', 'day_of_week (green)')
        logger.info(f"Yellow taxi - Heaviest day: {yellow_heavy_day.to_dict()}, Lightest day: {yellow_light_day.to_dict()}")
        logger.info(f"Green taxi - Heaviest day: {green_heavy_day.to_dict()}, Lightest day: {green_light_day.to_dict()}")
        print(f"Yellow taxi - Heaviest day: {yellow_heavy_day.to_dict()}, Lightest day: {yellow_light_day.to_dict()}")
        print(f"Green taxi - Heaviest day: {green_heavy_day.to_dict()}, Lightest day: {green_light_day.to_dict()}")

        # 4. finding CO2 emissions by week of year 
        logger.info("Querying CO2 by week of year.")
        yellow_heavy_week, yellow_light_week = get_heavy_light_avg('yellow_transform', 'week_of_year', 'week_of_year (yellow)')
        green_heavy_week, green_light_week = get_heavy_light_avg('green_transform', 'week_of_year', 'week_of_year (green)')
        logger.info(f"Yellow taxi - Heaviest week: {yellow_heavy_week.to_dict()}, Lightest week: {yellow_light_week.to_dict()}")
        logger.info(f"Green taxi - Heaviest week: {green_heavy_week.to_dict()}, Lightest week: {green_light_week.to_dict()}")
        print(f"Yellow taxi - Heaviest week: {yellow_heavy_week.to_dict()}, Lightest week: {yellow_light_week.to_dict()}")
        print(f"Green taxi - Heaviest week: {green_heavy_week.to_dict()}, Lightest week: {green_light_week.to_dict()}")

        # 5. Finding CO2 emissions by month of year
        yellow_heavy_month, yellow_light_month = get_heavy_light_avg('yellow_transform', 'month_of_year', 'month_of_year (yellow)')
        green_heavy_month, green_light_month = get_heavy_light_avg('green_transform', 'month_of_year', 'month_of_year (green)')
        logger.info(f"Yellow taxi - Heaviest month: {yellow_heavy_month.to_dict()}, Lightest month: {yellow_light_month.to_dict()}")
        logger.info(f"Green taxi - Heaviest month: {green_heavy_month.to_dict()}, Lightest month: {green_light_month.to_dict()}")
        print(f"Yellow taxi - Heaviest month: {yellow_heavy_month.to_dict()}, Lightest month: {yellow_light_month.to_dict()}")
        print(f"Green taxi - Heaviest month: {green_heavy_month.to_dict()}, Lightest month: {green_light_month.to_dict()}")

        # 6. Plot: CO2 totals by taxi type for each year in a time series graph
        
        ## CO2 totals for each year
        CO2_Per_Year = """
        SELECT year, 
        SUM(yellow_co2) AS yellow_total,
        SUM(green_co2) AS green_total
            FROM (
                SELECT EXTRACT(year FROM tpep_pickup_datetime) AS year,
                    trip_co2_kgs AS yellow_co2,
                    NULL AS green_co2
                FROM yellow_transform

                UNION ALL

                SELECT EXTRACT(year FROM lpep_pickup_datetime) AS year,
                    NULL AS yellow_co2,
                    trip_co2_kgs AS green_co2
                FROM green_transform
            )
            GROUP BY year
            ORDER BY year;
        """

        results = con.execute(CO2_Per_Year).fetchall()

        logger.info("Fetched yearly CO2 emission totals")

        # looping through yellow and green totals for the years
        years = [int(row[0]) for row in results]
        yellow_totals = [row[1] or 0 for row in results]
        green_totals = [row[2] or 0 for row in results]

        # plotting the graph
        plt.figure(figsize=(10, 6))
        plt.plot(years, yellow_totals, label='Yellow Taxi', marker='o')
        plt.plot(years, green_totals, label='Green Taxi', marker='o')
        plt.title("Total CO₂ Emissions by Year (Yellow vs Green Taxis)")
        plt.xlabel("Year")
        plt.ylabel("Total CO₂ Emissions")
        plt.legend()
        plt.grid(True)
        plt.xlim(left=2015, right=2025)
        plt.tight_layout()
        plt.savefig("co2_emissions_yearly.png")
        plt.show()
        logger.info("Generated CO2 emissions plot")


    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analysis() 