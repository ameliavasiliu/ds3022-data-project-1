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
            # finding the largest CO2 trip for green taxis
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

        # 2. CO2 by hour (heaviest and lightest)
        logger.info("Querying CO2 by hour of day.")
        yellow_heavy_hour, yellow_light_hour = get_heavy_light_avg('yellow_transform', 'hour_of_day', 'hour_of_day (yellow)')
        green_heavy_hour, green_light_hour = get_heavy_light_avg('green_transform', 'hour_of_day', 'hour_of_day (green)')
        logger.info(f"Yellow taxi - Heaviest hour: {yellow_heavy_hour.to_dict()}, Lightest hour: {yellow_light_hour.to_dict()}")
        logger.info(f"Green taxi - Heaviest hour: {green_heavy_hour.to_dict()}, Lightest hour: {green_light_hour.to_dict()}")

        # 3. CO2 by day of week heaviest + lightest 
        logger.info("Querying CO2 by day of week.")
        yellow_heavy_day, yellow_light_day = get_heavy_light_avg('yellow_transform', 'day_of_week', 'day_of_week (yellow)')
        green_heavy_day, green_light_day = get_heavy_light_avg('green_transform', 'day_of_week', 'day_of_week (green)')
        logger.info(f"Yellow taxi - Heaviest day: {yellow_heavy_day.to_dict()}, Lightest day: {yellow_light_day.to_dict()}")
        logger.info(f"Green taxi - Heaviest day: {green_heavy_day.to_dict()}, Lightest day: {green_light_day.to_dict()}")

        # 4. CO2 by week of year 
        logger.info("Querying CO2 by week of year.")
        yellow_heavy_week, yellow_light_week = get_heavy_light_avg('yellow_transform', 'week_of_year', 'week_of_year (yellow)')
        green_heavy_week, green_light_week = get_heavy_light_avg('green_transform', 'week_of_year', 'week_of_year (green)')
        logger.info(f"Yellow taxi - Heaviest week: {yellow_heavy_week.to_dict()}, Lightest week: {yellow_light_week.to_dict()}")
        logger.info(f"Green taxi - Heaviest week: {green_heavy_week.to_dict()}, Lightest week: {green_light_week.to_dict()}")

        # 5. CO2 by month of year
        yellow_heavy_month, yellow_light_month = get_heavy_light_avg('yellow_transform', 'month_of_year', 'month_of_year (yellow)')
        green_heavy_month, green_light_month = get_heavy_light_avg('green_transform', 'month_of_year', 'month_of_year (green)')
        logger.info(f"Yellow taxi - Heaviest month: {yellow_heavy_month.to_dict()}, Lightest month: {yellow_light_month.to_dict()}")
        logger.info(f"Green taxi - Heaviest month: {green_heavy_month.to_dict()}, Lightest month: {green_light_month.to_dict()}")

        # 6. Plot: Monthly CO2 totals by taxi type
        logger.info("Generating CO2 by month plot.")
        q_plot_yellow = """
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM yellow_transform
            GROUP BY month_of_year
            ORDER BY month_of_year
        """
        q_plot_green = """
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM green_transform
            GROUP BY month_of_year
            ORDER BY month_of_year
        """
        df_yellow = con.execute(q_plot_yellow).df()
        df_green = con.execute(q_plot_green).df()

        df_yellow['taxi_type'] = 'yellow'
        df_green['taxi_type'] = 'green'

        df_plot = pd.concat([df_yellow, df_green])

        pivot = df_plot.pivot(index="month_of_year", columns="taxi_type", values="total_co2").fillna(0)
        pivot.plot(kind="bar", figsize=(10, 6))
        plt.title("Monthly CO2 Emissions by Taxi Type")
        plt.xlabel("Month")
        plt.ylabel("Total CO2 (kg)")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig("co2_by_month.png")
        plt.close()
        logger.info("Plot saved as co2_by_month.png")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analysis() 