from pyspark.sql import SparkSession

from libs.postgres_connector import PostgresConnector
from libs.csv_connector import CsvConnector
from libs.parquet_connector import ParquetConnector

def main():
    spark = SparkSession.builder \
        .appName("Demo") \
        .getOrCreate()


    # Init connectors
    pg = PostgresConnector(spark, db="spark_demo", user="admin", password="admin")
    csv = CsvConnector(spark)
    pq = ParquetConnector(spark)

    df_pg = pg.read_table("customers")
    df_pg.show()

    df_csv = csv.read_csv(path="/home/iceberg/data/taxi_zone_lookup.csv")
    df_csv.show()

    df_pq = pq.read_parquet("/home/iceberg/data/yellow_tripdata_2025-01.parquet")
    df_pq.show()

if __name__ == "__main__":
    main()