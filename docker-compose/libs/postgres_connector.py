from libs.base_connector import BaseConnector

class PostgresConnector(BaseConnector):
    def __init__(self, spark, db, user, password, host="postgres", port=5432):
        super().__init__(spark)
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
        self.properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }
    
    def read_table(self, table):
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table,
            properties=self.properties
        )

    def write_table(self, df, table, mode="append"):
        df.write.jdbc(
            url=self.jdbc_url,
            table=table,
            mode=mode,
            properties=self.properties
        )


# from pyspark.sql import SparkSession
# # import psycopg2

# class PostgresConnector:
#     def __init__(self, db, user, password, host="postgres", port=5432):
#         # Spark session
#         self.spark = SparkSession.builder \
#             .appName("Spark-Postgres-CRUD") \
#             .getOrCreate()
        
#         # JDBC URL
#         self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
#         self.properties = {
#             "user": user,
#             "password": password,
#             "driver": "org.postgresql.Driver"
#         }

#         # # psycopg2 connection
#         # self.conn = psycopg2.connect(
#         #     dbname=db,
#         #     user=user,
#         #     password=password,
#         #     host=host,
#         #     port=port
#         # )
#         # self.cur = self.conn.cursor()

#     # ---- CRUD with Spark ----
#     def read_table(self, table):
#         return self.spark.read.jdbc(
#             url=self.jdbc_url,
#             table=table,
#             properties=self.properties
#         )

#     def write_table(self, df, table, mode="append"):
#         df.write.jdbc(
#             url=self.jdbc_url,
#             table=table,
#             mode=mode,
#             properties=self.properties
#         )

#     # # ---- CRUD with psycopg2 ----
#     # def execute_sql(self, sql, params=None):
#     #     self.cur.execute(sql, params or ())
#     #     self.conn.commit()

#     def close(self):
#         # self.cur.close()
#         # self.conn.close()
#         self.spark.stop()
