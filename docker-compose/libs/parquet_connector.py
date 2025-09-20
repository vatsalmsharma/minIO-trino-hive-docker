from libs.base_connector import BaseConnector

class ParquetConnector(BaseConnector):
    def read_parquet(self, path):
        return self.spark.read.parquet(path)

    def write_parquet(self, df, path, mode="overwrite"):
        df.write.parquet(path, mode=mode)