from libs.base_connector import BaseConnector

class CsvConnector(BaseConnector):
    def read_csv(self, path, header=True, inferSchema=True):
        return self.spark.read.csv(path, header=header, inferSchema=inferSchema)

    def write_csv(self, df, path, mode="overwrite", header=True):
        df.write.csv(path, mode=mode, header=header)