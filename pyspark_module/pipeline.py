# import standard modules
import json
import inspect

# import third party modules
from pyspark.sql import SparkSession

# import project related modules
from .pyspark_functions import PipelineFunctions


class Pipeline:

    def __init__(self, pipe):
        self.pipe = pipe
        self.functions = {key: value for key, value in inspect.getmembers(PipelineFunctions()) if inspect.isfunction(value)}

    def to_json(self, df):
        data_list = df.limit(1000).collect()

        data = dict()
        for column in df.columns:
            data[column] = [str(x[column]) for x in data_list]

        return data

    @staticmethod
    def create_mount_url(path, mount, transform=None, local=False, is_file=False):

        # remove the first entry since this is related to the detective instance name
        path = "/".join(x for x in path.split('/')[2:])
        path = (
            path.replace("mnt/", "").replace("dbfs:/", "").replace("/dbfs/", "").replace("//", "/")
        )
        path = f"/mnt/{mount}/" + path + "/"
        path = path.replace("//", "/")
        print(path)
        if is_file:
            path = "/" + path.strip("/")

        if local:
            path = "/dbfs" + path
        else:
            path = "dbfs:/" + path

        if transform == "to_trusted":
            path = path.replace("/landing/", "/trusted/")

        path = path.replace("//", "/")

        return path

    def remove_index(self, index):
        self.pipe.remove(self.pipe[index])

    def add_index(self, index, stage):
        self.pipe.insert(index, stage)

    def read_path(self, path, spark):

        # check if path includes file or folder
        if path.endswith(".csv"):
            print("PATH CSV")
            return spark.read.csv(path, header=True, mode="DROPMALFORMED")
        elif path.endswith("/"):
            print("PATH PARQUET")
            return spark.read.parquet(path)
        elif path.endswith(".json"):
            print("PATH JSON")
            return None
        else:
            return None

    def get_view(self, path, mount):

        spark = SparkSession.builder.getOrCreate()
        url = self.create_mount_url(path, mount)
        df = spark.read.format("csv").option("header", True).csv(url)

        if df is not None:
            for i, stage in enumerate(self.pipe):
                if stage["attribute"]["status"] == "update":
                    df = self.functions[stage["attribute"]["type"]](
                        df, stage["attribute"]["params"]
                    )
                    schema = dict(df.dtypes)
                    shape = {"rows": df.count(), "columns": len(df.columns)}
                    report = ["placeholder"]

                    self.pipe[i]["attribute"]["data"] = self.to_json(df)
                    self.pipe[i]["attribute"]["meta"]["schema"] = schema
                    self.pipe[i]["attribute"]["meta"]["shape"] = shape
                    self.pipe[i]["attribute"]["meta"]["report"] = report
                    self.pipe[i]["attribute"]["status"] = "state"

            return self.pipe
        else:
            for i, stage in enumerate(self.pipe):
                if stage["attribute"]["status"] == "update":
                    report = ["no data found with the current identifier"]
                    self.pipe[i]["attribute"]["meta"]["report"] = report
                    self.pipe[i]["attribute"]["status"] = "state"

            return self.pipe
