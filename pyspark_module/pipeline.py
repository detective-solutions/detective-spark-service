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

    @staticmethod
    def to_json(df):
        data_list = df.limit(1000).toJSON().collect()
        data_list = [json.loads(x) for x in data_list]

        # create a single dict out of the existing data frame
        data = {}
        for column in data_list[0].keys():
            data[column] = [row[column] for row in data_list]

        return data

    @staticmethod
    def create_mount_url(path, mount, transform=None, local=False, is_file=False):
        # TODO: create an explicit method for it
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

    def get_view(self, path, mount):
        spark = SparkSession.builder.getOrCreate()
        url = self.create_mount_url(path, mount)
        df = spark.read.csv(url, header=True, mode="DROPMALFORMED")

        for i, stage in enumerate(self.pipe):
            df = self.functions[stage["type"]](df, stage["params"])
            # if stage["status"] == "update":
                # schema = dict(zip)

        return self.to_json(df)
