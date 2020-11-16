# TODO: check if it is possible to set env variables within this code

# docker build -t databricks-connect-test .
# docker run -t -p 5001:80 databricks-connect-test

import json
from flask import Flask, request
from flask_restful import Resource, Api
from pyspark_module.pipeline import Pipeline

app = Flask(__name__)
api = Api(app)


class DataBricksRequest(Resource):
    def post(self):
        print(request.data)

        request_data = json.loads(request.data)

        # get just the url from received data
        url = request_data["data_id"]

        # set empty stages since the test will just return the data set
        stages = request_data["stages"]

        pipe = Pipeline(stages)
        df = pipe.get_view(url, "ddl")

        response_body = {"data": df}

        return response_body

    def get(self):
        return {"file": ["product-service", "ddl"]}


# url path
api.add_resource(DataBricksRequest, "/ddl")
