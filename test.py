
# TODO: check if it is possible to set env variables within this code

# docker build -t databricks-connect-test .
# docker run -t -p 5001:80 databricks-connect-test

import json
from flask import Flask, request
from flask_restful import Resource, Api
from detective_spark_module.pyspark_module.pipeline import Pipeline

app = Flask(__name__)
api = Api(app)


class DataBricksRequest(Resource):

    def post(self):
        print(request.data)

        request_data = json.loads(request.data)

        url = request_data["url"]

        """
        stages = [
            {
                "type": "select_columns",
                "params": {
                    "columns": ["year", "exch_usd"]
                }
            },
            {
                "type": "row_filter",
                "params": {
                    "columns": ["year"],
                    "filter": ["=="],
                    "values": [1870]
                }
            }
        ]
        """
        stages = []
        pipe = Pipeline(stages)
        df = pipe.get_view(url, "ddl")

        response_body = {
            "data": df
        }

        return response_body

    def get(self):
        return {'file': ['product-service', "ddl"]}


# url path
api.add_resource(DataBricksRequest, '/ddl')

# run app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
