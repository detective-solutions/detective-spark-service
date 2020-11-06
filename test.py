
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
        url = request_data["url"]

        # set empty stages since the test will just return the data set
        stages = request_data["stages"]

        pipe = Pipeline(stages)
        df = pipe.get_view(url, "ddl")

        response_body = {
            "data": df
        }

        return response_body

    def get(self):
        return {'file': ['product-service', "ddl"]}


class PlainDataRequest(Resource):

    def post(self):

        request_data = json.loads(request.data)

        # get url
        url = request_data["url"]

        stages = [
            {
                'attribute': {
                    'type': 'empty_handover',
                    'params': {'blank_key': 'blank_value'},
                    'status': 'update',
                    'meta': {'schema': 'None', 'shape': 'None', 'report': 'None'},
                    'loading': True,
                    'applied': True,
                    'data': dict()}
                }
        ]
        pipe = Pipeline(stages)
        df = pipe.get_view(url, "ddl")

        response_body = {
            "data": df
        }

        return response_body


# url path
api.add_resource(DataBricksRequest, '/ddl')
api.add_resource(PlainDataRequest, '/init')

# run app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
