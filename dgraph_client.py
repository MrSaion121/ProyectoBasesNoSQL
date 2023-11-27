
import os

import pydgraph

import modelDgraph

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()

client_stub = create_client_stub()
client = create_client(client_stub)

modelDgraph.drop_all(client)
modelDgraph.set_schema(client)
modelDgraph.create_data(client)
