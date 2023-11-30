
import os

import pydgraph

import modelDgraph

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    menu_options = {
        1: "Create data",
        2: "Get all passengers",
        3: "Get passengers with a specific reason",
        4: "Get the number of reasons passengers take flights within an age range",
        5: "Exit",
    }
    for key in menu_options.keys():
        print(key, '--', menu_options[key])

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)

    modelDgraph.drop_all(client)
    modelDgraph.set_schema(client)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            modelDgraph.upload_data(client)
        if option == 2:
            modelDgraph.get_all_data(client)
        if option == 3:
            reason = input("Reason: ")
            modelDgraph.passengers_by_reason(client, reason)
        if option == 4:
            ageMin = input("Minimum age: ")
            ageMax = input("Maximum age: ")
            modelDgraph.reason_count(client, ageMin, ageMax)
        if option == 5:
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))