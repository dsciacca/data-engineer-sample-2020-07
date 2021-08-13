"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
from src.some_storage_library import SomeStorageLibrary


if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')
    data_path = "data/source/SOURCEDATA.txt"
    schema_path = "data/source/SOURCECOLUMNS.txt"
    output_file = 'output.csv'
    storage_library = SomeStorageLibrary()

    with open(data_path, 'r') as f:
        print(f"Reading {data_path}...")
        data = f.read().replace('|', ',')
    with open(schema_path, 'r') as f:
        print(f"Reading {schema_path}...")
        schema = f.read().split('\n')

    schema = [(x.split('|')[0], x.split('|')[1]) for x in schema]
    schema.sort(key=lambda x: int(x[0]))
    header = ','.join([x[1] for x in schema])

    with open('output.csv', 'w') as f:
        f.write(f"{header}\n{data}")

    storage_library.load_csv('output.csv')
