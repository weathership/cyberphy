from pyiceberg.catalog import load_catalog

catalog = load_catalog('cybersec', **{
    'type': 'sql',
    'uri': 'postgresql://cybersec:cybersec@localhost:5438/iceberg',
    'warehouse': 's3://cyberphy/iceberg/warehouse',
    's3.endpoint': 'http://localhost:9010',
    's3.path-style-access': 'true',
    's3.access-key-id': 'minioadmin',
    's3.secret-access-key': 'minioadmin',
    'py-io-impl': 'pyiceberg.io.fsspec.FsspecFileIO'
})

print('Namespaces:')
for ns in catalog.list_namespaces():
    print(f'  {ns}')
    try:
        tables = catalog.list_tables(ns)
        for table in tables:
            print(f'    - {table}')
    except Exception as e:
        print(f'    Error: {e}')
