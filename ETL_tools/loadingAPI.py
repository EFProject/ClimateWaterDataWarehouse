from sqlalchemy import text

def createSchema(connection):

    connection.execute(text('CREATE SCHEMA IF NOT EXISTS "ClimateWaterDataWarehouse";'))

    # Verify that the schema exists
    schemas = connection.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ClimateWaterDataWarehouse';")).fetchall()
    if not schemas:
        print("Schema 'ClimateWaterDataWarehouse' was not created. \n")
    else:
        print("Schema 'ClimateWaterDataWarehouse' exists. \n")

def loadDataFrame(df, connection, table_name):

    try:
        df.to_sql(table_name, connection, schema='ClimateWaterDataWarehouse', if_exists='replace', index=False)

        # Fetch data from the newly created table
        # result = connection.execute(text('SELECT * FROM "ClimateWaterDataWarehouse".{table_name};')).fetchall()
        # print(result)

    except Exception as e:
        print(f"An error occurred: {e}")