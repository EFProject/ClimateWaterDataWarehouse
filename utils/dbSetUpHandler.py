from sqlalchemy import text

def setUpDB(connection):
    
	schemas = connection.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ClimateWaterDataWarehouse';")).fetchall()
	if not schemas:
		# SetUp ClimateWaterDataWarehouse Schema
		connection.execute(text('CREATE SCHEMA IF NOT EXISTS "ClimateWaterDataWarehouse";'))     

	# Set the search path to use the ClimateWaterDataWarehouse schema as default
	connection.execute(text('SET search_path TO "ClimateWaterDataWarehouse";'))              
	tables = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ClimateWaterDataWarehouse';")).fetchall()
	if len(tables) != 5:
		#SetUp Tables
		connection.execute(text('''
								CREATE TABLE IF NOT EXISTS "Date_Dim" (
									date_id SERIAL PRIMARY KEY,
									date DATE UNIQUE NOT NULL,
									month INTEGER NOT NULL,
									quarter VARCHAR(255) NOT NULL,
									year INTEGER NOT NULL
								);
								'''))
		connection.execute(text('''
								CREATE TABLE IF NOT EXISTS "Location_Dim" (
									location_id SERIAL PRIMARY KEY,
									country VARCHAR(255) NOT NULL,
									city VARCHAR(255),
									latitude VARCHAR(255),
									longitude VARCHAR(255),
						  			climate_zone VARCHAR(255),
						  			precipitation_zone VARCHAR(255),
						  			temperature_zone VARCHAR(255),
						  			CONSTRAINT unique_location UNIQUE (country, city)
								);
								'''))
		connection.execute(text('''
								CREATE TABLE IF NOT EXISTS "Param_Dim" (
									param_id SERIAL PRIMARY KEY,
									param_name VARCHAR(255) UNIQUE NOT NULL,
									unit_of_measure VARCHAR(255) NOT NULL,
									category VARCHAR(255),
									threshold_value FLOAT(53),
									description VARCHAR(2000)
								);
								'''))
		connection.execute(text('''
								CREATE TABLE IF NOT EXISTS "Source_Dim" (
									source_id SERIAL PRIMARY KEY,
									source_name VARCHAR(255) UNIQUE NOT NULL,
									source_link VARCHAR(255),
									source_type VARCHAR(255),
									source_data_quality VARCHAR(1000)
								);
								'''))
		connection.execute(text('''
								CREATE TABLE IF NOT EXISTS "Environment_Fact" (
						  			date_id INT REFERENCES "ClimateWaterDataWarehouse"."Date_Dim"(date_id),
									location_id INT REFERENCES "ClimateWaterDataWarehouse"."Location_Dim"(location_id),
									param_id INT REFERENCES "ClimateWaterDataWarehouse"."Param_Dim"(param_id),
						  			source_id INT REFERENCES "ClimateWaterDataWarehouse"."Source_Dim"(source_id),
									measurement_value FLOAT(24),
						  			CONSTRAINT unique_mv UNIQUE (date_id, location_id, param_id, source_id)
								);
								'''))

	print("Schema 'ClimateWaterDataWarehouse' and relative tables are correctly set up. \n")



def getLookupTable(connection):

	# LookupTable of Date_Dim

    date_ids = {}
    date_data = connection.execute(text('''
                                        SELECT date, date_id
                                        FROM "Date_Dim"
                                    ''')).fetchall()
    date_ids = {row[0]: row[1] for row in date_data}

    # LookupTable of Location_Dim

    location_ids = {}
    location_data = connection.execute(text('''
                                        SELECT country, city, location_id
                                        FROM "Location_Dim"
                                    ''')).fetchall()
    location_ids = {(row[0], row[1]): row[2] for row in location_data}

    # LookupTable of Param_Dim

    param_ids = {}
    param_data = connection.execute(text('''
                                        SELECT param_name, param_id
                                        FROM "Param_Dim"
                                    ''')).fetchall()
    param_ids = {row[0]: row[1] for row in param_data}
    
    print("LookupTables obtained:")
    print("Date_Dim : ", len(date_ids), "Location_Dim : ", len(location_ids), " rows, ","Param_Dim : ", len(param_ids), " rows \n")

    return [location_ids, param_ids, date_ids]


    # location_names = non_numericalColumns['Country'].tolist()
    # result = connection.execute(text('''
    #                                     SELECT param_name, param_id
    #                                     FROM "Param_Dim"
    #                                     WHERE param_name IN :location_names
    #                                 '''), {'location_names': tuple(location_names)})