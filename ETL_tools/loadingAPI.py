import datetime
import numpy as np
from sqlalchemy import text
from utils.formulas import *
from utils.pandasAPI import *

def loadDataFrame(df, connection, lookupTables, nnnColumns, source_id):

    numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df, 1, nnnColumns)

    try:

        ### Date_Dim handler

        for date in non_numericalColumns['Date']:
            if not date in lookupTables[2] :
                insert_date = text('''
                                    INSERT INTO "Date_Dim" (date, month, quarter, year) 
                                    VALUES (:date, :month, :quarter, :year)
                                    RETURNING date_id;
                                    ''')
                if date.month in [1, 2, 3, 4]:
                    quarter = "Q1"
                elif date.month in [5, 6, 7, 8]:
                    quarter = "Q2"
                elif date.month in [9, 10, 11, 12]:
                    quarter = "Q3"
                new_insert = connection.execute(insert_date, {
                    'date': date,
                    'month': date.month,
                    'quarter': quarter,
                    'year': date.year
                })
                date_id = new_insert.fetchone()[0]
                lookupTables[2][date] = date_id
        print("The 'Date_Dim' table correctly populated")

        ### Location_Dim handler

        for index, country in enumerate(non_numericalColumns['Country']):
            city="Undefined"
            if "City" in non_numericalColumns.columns: city = non_numericalColumns['City'].iloc[index]
            if not (country,city) in lookupTables[0] :
                insert_country = text('''
                                    INSERT INTO "Location_Dim" (country, city) 
                                    VALUES (:country, :city)
                                    RETURNING location_id;
                                    ''')
                new_insert = connection.execute(insert_country, {'country': country, 'city': city})
                location_id = new_insert.fetchone()[0]
                lookupTables[0][(country, city)] = location_id
        print("The 'Location_Dim' table correctly populated")

        ### Param_Dim handler

        non_numericalRows = non_numericalRows.iloc[:,nnnColumns:]
        for column_name in non_numericalRows.columns:
            if not column_name in lookupTables[1] :
                unit_of_measure = non_numericalRows.loc[0, column_name]
                insert_param = text("""
                    INSERT INTO "Param_Dim" (param_name, unit_of_measure)
                    VALUES (:param_name, :unit_of_measure)
                    RETURNING param_id;
                """)
                new_insert = connection.execute(insert_param, {
                    'param_name': column_name,
                    'unit_of_measure': unit_of_measure
                })
                param_id = new_insert.fetchone()[0]
                lookupTables[1][column_name] = param_id
        print("The 'Param_Dim' table correctly populated")


        print("\n LookupTables status:")
        print("Date_Dim : ", len(lookupTables[2]), "Location_Dim : ", len(lookupTables[0]), " rows, ", "Source_Dim : ", len(lookupTables[3]), " rows, ", "Param_Dim : ", len(lookupTables[1]), " rows \n")


        ### Environment_Fact handler

        for index, row in df.iloc[1:].iterrows():

            date_id = lookupTables[2].get(row['Date'])
            if date_id is None:
                print(f"Date of '{row['Date']}' not found in date_dim.")
                #continue    # Skip this row if location_id is not found

            city="Undefined"
            if "City" in non_numericalColumns.columns: city = row['City']
            location_id = lookupTables[0][(row['Country'], city)]
            if location_id is None:
                print(f"Location of '{row['Country']}' not found in location_dim.")
                #continue    # Skip this row if location_id is not found
            
            for column in df.columns[nnnColumns:]:
                param_id = lookupTables[1].get(column)
                if param_id is None:
                    print(f"Parameter '{column}' not found in param_dim.")
                    #continue  # Skip this column if param_id is not found
                
                measurement_value = round(row[column], 3) # Round to 3 decimal places
                
                insert_mv = text('''
                    INSERT INTO "Environment_Fact" (date_id, location_id, param_id, source_id, measurement_value)
                    VALUES (:date_id, :location_id, :param_id, :source_id, :measurement_value)
                    ON CONFLICT DO NOTHING;
                ''')
                
                connection.execute(insert_mv, {
                    'date_id': date_id,
                    'location_id': location_id,
                    'param_id': param_id,
                    'source_id': source_id,
                    'measurement_value': measurement_value
                })

        result = connection.execute(text('SELECT COUNT(*) FROM "Environment_Fact";'))
        count = result.scalar()
        print(f"The 'Environment_Fact' table correctly populated : {count} rows \n ")
        

        #numericalDf.to_sql("Location_Dim", connection, schema='ClimateWaterDataWarehouse', if_exists='replace', index=False)

    except Exception as e:
        print(f"An error occurred: {e}")


def loadSourceData(connection, sourceData):
        
    try:
        
        ### Source_Dim handler

        lookupSourceTable = {}

        for source in sourceData:

            nameDF = source['nameDF']
            source_name = source['source_name']
            source_link = source['source_link']
            source_data_quality = source['source_data_quality']

            insert_source = text('''
                                INSERT INTO "Source_Dim" (source_name, source_link, source_data_quality) 
                                VALUES (:source_name, :source_link, :source_data_quality)
                                ON CONFLICT (source_name) DO NOTHING
                                RETURNING source_id;
                                ''')
            new_insert = connection.execute(insert_source, {
                    'source_name': source_name,
                    'source_link': source_link,
                    'source_data_quality': source_data_quality
                })
            
            source_id = new_insert.fetchone()
    
            if source_id:
                lookupSourceTable[nameDF] = source_id[0]
            else:
                # If the insert failed due to conflict, retrieve the existing source_id
                select_source_id = text('''
                    SELECT source_id FROM "Source_Dim" WHERE source_name = :source_name;
                ''')
                existing_source_id = connection.execute(select_source_id, {'source_name': source_name}).fetchone()
                
                if existing_source_id:
                    lookupSourceTable[nameDF] = existing_source_id[0]
        print("The 'Source_Dim' table correctly populated")

        return lookupSourceTable

    except Exception as e:
        print(f"An error occurred: {e}")


def loadExtraData(connection, paramData, locationData):
        
    try:

        ### Param_Dim handler

        for param in paramData:
            update_param = text("""
                UPDATE "Param_Dim"
                SET description = :description
                WHERE param_name = :param_name;
            """)
            connection.execute(update_param, {
                'param_name': param,
                'description': paramData[param]
            })
        print("The 'Param_Dim' description column correctly populated \n")

        ### Location_Dim handler

        for index, row in locationData[2].iterrows():
            latitude = pd.to_numeric(row['latitude'], errors='coerce')
            longitude = pd.to_numeric(row['longitude'], errors='coerce')
            country = row['country']
            
            if pd.notna(latitude) and pd.notna(longitude):
                # Round latitude and longitude to the nearest 0.25 or 0.75
                rounded_latitude = round_to_nearest_quarter(latitude)
                rounded_longitude = round_to_nearest_quarter(longitude)

                # Check for the closest available row in locationData[0]
                closest_row = find_closest(rounded_latitude, rounded_longitude, locationData[0])
                cls_value = closest_row['Cls']
                #print(f"Lat {rounded_latitude}, Lon {rounded_longitude} is closest to Lat {closest_row['Lat']}, Lon {closest_row['Lon']} with Cls {cls_value}")

                descriptions = get_description_from_cls(cls_value, locationData[1])

                update_location = text("""
                    UPDATE "Location_Dim"
                    SET latitude = :latitude, longitude = :longitude, climate_zone = :climate_zone, precipitation_zone = :precipitation_zone, temperature_zone = :temperature_zone
                    WHERE country = :country;
                """)
                connection.execute(update_location, {
                    'country': country,
                    'latitude': rounded_latitude,
                    'longitude': rounded_longitude,
                    'climate_zone': descriptions.get('Main Climate'),
                    'precipitation_zone': descriptions.get('Precipitation'),
                    'temperature_zone': descriptions.get('Temperature'),
                })


        print("The 'Location_Dim' latitude, longitude and zones correctly populated \n")


    except Exception as e:
        print(f"An error occurred: {e}")



def loadWaterExtraData(df, connection, lookupTables):
        
    try:
        
        ### Location_Dim handler

        for country in df['Country Name']:
            city="Undefined"
            if not (country,city) in lookupTables[0] :
                insert_country = text('''
                                    INSERT INTO "Location_Dim" (country, city) 
                                    VALUES (:country, :city)
                                    RETURNING location_id;
                                    ''')
                new_insert = connection.execute(insert_country, {'country': country, 'city': city})
                location_id = new_insert.fetchone()[0]
                lookupTables[0][(country, city)] = location_id
        print("The 'Location_Dim' table correctly populated")


        ### Source_Dim handler

        for index, row in df.iterrows():

            source_name = row['Responsible Collection Agency']
            if pd.isna(source_name) : source_name = row['Station Identifier']
            source_type = row['Water Type']
            source_data_quality = row['Station Narrative']
            if pd.isna(source_data_quality) : source_data_quality = "Not Provided"

            insert_source = text('''
                                INSERT INTO "Source_Dim" (source_name, source_type, source_data_quality) 
                                VALUES (:source_name, :source_type, :source_data_quality)
                                ON CONFLICT (source_name) DO NOTHING
                                RETURNING source_id;
                                ''')
            new_insert = connection.execute(insert_source, {
                    'source_name': source_name,
                    'source_type': source_type,
                    'source_data_quality': source_data_quality
                })
            source_id = new_insert.fetchone()
            if source_id is not None:
                lookupTables[3][source_name] = source_id[0]
        print("The 'Source_Dim' table correctly populated")
        

    except Exception as e:
        print(f"An error occurred: {e}")