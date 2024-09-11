import datetime
import numpy as np
from sqlalchemy import text
from utils.formulas import *
from utils.pandasAPI import *


def loadWaterDataFrame(df, connection, lookUpCodeTable):

    try:

        ### Date_Dim handler

        for date in df['Date']:

            insert_date = text('''
                                INSERT INTO "Date_Dim" (date, month, quarter, year) 
                                VALUES (:date, :month, :quarter, :year)
                                ON CONFLICT (date) DO NOTHING
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
            date_id = new_insert.fetchone()
            if date_id is not None:
                lookUpCodeTable[3][date] = date_id[0]
            else:
                # If the insert failed due to conflict, retrieve the existing date_id
                select_date_id = text('''
                    SELECT date_id FROM "Date_Dim" WHERE date = :date;
                ''')
                existing_date_id = connection.execute(select_date_id, {'date': date}).fetchone()
                
                if existing_date_id:
                    lookUpCodeTable[3][date] = existing_date_id[0]

        print("The 'Date_Dim' table correctly populated")


        ### Environment_Fact handler

        for index, row in df.iterrows():

            date_id = lookUpCodeTable[3][row['Date']]
            if date_id is None:
                print(f"Date of '{row['Date']}' not found in date_dim.")
                #continue    # Skip this row if date_id is not found

            location_id = lookUpCodeTable[0][row['Station Number']]
            if location_id is None:
                print(f"Location of '{row['Station Number']}' not found in location_dim.")
                #continue    # Skip this row if location_id is not found
            
            source_id = lookUpCodeTable[1][row['Station Number']]
            if source_id is None:
                print(f"Source of '{row['Station Number']}' not found in source_dim.")
                #continue    # Skip this row if source_id is not found
            
            param_id = lookUpCodeTable[2][row['Code Param']]
            if param_id is None:
                print(f"Parameter '{row['Code Param']}' not found in param_dim.")
                #continue    # Skip this column if param_id is not found
            
            measurement_value = round(row['Value'], 3) # Round to 3 decimal places
            
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

    except Exception as e:
        print(f"An error occurred: {e}")



def loadWaterExtraData(stationData, paramData, methodData, connection, lookupTables, clsData):
        
    try:
        
        ### Location_Dim handler

        for index, row in stationData.iterrows():
            station_code = row['GEMS Station Number']
            country = row['Country Name']
            city ="Undefined"
            if not (country,city) in lookupTables[0] :

                # Merge with Climate Data

                latitude = pd.to_numeric(row['Latitude'], errors='coerce')
                longitude = pd.to_numeric(row['Longitude'], errors='coerce')
                rounded_latitude = round_to_nearest_quarter(latitude)
                rounded_longitude = round_to_nearest_quarter(longitude)
                closest_row = find_closest(rounded_latitude, rounded_longitude, clsData[0])
                cls_value = closest_row['Cls']
                descriptions = get_description_from_cls(cls_value, clsData[1])

                insert_country = text('''
                                    INSERT INTO "Location_Dim" (country, city, latitude, longitude, climate_zone, precipitation_zone, temperature_zone) 
                                    VALUES (:country, :city, :latitude, :longitude, :climate_zone, :precipitation_zone, :temperature_zone)
                                    RETURNING location_id;
                                    ''')
                new_insert = connection.execute(insert_country, {
                    'country': country,
                    'city': city,
                    'latitude': rounded_latitude,
                    'longitude': rounded_longitude,
                    'climate_zone': descriptions.get('Main Climate'),
                    'precipitation_zone': descriptions.get('Precipitation'),
                    'temperature_zone': descriptions.get('Temperature'),
                })
                location_id = new_insert.fetchone()[0]
                lookupTables[0][(country, city)] = location_id
        print("The 'Location_Dim' table correctly populated")


        ### Source_Dim handler

        for index, row in stationData.iterrows():

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
            else:
                # If the insert failed due to conflict, retrieve the existing source_id
                select_source_id = text('''
                    SELECT source_id FROM "Source_Dim" WHERE source_name = :source_name;
                ''')
                existing_source_id = connection.execute(select_source_id, {'source_name': source_name}).fetchone()
                
                if existing_source_id:
                    lookupTables[3][source_name] = existing_source_id[0]

        print("The 'Source_Dim' table correctly populated")


        ### Param_Dim handler

        for index, row in paramData.iterrows():
            param_code = row['Parameter Code']
            param_name = row["Parameter Name"]

            if not param_name in lookupTables[1] :
                unit_of_measure = methodData.loc[methodData['Parameter Code'] == param_code, 'Unit'].values[0]

                insert_param = text("""
                    INSERT INTO "Param_Dim" (param_name, unit_of_measure, category, sub_category, category_detail, description)
                    VALUES (:param_name, :unit_of_measure, :category, :sub_category, :category_detail, :description)
                    RETURNING param_id;
                """)
                new_insert = connection.execute(insert_param, {
                    'param_name': param_name,
                    'unit_of_measure': unit_of_measure,
                    'category': row['Category'],
                    'sub_category': row['Sub_Category'],
                    'category_detail': row['Category_Detail'],
                    'description': row['Parameter Description']
                })
                param_id = new_insert.fetchone()[0]
                lookupTables[1][param_name] = param_id
        print("The 'Param_Dim' table correctly populated")


    except Exception as e:
        print(f"An error occurred: {e}")


def getLookUpCodeTable(stationData, paramData, lookupTables):

    lookUpCodeTable = [{},{},{},{}]

    for index, row in stationData.iterrows():
        location_id = lookupTables[0][row['Country Name'],"Undefined"]
        lookUpCodeTable[0][row['GEMS Station Number']] = location_id

        source_name = row['Responsible Collection Agency']
        if pd.isna(source_name) : source_name = row['Station Identifier']
        source_id = lookupTables[3][source_name]
        lookUpCodeTable[1][row['GEMS Station Number']] = source_id

    for index, row in paramData.iterrows():
        param_id = lookupTables[1][row["Parameter Name"]]
        lookUpCodeTable[2][row['Parameter Code']] = param_id

    #print(lookUpCodeTable)

    return lookUpCodeTable