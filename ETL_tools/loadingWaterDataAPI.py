import datetime
import numpy as np
from sqlalchemy import text
from utils.formulas import *
from utils.pandasAPI import *


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

    lookUpCodeTable = {}

    # for stationCode in stationData['GEMS Station Number'] :
    #     stationData.loc[stationData['Parameter Code'] == stationCode, 'Unit'].values[0]
    #     lookupTables[0][(country, city)] = location_id

    return lookUpCodeTable