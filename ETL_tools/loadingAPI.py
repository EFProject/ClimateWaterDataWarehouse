from sqlalchemy import text
from utils.pandasAPI import *

def loadDataFrame(df, connection, lookupTables):

    numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df)

    try:

        ### Location_Dim handler

        if 'Country' in non_numericalColumns.columns and not non_numericalColumns['Country'].empty:
            for country in non_numericalColumns['Country']:
                if not country in lookupTables[0] :
                    insert_country = text('''
                                        INSERT INTO "Location_Dim" (country) 
                                        VALUES (:country)
                                        RETURNING location_id;
                                        ''')
                    new_insert = connection.execute(insert_country, {'country': country})
                    location_id = new_insert.fetchone()[0]
                    lookupTables[0][country] = location_id
            print("The 'Location_Dim' table correctly populated")
        else:
            print("The 'Country' column is either missing or empty in the DataFrame.")

        ### Param_Dim handler

        if not non_numericalRows.empty:
            non_numericalRows = non_numericalRows.iloc[:,1:]
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
        else:
            print("The row with unit of measures is either missing or empty in the DataFrame.")


        print("\n LookupTables status:")
        print("Location_Dim : ", len(lookupTables[0]), " rows, ","Param_Dim : ", len(lookupTables[1]), " rows \n")


        ### Environment_Fact handler




        

        #numericalDf.to_sql("Location_Dim", connection, schema='ClimateWaterDataWarehouse', if_exists='replace', index=False)

    except Exception as e:
        print(f"An error occurred: {e}")