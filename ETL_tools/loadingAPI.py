from sqlalchemy import text
from utils.pandasAPI import *

def loadDataFrame(df, connection, lookupTables):

    numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df)

    try:

        ### Date_Dim handler

        if 'latest year available' in non_numericalColumns.columns and not non_numericalColumns['latest year available'].empty:
            for year in non_numericalColumns['latest year available']:
                month = '01'
                quarter = 'Q1'
                date = f'{year}-{month}-01'
                if not date in lookupTables[2] :
                    insert_date = text('''
                                        INSERT INTO "Date_Dim" (date, month, quarter, year) 
                                        VALUES (:date, :month, :quarter, :year)
                                        RETURNING date_id;
                                        ''')
                    new_insert = connection.execute(insert_date, {
                        'date': date,
                        'month': month,
                        'quarter': quarter,
                        'year': year
                    })
                    date_id = new_insert.fetchone()[0]
                    lookupTables[2][date] = date_id
            print("The 'Date_Dim' table correctly populated")
        else:
            print("The 'latest year available' column is either missing or empty in the DataFrame.")

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
        print("Date_Dim : ", len(lookupTables[2]), "Location_Dim : ", len(lookupTables[0]), " rows, ","Param_Dim : ", len(lookupTables[1]), " rows \n")

        print(non_numericalColumns.columns,len(non_numericalColumns.columns))


        ### Environment_Fact handler

        for index, row in df.iloc[1:].iterrows():
            location_id = lookupTables[0].get(row['Country'])
            if location_id is None:
                print(f"Location of '{row['Country']}' not found in location_dim.")
                #continue    # Skip this row if location_id is not found
            
            for column in df.columns[len(non_numericalColumns.columns):]:
                param_id = lookupTables[1].get(column)
                if param_id is None:
                    print(f"Parameter '{column}' not found in param_dim.")
                    #continue  # Skip this column if param_id is not found
                
                measurement_value = round(row[column], 3) # Round to 3 decimal places
                
                insert_mv = text('''
                    INSERT INTO "Environment_Fact" (location_id, param_id, measurement_value)
                    VALUES (:location_id, :param_id, :measurement_value)
                    ON CONFLICT DO NOTHING;
                ''')
                
                connection.execute(insert_mv, {
                    'location_id': location_id,
                    'param_id': param_id,
                    'measurement_value': measurement_value
                })

        result = connection.execute(text('SELECT COUNT(*) FROM "Environment_Fact";'))
        count = result.scalar()
        print(f"The 'Environment_Fact' table correctly populated : {count} rows \n ")
        

        #numericalDf.to_sql("Location_Dim", connection, schema='ClimateWaterDataWarehouse', if_exists='replace', index=False)

    except Exception as e:
        print(f"An error occurred: {e}")