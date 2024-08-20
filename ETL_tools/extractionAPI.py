# Extraction Phase: relevant data is obtained from different sources (external data). 
# In this case, DW is populated for the first time (Static Extraction). 

import os
import glob

from utils.pandasAPI import *

def extractClimateData():

	basePath = os.getcwd()
	climatePath = basePath + '/datasets/Climate-Dataset/'

	CID = read_csv_file(climatePath + '1.1_Climate_Insights_Dataset/climate_change_data.csv', 0, None, None)

	GEI = []

	# glob is used for getting all Excel files in the parent directory and its subdirectories
	excel_files = glob.glob(os.path.join(climatePath + '1.2_Global_Environmental_Indicators/', '**/*.xlsx'), recursive=True)

	for file in excel_files:
		# Read the Excel file into a DataFrame
		df = pd.read_excel(file)
		
		# Append the DataFrame to the list
		GEI.append(df)

	# Concatenate all DataFrames into a single DataFrame
	combined_df = pd.concat(GEI, ignore_index=True)

	# Display the combined DataFrame
	print(combined_df)

	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
	# df = read_excel_file(climatePath + '1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)

	KGC_1 = read_csv_file(climatePath + '1.3_Köppen_Geiger_Classification/Koeppen-Geiger-ASCII.txt', 0, None, None)
	KGC_2 = read_csv_file(climatePath + '1.3_Köppen_Geiger_Classification/TableDefinitions.csv', 0, None, None)

	KGC = [KGC_1, KGC_2]

	df = KGC
	print(df)
	#print(CID.index, "\n", CID.columns ,"\n", CID.dtypes, "\n", CID.values)