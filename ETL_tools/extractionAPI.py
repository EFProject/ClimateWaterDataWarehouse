# Extraction Phase: relevant data is obtained from different sources (external data). 
# In this case, DW is populated for the first time (Static Extraction). 

import os
import glob

from utils.pandasAPI import *

def extractClimateData():

	basePath = os.getcwd()
	climatePath = basePath + '/datasets/Climate-Dataset/'

	CID = read_csv_file(climatePath + '1.1_Climate_Insights_Dataset/climate_change_data.csv', 0, None, None)

	GEIPath = climatePath + '1.2_Global_Environmental_Indicators/'

	GEI_AC_1 = read_csv_file(GEIPath + 'Air and Climate/CH4_N2O_Emissions.csv', 0, None, None)
	GEI_AC_2 = read_excel_file(GEIPath + 'Air and Climate/CO2_Emissions.xlsx', 0, 16, None, "B,C,E,G,I", 0, False, 0)
	GEI_AC_3 = read_excel_file(GEIPath + 'Air and Climate/GHG_by_Sector_Perc.xlsx', 0, 29, None, "B:I", 0, False, 0)
	GEI_AC_4 = read_excel_file(GEIPath + 'Air and Climate/GHG_Emissions_by_Sector.xlsx', 0, 16, None, "B:I", 0, False, 0)
	GEI_AC_5 = read_excel_file(GEIPath + 'Air and Climate/GHG_Emissions.xlsx', 0, 16, None, "B,C,D,F,H,J", 0, False, 20)
	GEI_AC_6 = read_excel_file(GEIPath + 'Air and Climate/NOx_Emissions.xlsx', 0, 16, None, "B,C,D,F,H", 0, False, 21)
	GEI_AC_7_A = read_excel_file(GEIPath + 'Air and Climate/ODS_Consumption.xlsx', 0, 20, None, "B,C,I", 0, False, 31)
	GEI_AC_7_B = read_excel_file(GEIPath + 'Air and Climate/ODS_Consumption.xlsx', 0, 20, None, "B,K", 0, False, 31)
	GEI_AC_8 = read_excel_file(GEIPath + 'Air and Climate/SO2_emissions.xlsx', 0, 17, None, "B,C,D,F,H", 0, False, 20)

	GEI = {"CH4_N2O_Emissions": GEI_AC_1, "CO2_Emissions": GEI_AC_2, "GHG_by_Sector_Perc": GEI_AC_3, "GHG_Emissions_by_Sector": GEI_AC_4, 
			"GHG_Emissions": GEI_AC_5, "NOx_Emissions": GEI_AC_6, "ODS_Consumption_2002": GEI_AC_7_A, "ODS_Consumption_2013": GEI_AC_7_B, 
			"SO2_emissions": GEI_AC_8}

	KGC_1 = read_csv_file(climatePath + '1.3_Köppen_Geiger_Classification/Koeppen-Geiger-ASCII.txt', 0, None, None)
	KGC_2 = read_csv_file(climatePath + '1.3_Köppen_Geiger_Classification/TableDefinitions.csv', 0, None, None)

	KGC = [KGC_1, KGC_2]

	return [CID, GEI, KGC]

def extractClimateExtraData():

	basePath = os.getcwd()
	climatePath = basePath + '/datasets/Climate-Dataset/'

	GEIPath = climatePath + '1.2_Global_Environmental_Indicators/'

	GEI_AC_2 = read_excel_file(GEIPath + 'Air and Climate/CO2_Emissions.xlsx', 0, 234, None, "A", 0, False, 0)
	GEI_AC_3 = read_excel_file(GEIPath + 'Air and Climate/GHG_by_Sector_Perc.xlsx', 0, 221, None, "A", 0, False, 0)
	GEI_AC_4 = read_excel_file(GEIPath + 'Air and Climate/GHG_Emissions_by_Sector.xlsx', 0, 205, None, "A", 0, False, 0)
	GEI_AC_5 = read_excel_file(GEIPath + 'Air and Climate/GHG_Emissions.xlsx', 0, 205, None, "A", 0, False, 0)
	GEI_AC_6 = read_excel_file(GEIPath + 'Air and Climate/NOx_Emissions.xlsx', 0, 185, None, "A", 0, False, 0)
	GEI_AC_7 = read_excel_file(GEIPath + 'Air and Climate/ODS_Consumption.xlsx', 0, 192, None, "A", 0, False, 0)
	GEI_AC_8 = read_excel_file(GEIPath + 'Air and Climate/SO2_emissions.xlsx', 0, 153, None, "A", 0, False, 0)

	GEI_Sources = [
			{ "nameDF" : "CH4_N2O_Emissions", "source_name" : GEI_AC_2.loc[1, 'Sources:'], "source_link" : GEI_AC_2.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_2.loc[13, 'Sources:']},
			{ "nameDF" : "CO2_Emissions", "source_name" : GEI_AC_2.loc[1, 'Sources:'], "source_link" : GEI_AC_2.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_2.loc[13, 'Sources:']},
			{ "nameDF" : "GHG_by_Sector_Perc", "source_name" : GEI_AC_3.loc[1, 'Sources:'], "source_link" : GEI_AC_3.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_3.loc[16, 'Sources:']},
			{ "nameDF" : "GHG_Emissions_by_Sector", "source_name" : GEI_AC_4.loc[1, 'Sources:'], "source_link" : GEI_AC_4.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_4.loc[16, 'Sources:']},
			{ "nameDF" : "GHG_Emissions", "source_name" : GEI_AC_5.loc[1, 'Sources:'], "source_link" : GEI_AC_5.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_5.loc[24, 'Sources:']},
			{ "nameDF" : "NOx_Emissions", "source_name" : GEI_AC_6.loc[1, 'Sources:'], "source_link" : GEI_AC_6.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_6.loc[28, 'Sources:']},
			{ "nameDF" : "ODS_Consumption_2002", "source_name" : GEI_AC_7.loc[1, 'Sources:'], "source_link" : GEI_AC_7.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_7.loc[32, 'Sources:']},
			{ "nameDF" : "ODS_Consumption_2013", "source_name" : GEI_AC_7.loc[1, 'Sources:'], "source_link" : GEI_AC_7.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_7.loc[32, 'Sources:']},
			{ "nameDF" : "SO2_emissions", "source_name" : GEI_AC_8.loc[1, 'Sources:'], "source_link" : GEI_AC_8.loc[2, 'Sources:'], "source_data_quality" : GEI_AC_8.loc[25, 'Sources:']}
		]
	
	GEI_Parameters = {
		"CO2 emissions" : GEI_AC_2.loc[8, 'Sources:'], 
		"GHG from Energy" : GEI_AC_3.loc[6, 'Sources:'], "GHG from Energy of which: from Transport" : GEI_AC_3.loc[7, 'Sources:'], "GHG from Industrial Processes" : GEI_AC_3.loc[8, 'Sources:'], "GHG from Agriculture" : GEI_AC_3.loc[9, 'Sources:'], "GHG from Waste" : GEI_AC_3.loc[10, 'Sources:'],
		"Total GHG emissions " : GEI_AC_5.loc[11, 'Sources:'], "Total GHG emissions including LULUCF/LUCF" : GEI_AC_5.loc[12, 'Sources:'], 
		"NOx emissions" : GEI_AC_6.loc[22, 'Sources:'], 
		"Consumption of CFCs" : GEI_AC_7.loc[18, 'Sources:'], "Consumption of all ODS" : GEI_AC_7.loc[18, 'Sources:'], 
		"SO2 emissions" : GEI_AC_8.loc[15, 'Sources:'], 
	}

	return GEI_Sources, GEI_Parameters


def extractEnvironmentalData():

	basePath = os.getcwd()
	climatePath = basePath + '/datasets/Environmental-Dataset/'

	GEIPath = climatePath + '1.2_Global_Environmental_Indicators/'

	GEI_B = read_excel_file(GEIPath + 'Biodiversity/Terrestrial_Marine protected areas.xlsx', "Data", 0, None, "B,C,D", 0, False, 0)
	GEI_F = read_excel_file(GEIPath + 'Forests\Forest Area.xlsx', "data", 0, None, "B:K", 0, False, 0)
	GEI_G = read_excel_file(GEIPath + 'Governance\Governance.xlsx', "Data", 0, None, "B:P", 0, False, 0)

	GEI_EM_1 = read_excel_file(GEIPath + 'Energy and Minerals\Contribution of mining to value added.xlsx', "Data", 0, None, None, 0, False, 0)
	GEI_EM_2 = read_excel_file(GEIPath + 'Energy and Minerals\Energy Indicators.xlsx', "Data", 0, None, "B,C,D,E", 0, False, 28)
	GEI_EM_3 = read_excel_file(GEIPath + 'Energy and Minerals\Energy intensity measured in terms of primary energy and GDP.xlsx', "Data", 0, None, "B:T", 0, False, 0)
	GEI_EM_4 = read_excel_file(GEIPath + 'Energy and Minerals\Energy supply per capita.xlsx', "Data", 0, None, "B:AD", 0, False, 28)
	GEI_EM_5 = read_excel_file(GEIPath + 'Energy and Minerals\Energy supply.xlsx', "Data", 0, None, "B:AD", 0, False, 28)
	GEI_EM_6 = read_excel_file(GEIPath + 'Energy and Minerals\Renewable elec production percentage.xlsx', "Data", 0, None, "B:AD", 0, False, 28)

	GEI = [
		GEI_B, GEI_F, GEI_G,
		GEI_EM_1, GEI_EM_2, GEI_EM_3, GEI_EM_4, GEI_EM_5, GEI_EM_6,
	]

	print(GEI)


def extractAllxlsxFile(directoryPath):
	# glob is used for getting all Excel files in the parent directory and its subdirectories
	excel_files = glob.glob(os.path.join(directoryPath, '**/*.xlsx'), recursive=True)

	dataframes = []

	for file in excel_files:
		df = pd.read_excel(file)
		dataframes.append(df)

	# Concatenate all DataFrames into a single DataFrame
	combined_df = pd.concat(dataframes, ignore_index=True)

	print(combined_df)