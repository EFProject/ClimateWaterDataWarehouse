import os
import pandas as pd

from utils.pandasAPI import read_excel_file

basePath = os.getcwd()
# climate_data = pd.read_csv('path_to_climate_data.csv')

# water_quality_data = pd.read_excel(basePath + '/datasets/WQ-Dataset/GemStat/River_Europe/metadata.xlsx')

print(os.getcwd())
# print(water_quality_data)

df = read_excel_file(basePath + '/datasets/Climate-Dataset/1.2_Global_Environmental_Indicators/Air and Climate/CO2_Emissions.xlsx', None, 16, None, "B,C,E,G,I", 0, False)
print(df)