import os
import pandas as pd

basePath = os.getcwd()
# climate_data = pd.read_csv('path_to_climate_data.csv')

water_quality_data = pd.read_excel(basePath + '/datasets/WQ-Dataset/GemStat/River_Europe/metadata.xlsx')

print(os.getcwd())
print(water_quality_data)