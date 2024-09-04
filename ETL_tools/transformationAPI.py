# Trasformation Phase: It converts data from its operational source format intoa specific data warehouse format.

import datetime
from datetime import datetime
from datetime import date
import numpy as np

from utils.pandasAPI import *


def standardize_numerical_format(value):

	if(not isinstance(value, (int, float))): 
		value = value.strip()
		value = value.replace(' ', '')
		value = value.replace(',', '.')

	try:
		return pd.to_numeric(value, errors='coerce')  	# Convert to numeric, errors='coerce' will convert invalid parsing to NaN
	except ValueError:
		return np.nan									# Replace all missing values with NaN
	
def standardize_string_format(value):

	if(not isinstance(value, (int, float))): 
		value = value.strip()
		#value = value.replace(' ', '_')
		value = value.replace(',', '.')
	
	if value == "..." or  value == "" or value == "…" :
		return np.nan									# Replace all missing values with NaN

	return value

def standardize_datetime_format(value):
	
	if pd.notna(value) and not isinstance(value, (pd.Timestamp, date)):
		if value == "..." or  value == "" or value == "…" : return np.nan
		year = int(value)
		month = 1
		date_obj = date(year, month=month, day=1)
		return date_obj
	
	return value

def standardize_datetime_format_CID(value):

		
	if value == "..." or  value == "" or value == "…" : return np.nan

	date = pd.to_datetime(value).date()
	date = date.replace(day=1)
	
	return date


def applyStandardizationFormat(df):

	numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df, 1, 2)				# Separate numerical Data from not numerical Data

	numericalDf = numericalDf.map(standardize_numerical_format)								# Apply the standardization function

	non_numericalRows = non_numericalRows.map(standardize_string_format)	
	non_numericalColumns['Country'] = non_numericalColumns['Country'].map(standardize_string_format)	
	non_numericalColumns['Date'] = non_numericalColumns['Date'].map(standardize_datetime_format)
	
	dfFormatted = pd.concat([non_numericalColumns, numericalDf], axis=1).reset_index(drop=True)
	dfFormatted = pd.concat([non_numericalRows, dfFormatted], axis=0).reset_index(drop=True)

	return dfFormatted

def applyStandardizationFormatCID(df):

	numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df, 1, 3)				

	numericalDf = numericalDf.map(standardize_numerical_format)								# Apply the standardization function

	non_numericalRows = non_numericalRows.map(standardize_string_format)	
	non_numericalColumns['Country'] = non_numericalColumns['Country'].map(standardize_string_format)
	non_numericalColumns['Location'] = non_numericalColumns['Location'].map(standardize_string_format)	
	non_numericalColumns['Date'] = non_numericalColumns['Date'].map(standardize_datetime_format_CID)
	
	dfFormatted = pd.concat([non_numericalColumns, numericalDf], axis=1).reset_index(drop=True)
	dfFormatted = pd.concat([non_numericalRows, dfFormatted], axis=0).reset_index(drop=True)

	return dfFormatted


def applyMappingFormat(df, dfName):

	# Date Mapping

	if 'latest year available' in df.columns :
		df.rename(columns={'latest year available': 'Date'}, inplace=True)
	else :
		month = 1
		year = 2000
		if dfName == "CO2_Emissions" : 
			year = 2011 
			month = 2
		if dfName == "ODS_Consumption_2002" : 
			year = 2002 
			month = 10
		if dfName == "ODS_Consumption_2013" : 
			year = 2013 
			month = 10
		
		df.insert(1, 'Date', date(year, month, day=1))
		df.loc[0, "Date"] = np.nan

	# Param Mapping

	if dfName == "CH4_N2O_Emissions" : 
		df.rename(columns={f' % change since 1990': f'CH4 emissions % change since 1990'}, inplace=True)
		df.rename(columns={f' % change since 1990.1': f'N20 emissions % change since 1990'}, inplace=True)
		df = pd.concat([pd.DataFrame([{'Country': "", 'Date': "", "CH4 emissions": "mio. tonnes", "CH4 emissions per capita": "tonnes", f'CH4 emissions % change since 1990': "%", "N2O emissions": "mio. tonnes", "N2O emissions per capita": "tonnes", f'N20 emissions % change since 1990': "%" }]), df], ignore_index=True)
	if dfName == "CO2_Emissions" : 
		df.rename(columns={f'% change since 1990': f'CO2 emissions % change since 1990'}, inplace=True)
	if dfName == "GHG_Emissions" : 
		df.rename(columns={f'% change since 1990': f'GHG emissions % change since 1990'}, inplace=True)
	if dfName == "NOx_Emissions" : 
		df.rename(columns={f'% change since 1990': f'NOx emissions % change since 1990'}, inplace=True)
	if dfName == "ODS_Consumption_2002" : 
		df = df.iloc[1:].reset_index(drop=True)
		df.loc[0, "Date"] = np.nan
	if dfName == "ODS_Consumption_2013" : 
		df.rename(columns={f'Unnamed: 10': f'Consumption of all ODS'}, inplace=True)
		df = df.iloc[1:].reset_index(drop=True)
		df.loc[0, "Date"] = np.nan
	if dfName == "SO2_emissions" : 
		df.rename(columns={f'% change since 1990': f'SO2 emissions % change since 1990'}, inplace=True)


	return df