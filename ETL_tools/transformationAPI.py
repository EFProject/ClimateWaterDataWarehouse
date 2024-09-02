# Trasformation Phase: It converts data from its operational source format intoa specific data warehouse format.

import datetime
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
	
	if not pd.isna(value) :
		year = int(value)
		month = 1
		#quarter = 'Q1'
		date = datetime.date(year, month=month, day=1)
		return date

	return value


def applyStandardizationFormat(df):

	numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df)				# Separate numerical Data from not numerical Data

	numericalDf = numericalDf.map(standardize_numerical_format)								# Apply the standardization function

	non_numericalRows = non_numericalRows.map(standardize_string_format)	
	non_numericalColumns = non_numericalColumns.map(standardize_string_format)	
	if 'latest year available' in non_numericalColumns.columns :
		non_numericalColumns['latest year available'] = non_numericalColumns['latest year available'].map(standardize_datetime_format)
	
	dfFormatted = pd.concat([non_numericalColumns, numericalDf], axis=1).reset_index(drop=True)
	dfFormatted = pd.concat([non_numericalRows, dfFormatted], axis=0).reset_index(drop=True)

	return dfFormatted