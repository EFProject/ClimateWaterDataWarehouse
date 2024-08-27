# Trasformation Phase: It converts data from its operational source format intoa specific data warehouse format.

import numpy as np

from utils.pandasAPI import *


def standardize_numerical_format(value):

	if(not isinstance(value, (int, float))): 
		value = value.strip()
		value = value.replace(' ', '')
		value = value.replace(',', '.')

	try:
		return pd.to_numeric(value, errors='coerce')  # Convert to numeric, errors='coerce' will convert invalid parsing to NaN
	except ValueError:
		return np.nan
	
def standardize_string_format(value):

	value = value.strip()
	#value = value.replace(' ', '_')
	value = value.replace(',', '.')

	return value


def applyStandardizationFormat(df):

	numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df)				# Separate numerical Data from not numerical Data

	numericalDf = numericalDf.applymap(standardize_numerical_format)						# Apply the standardization function

	non_numericalRows = non_numericalRows.applymap(standardize_string_format)	
	non_numericalColumns = non_numericalColumns.applymap(standardize_string_format)	
	
	dfFormatted = pd.concat([non_numericalColumns, numericalDf], axis=1).reset_index(drop=True)
	dfFormatted = pd.concat([non_numericalRows, dfFormatted], axis=0).reset_index(drop=True)

	dfFormatted.replace(["...", "…", ""], np.nan, inplace=True)								# Replace all missing values with NaN

	return dfFormatted