# Trasformation Phase: It converts data from its operational source format intoa specific data warehouse format.

import numpy as np

from utils.pandasAPI import *


def standardize_format(val):

	val = val.replace(' ', '')			# Remove spaces

	val = val.replace(',', '.')			# Replace comma with dot for consistent decimal separator

	return val


def applyStandardizationFormat(df):

	numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df)			# Separate numerical Data from not numerical Data

	numericalDf = numericalDf.apply(standardize_format)									# Apply the standardization function
	numericalDfFormatted = numericalDf.apply(pd.to_numeric, errors='coerce')			# Convert to numeric, errors='coerce' will convert invalid parsing to NaN

	dfFormatted = pd.concat([non_numericalColumns, numericalDfFormatted], axis=1).reset_index(drop=True)
	dfFormatted = pd.concat([non_numericalRows, dfFormatted], axis=0).reset_index(drop=True)

	dfFormatted.replace(["...", "…", ""], np.nan, inplace=True)							# Replace all missing values with NaN

	return dfFormatted