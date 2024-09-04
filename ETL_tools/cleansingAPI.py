# Cleansing Phase: improve data qualit, normally quite poor in sources.

from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from utils.pandasAPI import *


def handleMissingValuesRemoval(df, threshold):

	df.dropna(axis=0, how='all', inplace=True)						# Drop the rows where all elements are missing.
	df.dropna(axis=1, how='all', inplace=True)						# Drop the columns where all elements are missing.

	# Calculate the threshold for non-missing values
	col_threshold = int(df.shape[0] * (1 - threshold))
	row_threshold = int(df.shape[1] * (1 - threshold))

	df.dropna(axis=0, thresh=row_threshold, inplace=True)			# Drop rows with threshold% or more missing values
	df.dropna(axis=1, thresh=col_threshold, inplace=True)			# Drop columns with threshold% or more missing values

	first_row = df.iloc[[0]]																# exclude row with unit of measure
	df.dropna(axis=0, subset=['Country', 'Date'], inplace=True)			# Drop all rows where 'Country' or 'Date' has NaN values
	df_cleaned = pd.concat([first_row, df], ignore_index=True)								

	return df_cleaned.reset_index(drop=True)


def handleMissingValuesImputation(df):

	numericalDf, non_numericalRows, non_numericalColumns = getNumericalData(df, 1, 2)			# Separate numerical Data from not numerical Data

	imputer = IterativeImputer(max_iter=10, random_state=0)										# Use IterativeImputer to perform Imputation
	imputed_array = imputer.fit_transform(numericalDf)
	imputed_numerical_df = pd.DataFrame(imputed_array, columns=numericalDf.columns)
	imputed_numerical_df.index = imputed_numerical_df.index + 1 								# Resetting the index to start from the number of excluded rows

	#print("\n Numerical data after imputation: \n", imputed_numerical_df)

	dfImputed = pd.concat([non_numericalColumns, imputed_numerical_df], axis=1).reset_index(drop=True)
	dfImputed = pd.concat([non_numericalRows, dfImputed], axis=0).reset_index(drop=True)

	# df['parameter_value'].fillna(df['parameter_value'].mean(), inplace=True)		# Filling missing values with the column mean

	return dfImputed


def handleDuplicatesRemoval(df):

	df.drop_duplicates(inplace=True) 		# Remove duplicate rows

	return df