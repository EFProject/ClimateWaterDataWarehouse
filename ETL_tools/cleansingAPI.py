# Cleansing Phase: improve data qualit, normally quite poor in sources.

import numpy as np

from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from utils.pandasAPI import *


def handleMissingValuesRemoval(df, threshold):

	df.replace(["...", "…", ""], np.nan, inplace=True)				# Replace all missing values with NaN

	df.dropna(axis=0, how='all', inplace=True)						# Drop the rows where all elements are missing.
	df.dropna(axis=1, how='all', inplace=True)						# Drop the columns where all elements are missing.

	# Calculate the threshold for non-missing values
	col_threshold = int(df.shape[0] * (1 - threshold))
	row_threshold = int(df.shape[1] * (1 - threshold))

	df.dropna(axis=0, thresh=row_threshold, inplace=True)			# Drop rows with threshold% or more missing values
	df.dropna(axis=1, thresh=col_threshold, inplace=True)			# Drop columns with threshold% or more missing values

	return df


def handleMissingValuesImputation(df):

	# Separate numerical and non-numerical columns
	numerical_cols = df.select_dtypes(include=[np.number]).columns
	non_numerical_cols = df.select_dtypes(exclude=[np.number]).columns

	df_numerical_cols = df.iloc[:, 1:] #take only the first column (country)
	df_excluding_first_row = df_numerical_cols.drop(index=0).reset_index(drop=True) #exclude first row (unit of measure)
	print(df_excluding_first_row)

	imputer = IterativeImputer(max_iter=10, random_state=0)
	imputed_array = imputer.fit_transform(df_excluding_first_row)
	imputed_numerical_df = pd.DataFrame(imputed_array, columns=numerical_cols)

	final_df = pd.concat([imputed_numerical_df, df[non_numerical_cols]], axis=1) #reassmble new df with the excluded columns before

	print("After Imputation:")
	print(final_df)

	# df['parameter_value'].fillna(df['parameter_value'].mean(), inplace=True)		# Filling missing values with the column mean

	return df