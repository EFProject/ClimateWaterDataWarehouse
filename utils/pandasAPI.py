import pandas as pd


def read_excel_file(file_path, sheet_name, header, index_col, usecols, skiprows, na_filter, skipfooter):
    """Reads an Excel file and returns the DataFrame."""
    try:
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name, 
            header=header, 				# Row (0-indexed) to use for the column labels of the parsed DataFrame
            index_col=index_col,		# Column (0-indexed) to use as the row labels of the DataFrame
            usecols=usecols,			# Column letters or Column Ranges to be parsed
            skiprows=skiprows,			# Line numbers to skip (0-indexed) or number of lines to skip (int) at the start of the file
            na_filter=na_filter,		# Detect missing value markers, passing na_filter=False can improve the performance of reading a large file
            skipfooter=skipfooter,      # Rows at the end to skip (0-indexed)
            )
        return df
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None


def read_csv_file(file_path, sep, header, index_col, usecols):
    """Load the CSV file into a DataFrame"""
    try:
        df = pd.read_csv(
            file_path,  
            sep=sep,
            engine='python',
            header=header, 				# Row (0-indexed) to use for the column labels of the parsed DataFrame
            index_col=index_col,		# Column (0-indexed) to use as the row labels of the DataFrame
            usecols=usecols,			# Column letters or Column Ranges to be parsed
            )
        return df
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    

def getNumericalData(df, nnnRows, nnnColumns):
    
    numericalDf = df.iloc[nnnRows:, nnnColumns:] 		                # Dataframe with numerical rows and columns

    non_numericalRows = df.iloc[:nnnRows, :]					# Dataframe with rows excluded
    non_numericalColumns = df.iloc[nnnRows:, :nnnColumns]	            # Dataframe with columns excluded

    #print(non_numericalRows, "\n", non_numericalColumns)

    return numericalDf, non_numericalRows, non_numericalColumns