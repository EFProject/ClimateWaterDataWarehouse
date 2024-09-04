from ETL_tools.transformationAPI import *
from ETL_tools.cleansingAPI import *
from ETL_tools.loadingAPI import *


### CID DATASET ###

def CID_dataset_ETL(dfCID, threshold, dbConnection, lookupTables):

	print(f"\n######### ETL on CID DATASET #########\n")

	#print(f"\nDataframe after extraction: \n", dfCID)

	### TRASFORMATION ###

	dfStandardized = applyStandardizationFormatCID(dfCID)
	#print(f"\nDataframe after StandardizationFormat: \n", dfStandardized)

	### CLEANSING ###

	dfCleaned = handleDuplicatesRemoval(dfStandardized)
	#print(f"\n Dataframe after DuplicatesRemoval : \n",dfCleaned)

	dfCleaned = handleMissingValuesRemoval(dfCleaned, threshold)
	print(f"\n Dataframe after MissingValuesRemoval : \n",dfCleaned)

	#dfImputated = handleMissingValuesImputation(dfCleaned)
	#print(f"\n Dataframe after MissingValuesImputation: \n",dfImputated)

	### EXPLORATION ###

	#print(getDescriptionStatistics(dfImputated))

	#getDataDistribution(dfImputated)

	# getDataCorrelation(dfImputated)

	### LOADING ###

	source_id = lookupTables[3]["CID"]
	#loadDataFrame(dfCleaned, dbConnection, lookupTables, source_id)
	print (f"### CID DATASET correctly loaded into ClimateWaterDataWarehouse ###\n")


### GEI DATASET ###

def GEI_dataset_ETL(dfGEI, threshold, dbConnection, lookupTables):	

	print(f"\n######### ETL on GEI DATASET #########\n")

	for dfName, dfExtracted in dfGEI:
		
		#print(f"\nDataframe after extraction: {dfName}\n", dfExtracted)

		### TRASFORMATION ###

		dfMapped = applyMappingFormat(dfExtracted, dfName)
		#print(f"\nDataframe after MappingFormat: {dfName}\n", dfMapped)

		dfStandardized = applyStandardizationFormat(dfMapped)
		#print(f"\nDataframe after StandardizationFormat: {dfName}\n", dfStandardized)

		### CLEANSING ###

		dfCleaned = handleDuplicatesRemoval(dfStandardized)
		#print(f"\n Dataframe after DuplicatesRemoval : {dfName}\n",dfCleaned)

		dfCleaned = handleMissingValuesRemoval(dfCleaned, threshold)
		#print(f"\n Dataframe after MissingValuesRemoval : {dfName}\n",dfCleaned)

		dfImputated = handleMissingValuesImputation(dfCleaned)
		#print(f"\n Dataframe after MissingValuesImputation: {dfName}\n",dfImputated)

		### EXPLORATION ###

		#print(getDescriptionStatistics(dfImputated))

		#getDataDistribution(dfImputated)

		# getDataCorrelation(dfImputated)

		### LOADING ###

		source_id = lookupTables[3][dfName]
		loadDataFrame(dfImputated, dbConnection, lookupTables, source_id)
		print (f"### {dfName} correctly loaded into ClimateWaterDataWarehouse ###\n")