import sys

from ETL_tools.extractionAPI import *
from ETL_tools.transformationAPI import *
from ETL_tools.cleansingAPI import *
from utils.explorationAPI import *
from utils.dbConnectionHandler import connection_handler
from utils.dbSetUpHandler import *
from ETL_tools.loadingAPI import *


### SETUP DB ###

dbConnection = connection_handler()
if (dbConnection == None): sys.exit() 

setUpDB(dbConnection)

lookupTables = getLookupTable(dbConnection)


### EXTRACTION ###

climateDF = extractClimateData()

sourceData, parameterData = extractClimateExtraData()
lookupTables.append(loadSourceData(dbConnection, sourceData))


for dfName, dfExtracted in climateDF[1].items():
    
    #print(f"\nDataframe after extraction: {dfName}\n", dfExtracted)

	### TRASFORMATION ###

	dfMapped = applyMappingFormat(dfExtracted, dfName)
	#print(f"\nDataframe after MappingFormat: {dfName}\n", dfMapped)

	dfStandardized = applyStandardizationFormat(dfMapped)
	#print(f"\nDataframe after StandardizationFormat: {dfName}\n", dfStandardized)

	### CLEANSING ###

	dfCleaned = handleDuplicatesRemoval(dfStandardized)
	#print(f"\n Dataframe after DuplicatesRemoval : {dfName}\n",dfCleaned)

	dfCleaned = handleMissingValuesRemoval(dfCleaned, 0.8)
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

loadExtraData(dbConnection, parameterData)
print (f"### Extra Data correctly loaded into ClimateWaterDataWarehouse ###\n")