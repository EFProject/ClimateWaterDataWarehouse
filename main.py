import sys

### EXTRACTION ###

from ETL_tools.extractionAPI import *

climateDF = extractClimateData()
print("\n Dataframe: \n",climateDF[1][1])
#print(CID.index, "\n", CID.columns ,"\n", CID.dtypes, "\n", CID.values)

### TRASFORMATION ###

from ETL_tools.transformationAPI import *

dfStandardized = applyStandardizationFormat(climateDF[1][1])
print("\n Dataframe after StandardizationFormat: \n",dfStandardized)

### CLEANSING ###

from ETL_tools.cleansingAPI import *

dfCleaned = handleDuplicatesRemoval(dfStandardized)
print("\n Dataframe after DuplicatesRemoval : \n",dfCleaned)

dfCleaned = handleMissingValuesRemoval(dfCleaned, 0.8)
print("\n Dataframe after MissingValuesRemoval : \n",dfCleaned)

dfImputated = handleMissingValuesImputation(dfCleaned)
print("\n Dataframe after MissingValuesImputation: \n",dfImputated)

### EXPLORATION ###

from utils.explorationAPI import *

#print(getDescriptionStatistics(dfImputated))

#getDataDistribution(dfImputated)

# getDataCorrelation(dfImputated)

### LOADING ###

from utils.dbConnectionHandler import connection_handler
from ETL_tools.loadingAPI import *

dbConnection = connection_handler()
if (dbConnection == None): sys.exit() 

createSchema(dbConnection)

loadDataFrame(dfImputated, dbConnection, 'example_table')