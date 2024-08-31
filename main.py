import sys

### EXTRACTION ###

from ETL_tools.extractionAPI import *

climateDF = extractClimateData()
print("\n Dataframe: \n",climateDF[1][2])

### TRASFORMATION ###

from ETL_tools.transformationAPI import *

dfStandardized = applyStandardizationFormat(climateDF[1][2])
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

### SETUP DB ###

from utils.dbConnectionHandler import connection_handler
from utils.dbSetUpHandler import *

dbConnection = connection_handler()
if (dbConnection == None): sys.exit() 

setUpDB(dbConnection)

lookupTables = getLookupTable(dbConnection)

### LOADING ###

from ETL_tools.loadingAPI import *

loadDataFrame(dfImputated, dbConnection, lookupTables)