import sys

from ETL_tools.extractionAPI import *
from datasetOrchestrator import *

from utils.explorationAPI import *
from utils.dbConnectionHandler import connection_handler
from utils.dbSetUpHandler import *
from ETL_tools.loadingClimateDataAPI import *


### SETUP DB ###

dbConnection = connection_handler()
if (dbConnection == None): sys.exit() 

setUpDB(dbConnection)

lookupTables = getLookupTable(dbConnection)

### EXTRACTION ###

climateDF = extractClimateData()
waterDF = extractWaterData() 

sourceData, parameterData = extractClimateExtraData()
locationData = extractExtraData()
lookupTables.append(loadSourceData(dbConnection, sourceData))

### ETL ###

CID_dataset_ETL(climateDF[0], 0.8, dbConnection, lookupTables)

GEI_dataset_ETL(climateDF[1].items(), 0.8, dbConnection, lookupTables)

lookUpCodeTable = GGI_ExtraData_ETL(waterDF, 0.8, dbConnection, lookupTables, locationData)

GGI_dataset_ETL(waterDF, 0.8, dbConnection, lookUpCodeTable)


loadExtraData(dbConnection, parameterData, locationData)
print (f"\n ### Extra Data correctly loaded into ClimateWaterDataWarehouse ###\n")