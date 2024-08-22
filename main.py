
### EXTRACTION ###

from ETL_tools.extractionAPI import *

climateDF = extractClimateData()
print(climateDF[1][1])
#print(CID.index, "\n", CID.columns ,"\n", CID.dtypes, "\n", CID.values)

### CLEANSING ###

from ETL_tools.cleansingAPI import *

dfCleaned = handleMissingValuesRemoval(climateDF[1][1], 0.8)
print(dfCleaned)