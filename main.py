
### EXTRACTION ###

from ETL_tools.extractionAPI import *

climateDF = extractClimateData()
print(climateDF)
#print(CID.index, "\n", CID.columns ,"\n", CID.dtypes, "\n", CID.values)