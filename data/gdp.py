
import akshare as ak
import pymongo

# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# ddblist = myclient.list_database_names()
# print(ddblist)

lastDate = "2020-01-01"
gdp = ak.macro_china_gdp()
date = gdp.iloc[:, 0]
addData = gdp[gdp["季度"]>=lastDate]
for row1, row2 in  zip(addData[gdp.columns[0]],addData[gdp.columns[1]]):
    print(row1,row2)