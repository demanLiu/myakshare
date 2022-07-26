
import akshare as ak
import sys
sys.path.append(".")
import utils.mongodbUtil as mongodbUtil
from pymongo import UpdateOne

col = mongodbUtil.getCol("stock","gdp")

gdp = ak.macro_china_gdp()
date = gdp.iloc[:, 0]
updateOp=[]
for index,row in  gdp.iterrows():
    myquery = {"date": row[0] }
    valueItem={}
    valueItem["date"] = row[0]
    valueItem["gdp"]=row[1]
    valueItem["grow_rate"] = row[2]
    valueItem["first_amount"] = row[3]
    valueItem["first_grow"] = row[4]
    valueItem["second_amount"] = row[5]
    valueItem["second_grow"] = row[6]
    valueItem["third_amount"] = row[7]
    valueItem["third_grow"] = row[8]
    op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
    updateOp.append(op)

col.bulk_write(updateOp)
def  getQuarterValue(year):
    cursor = col.find({"date":{"$gt":str(year), "$lt":str(year+1)}}).sort("date",1)
    data=[]
    for gdpItem in cursor:
        data.append(gdpItem)
    length = len(data)
    if length==0:
        return
    updateOp=[]
    data[0]["curGdp"]=data[0]['gdp']
    myquery = {"date": data[0]['date'] }
    op = UpdateOne(myquery, {'$set': data[0]}, upsert=True)
    updateOp.append(op)
    if length==1:
        col.bulk_write(updateOp)
        return 
    for i in range(1,length):
        data[i]["curGdp"]=data[i]['gdp']-data[i-1]['gdp']
        myquery = {"date": data[i]['date'] }
        op = UpdateOne(myquery, {'$set': data[i]}, upsert=True)
        updateOp.append(op)

    col.bulk_write(updateOp)
    print(updateOp)
for year in range(2022,2023):
    getQuarterValue(year)
