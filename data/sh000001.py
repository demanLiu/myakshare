
import akshare as ak
import pymongo
import time
from pymongo import UpdateOne
result = ak.stock_zh_index_daily(symbol="sh000001")
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
historyCol = db["history"]
codeItem={}
codeItem["code"]="000001"
codeItem["region"]="sh"
updateOp=[]
for index, row in result.iterrows():
    myquery = {"code": codeItem["code"],"region": codeItem["region"], "date": str(index).split()[0]}
    valueItem={}
    for columnItem in result.columns:
        valueItem[columnItem] = row[columnItem]
    valueItem["code"]=codeItem["code"]
    op = UpdateOne(myquery, {'$set': valueItem}, upsert=True)
    updateOp.append(op)
if len(updateOp)>0:
    bulkRes = historyCol.bulk_write(updateOp)