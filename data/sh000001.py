
import sys
sys.path.append(".")
import utils.mongodbUtil as mongodbUtil
import akshare as ak
from pymongo import UpdateOne

historyCol = mongodbUtil.getCol("stock", "importIndex")
# 


def writeMongo(code, region, data):
    codeItem = {}
    codeItem["code"] = code
    codeItem["region"] = region
    updateOp = []
    for index, row in data.iterrows():
        myquery = {
            "code": codeItem["code"], "region": codeItem["region"], "date":str(row["date"])}
        valueItem = {}
        for columnItem in data.columns:
            valueItem[columnItem] = row[columnItem]
        valueItem["code"] = codeItem["code"]
        valueItem["date"] = str(row["date"])
        op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
        updateOp.append(op)
    if len(updateOp) > 0:
        historyCol.bulk_write(updateOp)


writeMongo("000001","sh",ak.stock_zh_index_daily(symbol="sh000001"))
writeMongo("399001","sz",ak.stock_zh_index_daily(symbol="sz399001"))
writeMongo("399006","sz",ak.stock_zh_index_daily(symbol="sz399006"))
