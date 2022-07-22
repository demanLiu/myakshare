
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