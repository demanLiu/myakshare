import akshare as ak
from datetime import date, datetime
import pymongo
from pymongo import UpdateOne


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
col = db["bond"]

bond_zh_us_rate_df = ak.bond_zh_us_rate()


lastDate = "2021-10-15"

filterData = bond_zh_us_rate_df[bond_zh_us_rate_df["日期"]>=datetime.date(datetime.strptime(lastDate, "%Y-%m-%d"))]
updateOp=[]
for index,data in filterData.iterrows():
        myquery = {"日期": data["日期"].strftime("%Y-%m-%d")}
        valueItem={}
        for columnItem in filterData.columns:
            valueItem[columnItem] = data[columnItem]
        valueItem["日期"] = data["日期"].strftime("%Y-%m-%d")
        op = UpdateOne(myquery, {'$set': valueItem}, upsert=True)
        updateOp.append(op)
bulkRes = col.bulk_write(updateOp)
