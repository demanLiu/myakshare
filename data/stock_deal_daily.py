
import sys

from black import err
sys.path.append(".")
import utils.mongodbUtil as mongodbUtil
from datetime import datetime, timedelta
import akshare as ak
from pymongo import UpdateOne
import calendar
start = "20190826"
end1 = "20211224"

col = mongodbUtil.getCol("stock", "daily_deal")

start='20191209'

def sh_deal_daily(start, end1, col):
    startDate = datetime.strptime(start, "%Y%m%d")
    updateOp = []
    for i in range(10000):
        try:
            date = startDate + timedelta(days=i)
            year= date.year
            month = date.month
            day = date.day
            currentday =calendar.weekday(year,month,day)
            stock_sse_deal_daily_df = ak.stock_sse_deal_daily(date=date.strftime("%Y%m%d"))
            valueItem = {}
            valueItem["date"] = date.strftime("%Y%m%d")
            myquery = { "date": valueItem["date"],"type":"sh"}
            if date.strftime("%Y%m%d") > end1:
                valueItem={}
                valueItem["market_amount"] = stock_sse_deal_daily_df.iat[0, 1]
                valueItem["deal_amount"] = stock_sse_deal_daily_df.iat[3, 1]
                valueItem["deal_nums"] = stock_sse_deal_daily_df.iat[2, 1]
                valueItem["pe"] = stock_sse_deal_daily_df.iat[1, 1]
                valueItem["exchange"] = stock_sse_deal_daily_df.iat[8, 1]
            else:
                valueItem["market_amount"] = stock_sse_deal_daily_df.at[0, "股票"]
                valueItem["deal_amount"] = stock_sse_deal_daily_df.at[4, "股票"]
                valueItem["deal_nums"] = stock_sse_deal_daily_df.at[3, "股票"]
                valueItem["pe"] = stock_sse_deal_daily_df.at[1, "股票"]
                valueItem["exchange"] = stock_sse_deal_daily_df.at[9, "股票"]
            op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
            updateOp.append(op)
        except Exception as err:
            print("err:"+date.strftime("%Y%m%d"))
            if len(updateOp) > 0:
                col.bulk_write(updateOp)
                updateOp=[]
            pass
    if len(updateOp) > 0:
        col.bulk_write(updateOp)

sh_deal_daily(start, end1, col)