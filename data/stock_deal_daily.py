
import sys
sys.path.append(".")
from datetime import datetime, timedelta, date
import time
import calendar
from pymongo import UpdateOne
import akshare as ak
import utils.mongodbUtil as mongodbUtil

start =  "20220722"
end1 = "20211224"
end = date.today().strftime("%Y%m%d")
col = mongodbUtil.getCol("stock", "daily_deal")


def sh_deal_daily(start, end1, col):
    startDate = datetime.strptime(start, "%Y%m%d")
    updateOp = []
    for i in range(10000):
        try:
            date = startDate + timedelta(days=i)
            if date.strftime("%Y%m%d") > end:
                break
            year = date.year
            month = date.month
            day = date.day
            currentday = calendar.weekday(year, month, day)
            if currentday >= 5:
                continue
            stock_sse_deal_daily_df = ak.stock_sse_deal_daily(
                date=date.strftime("%Y%m%d"))
            valueItem = {}
            valueItem["date"] = date.strftime("%Y%m%d")
            myquery = {"date": valueItem["date"], "type": "sh"}
            if date.strftime("%Y%m%d") > end1:
                valueItem = {}
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
            time.sleep(2)
        except Exception as err:
            print("err:"+date.strftime("%Y%m%d"))
            print(err)
            if len(updateOp) > 0:
                col.bulk_write(updateOp)
                updateOp = []
            pass
    if len(updateOp) > 0:
        col.bulk_write(updateOp)


def sz_deal_daily(start,  col):
    startDate = datetime.strptime(start, "%Y%m%d")
    updateOp = []
    for i in range(10000):
        try:
            date = startDate + timedelta(days=i)
            if date.strftime("%Y%m%d") > end:
                break
            year = date.year
            month = date.month
            day = date.day
            currentday = calendar.weekday(year, month, day)
            if currentday >= 5:
                continue
            stock_sse_deal_daily_df = ak.stock_szse_summary(
                date=date.strftime("%Y%m%d"))
            valueItem = {}
            valueItem["date"] = date.strftime("%Y%m%d")
            myquery = {"date": valueItem["date"], "type": "sz"}
            valueItem["market_amount"] = stock_sse_deal_daily_df.iat[0, 3]/100000000
            valueItem["deal_amount"] = stock_sse_deal_daily_df.iat[0, 2]/100000000
            op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
            updateOp.append(op)
            valueItem = {}
            valueItem["date"] = date.strftime("%Y%m%d")
            myquery = {"date": valueItem["date"], "type": "secondSZ"}
            valueItem["market_amount"] = stock_sse_deal_daily_df.iat[3, 3]/100000000
            valueItem["deal_amount"] = stock_sse_deal_daily_df.iat[3, 2]/100000000
            op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
            updateOp.append(op)
            time.sleep(2)
        except Exception as err:
            print("err:"+date.strftime("%Y%m%d"))
            print(err)
            if len(updateOp) > 0:
                col.bulk_write(updateOp)
                updateOp = []
            pass
    if len(updateOp) > 0:
        col.bulk_write(updateOp)

# 2022-07-22
#20200414  20220217 上证没数据
sh_deal_daily(start, end1, col)
sz_deal_daily(start,  col)
