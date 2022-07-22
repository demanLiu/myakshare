from signal import valid_signals
import akshare as ak
import sys
sys.path.append(".")
from data.get_stock_by_code import writeToMonogo
from pymongo import UpdateOne
import utils.mongodbUtil as mongodbUtil
etfCol =  mongodbUtil.getCol("stock","etf")

code = "515180"
date="2021"
fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol=code, date=date)
print(fund_portfolio_hold_em_df)
updateOp=[]
for index,data in fund_portfolio_hold_em_df.iterrows():
    if data["季度"].startswith(date+"年1") :
        myquery = {"etf_code": code, "date":date+"-1","child_code":data["股票代码"] }
        valueItem={}
        valueItem["date"] = date+"-1"
        valueItem["etf_code"]=code
        valueItem["child_code"] = data["股票代码"]
        valueItem["share"] = data["占净值比例"]
        valueItem["startDate"] = date+"-01-01"
        valueItem["endDate"] = date+"-03-31"
        op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
        updateOp.append(op)
        # writeToMonogo(data['股票代码'])
    if data["季度"].startswith(date+"年2") :
        myquery = {"etf_code": code, "date":date+"-2","child_code":data["股票代码"] }
        valueItem={}
        valueItem["date"] = date+"-2"
        valueItem["etf_code"]=code
        valueItem["child_code"] = data["股票代码"]
        valueItem["share"] = data["占净值比例"]
        valueItem["startDate"] = date+"-04-01"
        valueItem["endDate"] = date+"-06-30"
        op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
        updateOp.append(op)
        # writeToMonogo(data['股票代码'])
    if data["季度"].startswith(date+"年3") :
        myquery = {"etf_code": code, "date":date+"-3","child_code":data["股票代码"] }
        valueItem={}
        valueItem["date"] = date+"-3"
        valueItem["etf_code"]=code
        valueItem["child_code"] = data["股票代码"]
        valueItem["share"] = data["占净值比例"]
        valueItem["startDate"] = date+"-07-01"
        valueItem["endDate"] = date+"-09-30"
        op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
        updateOp.append(op)
        # writeToMonogo(data['股票代码'])
    if data["季度"].startswith(date+"年4") :
        myquery = {"etf_code": code, "date":date+"-4","child_code":data["股票代码"] }
        valueItem={}
        valueItem["date"] = date+"-4"
        valueItem["etf_code"]=code
        valueItem["child_code"] = data["股票代码"]
        valueItem["share"] = data["占净值比例"]
        valueItem["startDate"] = date+"-10-01"
        valueItem["endDate"] = date+"-12-31"
        op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
        updateOp.append(op)
        # writeToMonogo(data['股票代码'])

etfCol.bulk_write(updateOp)

# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="600327", period="daily", start_date="20170301", end_date='20220706', adjust="")
# print(stock_zh_a_hist_df)
# writeToMonogo("600327")



