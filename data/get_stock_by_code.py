import baostock as bs
import pymongo
import pandas as pd
from pymongo import UpdateOne
import time
import traceback

startDate = '2010-01-01'
endDate = '2022-06-30'

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
codeCol = db["stockcode"]
historyCol = db["history"]


def writeToMonogo(code):
    bs.login()
    region="sz"
    if code.startswith("6"):
        region="sh"
    try:
            rs = bs.query_history_k_data_plus(region+"."+code,
                                            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                            start_date=startDate, end_date=endDate,
                                            frequency="d", adjustflag="3")
            if(rs.error_code !='0'):
                print(code+": error "+ rs.error_msg) 
            data_list = []
            while (rs.error_code == '0') & rs.next():
                # 获取一条记录，将记录合并在一起
                data_list.append(rs.get_row_data())
            result = pd.DataFrame(data_list, columns=rs.fields)
            rs1 = bs.query_history_k_data_plus(region+"."+code,
                                            "date,open,high,low,close,preclose",
                                            start_date=startDate, end_date=endDate,
                                            frequency="d", adjustflag="1")
            data_list = []
            while (rs1.error_code == '0') & rs1.next():
                # 获取一条记录，将记录合并在一起
                data_list.append(rs1.get_row_data())
            result1 = pd.DataFrame(data_list, columns=rs1.fields)

            hfqData = [ i for i in ["open","high","low","close","preclose"]  if i in result1.columns]
            for  hfq in hfqData:
                result["hfq"+hfq] = result1[hfq]
            updateOp=[]
            for index, row in result.iterrows():
                myquery = {"code": code,"region": region, "date": row["date"]}
                valueItem={}
                for columnItem in result.columns:
                    valueItem[columnItem] = row[columnItem]
                valueItem["code"]=code
                op = UpdateOne(myquery, {'$setOnInsert': valueItem}, upsert=True)
                updateOp.append(op)
            if len(updateOp)>0:
                bulkRes = historyCol.bulk_write(updateOp)
                codeColQuery= { "code": code,"region": region }
                codeColUpdateTime = { "$set": { "historyUpdateDate": endDate} }
                codeCol.update_one(codeColQuery,codeColUpdateTime)
    except Exception as err:
            print(code+": error ") 
            traceback.print_exc()
            pass
    bs.logout()