import akshare as ak
import baostock as bs
import pymongo
import pandas as pd
from pymongo import UpdateOne



client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
codeCol = db["stockcode"]
historyCol = db["history"]
lg = bs.login()
#### 登陆系统 ####
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

startDate = '2021-09-09'
endDate = '2021-09-09'



for codeItem in codeCol.find({}):
    rs = bs.query_history_k_data_plus(codeItem["region"]+"."+codeItem["code"],
                                      "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                      start_date=startDate, end_date=endDate,
                                      frequency="d", adjustflag="3")
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    rs1 = bs.query_history_k_data_plus(codeItem["region"]+"."+codeItem["code"],
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
        myquery = {"code": codeItem["code"],"region": codeItem["region"], "date": row["date"]}
        valueItem={}
        for columnItem in result.columns:
            valueItem[columnItem] = row[columnItem]
        valueItem["code"]=codeItem["code"]
        newvalues = {"$setOnInsert": valueItem}
        op = UpdateOne(myquery, {'$set': valueItem}, upsert=True)
        updateOp.append(op)
    bulkRes = historyCol.bulk_write(updateOp)
#### 登出系统 ####
bs.logout()

