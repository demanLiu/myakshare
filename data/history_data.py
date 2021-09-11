import akshare as ak
import baostock as bs
import pymongo
import pandas as pd
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor,ALL_COMPLETED,as_completed
import time
import traceback


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
codeCol = db["stockcode"]
historyCol = db["history"]

startDate = '2021-09-01'
endDate = '2021-09-10'
threadNum=1

def writeToMonogo(codeList):
    lg = bs.login()
    for codeItem in codeList:
        try:
            rs = bs.query_history_k_data_plus(codeItem["region"]+"."+codeItem["code"],
                                            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                            start_date=startDate, end_date=endDate,
                                            frequency="d", adjustflag="3")
            if(rs.error_code !='0'):
                print(codeItem["code"]+": error "+ rs.error_msg) 
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
                op = UpdateOne(myquery, {'$set': valueItem}, upsert=True)
                updateOp.append(op)
            bulkRes = historyCol.bulk_write(updateOp)
            codeColQuery= { "code": codeItem["code"],"region": codeItem["region"] }
            codeColUpdateTime = { "$set": { "historyUpdateDate": time.strptime(startDate, '%Y-%m-%d').tm_year,"historylastUpdatedTime": codeItem["historyUpdateDate"]  if "historyUpdateDate" in codeItem else "" } }
            codeCol.update_one(codeColQuery,codeColUpdateTime)
        except Exception as err:
            print(codeItem["code"]+": error ") 
            traceback.print_exc()
    bs.logout()

codeList = list(codeCol.find({},{"_id":0,"code":1,"region":1,"historyUpdateDate":1}))
splitNum=int(len(codeList)/threadNum)
taskList =  [codeList[i:i+splitNum] for i in range(0,len(codeList),splitNum)]
executor = ThreadPoolExecutor(max_workers=threadNum)
with ThreadPoolExecutor(max_workers=threadNum) as executor:
    tasks = [executor.submit(writeToMonogo, eachList) for eachList in taskList]
    for future in as_completed(tasks):
        print("in main: get page {}s success".format(future.done()))
    print("all_cone")



