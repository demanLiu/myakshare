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


startDate = '2019-06-01'
endDate = '2019-06-20'
# threadNum=1
# executor = ThreadPoolExecutor(max_workers=threadNum) 


def  realDo(updateOp,codeItem):
    bulkRes = historyCol.bulk_write(updateOp)
    codeColQuery= { "code": codeItem["code"],"region": codeItem["region"] }
    codeColUpdateTime = { "$set": { "historyUpdateDate": time.strptime(startDate, '%Y-%m-%d').tm_year,"historylastUpdatedTime": codeItem["historyUpdateDate"]  if "historyUpdateDate" in codeItem else "" } }
    codeCol.update_one(codeColQuery,codeColUpdateTime)

def writeToMonogo(codeList):
    bs.login()
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
            if len(updateOp)>0:
                bulkRes = historyCol.bulk_write(updateOp)
                codeColQuery= { "code": codeItem["code"],"region": codeItem["region"] }
                codeColUpdateTime = { "$set": { "historyUpdateDate": time.strptime(startDate, '%Y-%m-%d').tm_year,"historylastUpdatedTime": codeItem["historyUpdateDate"]  if "historyUpdateDate" in codeItem else "" } }
                codeCol.update_one(codeColQuery,codeColUpdateTime)
        except Exception as err:
            print(codeItem["code"]+": error ") 
            traceback.print_exc()
            pass
    bs.logout()




codeList = list(codeCol.find({},{"_id":0,"code":1,"region":1,"historyUpdateDate":1}))
# group having
# match_dict = {"$match": {"count": {"$lt":170}}}
# group_dict = {"$group":{"_id":"$code","count":{"$sum":1}}}
# result = historyCol.aggregate([group_dict,match_dict])
# codeList=[]
# for i in result:
#     codeList.append(codeCol.find_one({"code":["_id"]},{"_id":0,"code":1,"region":1,"historyUpdateDate":1}))


# doneList = list(historyCol.find({"date":"2019-07-10"},{"_id":0,"code":1}))
# for delItem in doneList:
#     for temp in codeList:
#         if(temp["code"]==delItem["code"]):
#             codeList.remove(temp)
writeToMonogo(codeList)


# splitNum=int(len(codeList)/threadNum)
# taskList =  [codeList[i:i+splitNum] for i in range(0,len(codeList),splitNum)]
# executor = ThreadPoolExecutor(max_workers=threadNum)
# with ThreadPoolExecutor(max_workers=threadNum) as executor:
#     tasks = [executor.submit(writeToMonogo, eachList) for eachList in taskList]
#     for future in as_completed(tasks):
#         print("in main: get page {}s success".format(future.done()))
#     print("all_cone")

