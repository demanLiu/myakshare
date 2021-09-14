import baostock as bs
import pandas as pd
import pymongo
import datetime
from pymongo import UpdateOne
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
codeCol = db["stockcode"]
bonusCol = db["bonus"]
#### 登陆系统 ####
lg = bs.login()
# print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

startYear = 2000
endYear = datetime.datetime.now().year
colRes = codeCol.find({"code":"600000"},{"_id":0,"code":1,"region":1,"bonusUpdateDate":1})
for codeItem in colRes:
    rs_list = []
    # startYear = codeItem["bonusUpdateDate"] -1  if  "bonusUpdateDate" in codeItem else startYear
    for  year in range( startYear ,endYear+1):
        rs_dividend = bs.query_dividend_data(code=codeItem["region"]+"."+codeItem["code"], year=year, yearType="report")
        while (rs_dividend.error_code == '0') & rs_dividend.next():
            rs_list.append(rs_dividend.get_row_data())
    result = pd.DataFrame(rs_list, columns=rs_dividend.fields)
    updateOp=[]
    for index, row in result.iterrows():
        myquery = {"code": codeItem["code"],"region": codeItem["region"], "payDate": row["dividPayDate"]}
        valueItem={}
        valueItem["payDate"] = row["dividPayDate"]
        valueItem["cashBeforeTax"] = row["dividCashPsBeforeTax"]
        valueItem["giveStock"] = row["dividStocksPs"]
        valueItem["convertStock"] = row["dividReserveToStockPs"]
        op = UpdateOne(myquery, {'$set': valueItem}, upsert=True)
        updateOp.append(op)
    bulkRes = bonusCol.bulk_write(updateOp)
    codeColQuery= { "code": codeItem["code"],"region": codeItem["region"] }
    codeColUpdateTime = { "$set": { "bonusUpdateDate": endYear,"bonuslastUpdatedTime": codeItem["bonusUpdateDate"]  if "bonusUpdateDate" in codeItem else "" } }
    codeCol.update_one(codeColQuery,codeColUpdateTime)

    
# 打印输出


#### 登出系统 ####
bs.logout()
