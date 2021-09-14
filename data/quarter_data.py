import datetime
import baostock as bs
import pandas as pd
import pymongo
from pymongo import UpdateOne
import quarter_data_type
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock"]
codeCol = db["stockcode"]

switchMap={ quarter_data_type.QuarterDataType.Profit.name.lower() : bs.query_profit_data,
                    quarter_data_type.QuarterDataType.Operation.name.lower():bs.query_operation_data,
                    quarter_data_type.QuarterDataType.Growth.name.lower():bs.query_growth_data,
                    quarter_data_type.QuarterDataType.Debate.name.lower():bs.query_balance_data,
                    quarter_data_type.QuarterDataType.Flow.name.lower():bs.query_cash_flow_data,
                    quarter_data_type.QuarterDataType.Dupont.name.lower():bs.query_dupont_data,
                    quarter_data_type.QuarterDataType.Performance.name.lower():bs.query_performance_express_report,
                    };

def  getQuarterData(dataType):
    # 登陆系统pe
    lg = bs.login()
    # 查询季频估值指标盈利能力
    startYear = 2000
    endYear = datetime.datetime.now().year
    targetCol = db[dataType]
    updateField = dataType+"UpdateDate"
    colRes = codeCol.find({"code":"600000"},{"_id":0,"code":1,"region":1,updateField:1})
    for codeItem in colRes:
        search_list = []
        # startYear = codeItem[updateField] -1  if  updateField in codeItem else startYear
        for  year in range( startYear ,endYear+1):
            for quarter in range(1,5):
                search_rs = switchMap.get(dataType)(code=codeItem["region"]+"."+codeItem["code"], year=year, quarter=quarter)
                while (search_rs.error_code == '0') & search_rs.next():
                    search_list.append(search_rs.get_row_data())
        result = pd.DataFrame(search_list, columns=search_rs.fields)
        updateOp=[]
        for index, row in result.iterrows():
            myquery = {"code": codeItem["code"],"region": codeItem["region"], "date": row["statDate"]}
            valueItem={}
            for columnItem in result.columns:
                valueItem[columnItem] = row[columnItem]
            valueItem["code"]=codeItem["code"]
            op = UpdateOne(myquery, {'$set': valueItem}, upsert=True)
            updateOp.append(op)
        bulkRes = targetCol.bulk_write(updateOp)
        codeColQuery= { "code": codeItem["code"],"region": codeItem["region"] }
        codeColUpdateTime = { "$set": { updateField: endYear} }
        codeCol.update_one(codeColQuery,codeColUpdateTime)
        
    # 登出系统
    bs.logout()



getQuarterData("operation")