import akshare as ak
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["stock"]
mycol = mydb["stockcode"]
# stock_info_a_code_name_df = ak.stock_info_a_code_name()
stock_info_sh_df = ak.stock_info_sh_name_code(indicator="主板A股")

for code ,name in zip(stock_info_sh_df["COMPANY_CODE"],stock_info_sh_df["COMPANY_ABBR"]):
    myquery = { "code": code }
    newvalues = { "$setOnInsert": { "name": name ,"region":"sh"} }
    mycol.update_one(myquery,newvalues,upsert=True)

stock_info_sz_name_code_df = ak.stock_info_sz_name_code(indicator="A股列表")

for code ,name in zip(stock_info_sz_name_code_df["A股代码"],stock_info_sz_name_code_df["A股简称"]):
    myquery = { "code": code }
    newvalues = { "$setOnInsert": { "name": name ,"region":"sz"} }
    mycol.update_one(myquery,newvalues,upsert=True)