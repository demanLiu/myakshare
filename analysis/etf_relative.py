from regex import P
import yaml
import sys
sys.path.append(".")
import xalpha as xa
import pandas as pd
import etf.property_top_category  as ptc


def getFundInfo(members):
    resList=[]
    for item in members:
        resList.append(xa.fundinfo(item['code']))
    return resList


f=open("etf/settings/etf_share.yaml")
y =yaml.safe_load(f)
fundList=[]
chinaStockList = getFundInfo(y["compose"][ptc.PropertyTopCategory.ChinaStock.name]['members'])
newMarketList = getFundInfo(y["compose"][ptc.PropertyTopCategory.OverSeaNewMarket.name]['members'])
goldList = getFundInfo(y["compose"][ptc.PropertyTopCategory.GOLD.name]['members'])
matureList = getFundInfo(y["compose"][ptc.PropertyTopCategory.OverSeaMature.name]['members'])
fundList.extend(chinaStockList)
fundList.extend(newMarketList)
fundList.extend(goldList)
fundList.extend(matureList)
print(fundList)
fundTuple = tuple(fundList)
comparison = xa.evaluate(*fundTuple,start='20220501')
table = comparison.correlation_table()
resultDict = {}
for colCode, dict in table.iterrows():
    resultDict[colCode]={}
    resultDict[colCode]["strong"]=[]
    for  rowCode  in dict.keys():
        if dict[rowCode]>0.7 and rowCode != colCode:
            resultDict[colCode]['strong'].append(rowCode)
    print(colCode+':' )
    print(resultDict[colCode]['strong'])
