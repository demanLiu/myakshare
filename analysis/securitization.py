import sys
sys.path.append(".")
import utils.mongodbUtil as mongodbUtil

gdpCol = mongodbUtil.getCol("stock","gdp")
dailyCol = mongodbUtil.getCol("stock","daily_deal")

gdpCursor = gdpCol.find({"date":{"$gt":"2019-03-01"}}).sort("date",1)
gdpDict={}
for gdpData in gdpCursor:
    gdpDict[gdpData["date"]]=gdpData


pipeline = [
    {'$match': {"type": {"$in": ["sh", "sz"]}}},
    {'$group': {'_id': "$date", 'market_amount': {'$sum': "$market_amount"},'deal_amount': {'$sum': "$deal_amount"}}},
    {"$sort": {"_id": 1}}
]
dailyCursor = dailyCol.aggregate(pipeline)
for dailyData in dailyCursor:
    print(dailyData)

import pyecharts.options as opts
from pyecharts.charts import Line

# 写入数据
years = ["1995", "1996", "1997", "1998", "1999", "2000",
         "2001", "2002", "2003", "2004", "2005", "2006",
         "2007", "2008", "2009"]
Postage = [0.32, 0.32, 0.32, 0.32, 0.33, 0.33, 0.34,
           0.37, 0.37, 0.37, 0.37, 0.39, 0.41, 0.42, 0.44]

(
    Line()
    # 全局配置
    .set_global_opts()
    # x轴
    .add_xaxis(xaxis_data=years)
    # y轴
    .add_yaxis(
        series_name="",         # 数据所属集合的名称
        y_axis=Postage,            # y轴数据
    )
    # 输出为HTML文件
    # .render_notebook()
)