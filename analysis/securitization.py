import sys
sys.path.append(".")
import utils.mongodbUtil as mongodbUtil
from  datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
gdpCol = mongodbUtil.getCol("stock","gdp")
dailyCol = mongodbUtil.getCol("stock","daily_deal")


def  getGdpDate(year, quarter):
    if quarter == 0:
        return str(year-1)+"-12-01"
    if quarter ==4:
        return str(year)+'-12-01'
    return str(year)+'-0'+ str(quarter*3)+'-01'



gdpCursor = gdpCol.find({"date":{"$gt":"2018-03-01"}}).sort("date",1)
gdpDict={}
for gdpData in gdpCursor:
    gdpDict[gdpData["date"]]=gdpData


pipeline = [
    {'$match': {"type": {"$in": ["sh", "sz"]}}},
    {'$group': {'_id': "$date", "count":{"$sum":1},'market_amount': {'$sum': "$market_amount"},'deal_amount': {'$sum': "$deal_amount"}}},
    {"$sort": {"_id": 1}}
]
date=[]
amountGdpRate=[]
dailyCursor = dailyCol.aggregate(pipeline)
for dailyData in dailyCursor:
    if dailyData['count'] != 2:
        continue
    date.append(dailyData["_id"])
    dt = datetime.strptime(dailyData["_id"], "%Y%m%d") 
    quarter = int(dt.month/3)
    gdpDate = getGdpDate(dt.year,quarter)
    formatDate = datetime.strptime(gdpDate, "%Y-%m-%d")
    formatDate1 = (formatDate + relativedelta(months=-3) ).strftime("%Y-%m-%d") 
    formatDate2 = (formatDate + relativedelta(months=-6) ).strftime("%Y-%m-%d") 
    formatDate3 = (formatDate + relativedelta(months=-9) ).strftime("%Y-%m-%d") 
    gdp = gdpDict[gdpDate]['curGdp'] + gdpDict[formatDate1]['curGdp'] + gdpDict[formatDate2]['curGdp'] + gdpDict[formatDate3]['curGdp']
    amountGdpRate.append(round(dailyData['market_amount']/gdp,3))

import pyecharts.options as opts
from pyecharts.charts import Line

(
    Line()
    # 全局配置
    .set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True),datazoom_opts=[opts.DataZoomOpts(type_="slider")])
    # x轴
    .add_xaxis(xaxis_data=date)
    # y轴
    .add_yaxis(
        series_name="",         # 数据所属集合的名称
        y_axis=amountGdpRate,            # y轴数据
    )
    # 输出为HTML文件
    .render()
)