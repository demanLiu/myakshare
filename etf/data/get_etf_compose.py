import akshare as ak
import sys
sys.path.append(".")
from data.get_stock_by_code import writeToMonogo

code = "515180"
date="2021"
fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol=code, date=date)
print(fund_portfolio_hold_em_df)
for index,data in fund_portfolio_hold_em_df.iterrows():
    if data["季度"].startswith(date+"年1") :
        writeToMonogo(data['股票代码'])


# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="600327", period="daily", start_date="20170301", end_date='20220706', adjust="")
# print(stock_zh_a_hist_df)
# writeToMonogo("600327")



