import akshare as ak
from tomlkit import date
code = "515180"
date="2021"
fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol=code, date=date)

for index,data in fund_portfolio_hold_em_df.iterrows():
    if data["季度"].startswith(date+"年1") :
        print(data['股票名称'])

