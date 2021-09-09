import baostock as bs
import pandas as pd

#### 登陆系统 ####
lg = bs.login()


rs_list = []
rs_dividend_2015 = bs.query_dividend_data(code="sh.600000", year="2015", yearType="report")
while (rs_dividend_2015.error_code == '0') & rs_dividend_2015.next():
    rs_list.append(rs_dividend_2015.get_row_data())

result_dividend = pd.DataFrame(rs_list, columns=rs_dividend_2015.fields)
# 打印输出
print(result_dividend)


#### 登出系统 ####
bs.logout()
