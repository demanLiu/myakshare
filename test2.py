from cProfile import label
import imp
import time
import akshare as ak
import matplotlib.pyplot as plt
from scipy import interpolate
import numpy as np
from scipy.misc import derivative

stock_sse_summary_df = ak.stock_sse_summary()
print(stock_sse_summary_df)


stock_sse_deal_daily_df = ak.stock_sse_deal_daily(date="20210903")
print(stock_sse_deal_daily_df)
stock_szse_summary_df = ak.stock_szse_summary(date="20210903")
print(stock_szse_summary_df)


print(stock_sse_deal_daily_df.loc[0]["股票"])


stock_zh_a_hist_df = ak.stock_zh_a_hist(
    symbol="000001", start_date="20210901", end_date='20210916')
stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000001")
print(stock_zh_index_daily_df)

plt.figure(figsize=(8000, 50))
plt.plot(stock_zh_a_hist_df["日期"],
         stock_zh_a_hist_df["收盘"], marker='.', label="test")

plt.legend(bbox_to_anchor=(1.05, 0), loc=3)
plt.gca().set_xticks(stock_zh_a_hist_df["日期"])
for a, b in zip(stock_zh_a_hist_df["日期"], stock_zh_a_hist_df["收盘"]):
    plt.text(a, b, b, ha='center', va='bottom', fontsize=20)
plt.show()




## 拟合
x=stock_zh_a_hist_df["日期"].map(lambda x:int( time.mktime(time.strptime(x, "%Y-%m-%d"))))
y=stock_zh_a_hist_df["收盘"]

plt.plot(x, y, "o")


# for s in (0, 1e-4):
#     tck, t = interpolate.splprep([x.values, y.values], s=s)  #❶
#     xi, yi = interpolate.splev(np.linspace(t[0], t[-1], 200), tck)  #❷
#     plt.plot(xi, yi, lw=2, label=u"s=%g" % s)

xnew = np.linspace(1630425600,1631721600 , 101)
for kind in ['nearest', 'zero','linear','quadratic',5]:
    #根据kind创建插值对象interp1d
    f = interpolate.interp1d(x, y, kind = kind)
    ynew = f(xnew)#计算插值结果
    plt.plot(xnew, ynew, label = str(kind))

print(derivative(f,1630771200,dx=0.0001,n=1))
plt.legend()
plt.show()