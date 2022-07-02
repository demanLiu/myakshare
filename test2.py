from cProfile import label
import akshare as ak
import matplotlib.pyplot as plt

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
