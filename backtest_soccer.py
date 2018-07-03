import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df_bt = pd.read_csv('./Soccer_20180614_20_No_T_summary.csv')

# df_bt = df_bt.groupby(['sport_name', 'No_T','date']).agg({'mb_pnl': np.sum, 'diff_pnl': np.sum}).reset_index()
df_bt = df_bt[df_bt.No_T == 'T4']
df_bt = df_bt.groupby(['date', 'pool']).agg({'mb_pnl': np.sum, 'diff_pnl': np.sum}).reset_index()
# print(df_bt)

d = dict(tuple(df_bt.groupby('pool')))
print(df_bt['pool'].unique().tolist())
#
# print(d)

# df_T2 = df_bt[df_bt.No_T == 'T2']
# df_T4 = df_bt[df_bt.No_T == 'T4']
# print(df_T2)
# print(df_T4)
#
fig = plt.figure(figsize=(20,10))
ax1 = fig.add_subplot(111)

# #T2
# y = [d['AquaPool1']['diff_pnl'], d['AquaPool3']['diff_pnl'], d['Bigpool1']['diff_pnl'], d['BluePool1']['diff_pnl'], d['BronzePool1']['diff_pnl'],
#      d['CyanPool1']['diff_pnl'], d['FancyPool']['diff_pnl'], d['GreenPool1']['diff_pnl'], d['GreenPool2']['diff_pnl'], d['NavyPool1']['diff_pnl'], d['UNMATCH']['diff_pnl']]
# x = [d['AquaPool1']['date'], d['AquaPool3']['date'], d['Bigpool1']['date'], d['BluePool1']['date'], d['BronzePool1']['date'],
#      d['CyanPool1']['date'], d['FancyPool']['date'], d['GreenPool1']['date'], d['GreenPool2']['date'], d['NavyPool1']['date'], d['UNMATCH']['date']]
#
# color = ['b', 'g', 'r', 'c', 'm', 'y', 'blueviolet', 'deeppink', 'goldenrod', 'orange', 'skyblue']
# label = ['AquaPool1', 'AquaPool3', 'Bigpool1', 'BluePool1', 'BronzePool1', 'CyanPool1', 'FancyPool', 'GreenPool1', 'GreenPool2', 'NavyPool1', 'UNMATCH']

#T4
y = [d['AquaPool1']['diff_pnl'], d['AquaPool3']['diff_pnl'], d['Bigpool1']['diff_pnl'], d['BluePool1']['diff_pnl'], d['BronzePool1']['diff_pnl'],
     d['GrayPool1']['diff_pnl'], d['GreenPool1']['diff_pnl'], d['GreenPool2']['diff_pnl'], d['NavyPool1']['diff_pnl'], d['UNMATCH']['diff_pnl']]
x = [d['AquaPool1']['date'], d['AquaPool3']['date'], d['Bigpool1']['date'], d['BluePool1']['date'], d['BronzePool1']['date'],
     d['GrayPool1']['date'], d['GreenPool1']['date'], d['GreenPool2']['date'], d['NavyPool1']['date'], d['UNMATCH']['date']]

color = ['b', 'g', 'r', 'c', 'm', 'y', 'blueviolet', 'deeppink', 'goldenrod', 'orange']
label = ['AquaPool1', 'AquaPool3', 'Bigpool1', 'BluePool1', 'BronzePool1', 'GrayPool1', 'GreenPool1', 'GreenPool2', 'NavyPool1', 'UNMATCH']


for ix, iy, icolor, ilabel in zip(x, y, color, label):
    ax1.plot(ix, iy, c=icolor, marker=".", label=ilabel)


ax1.plot(df_T2['date'], df_T2['diff_pnl'], c='b', marker=".", label='T2')
ax1.plot(df_T4['date'], df_T4['diff_pnl'], c='r', marker=".", label='T4')
ax1.plot(df_bt['date'], df_bt['diff_pnl2'], c='r', marker=".", label='diff_pnl2')
ax1.plot(df_bt['date'], df_bt['diff_pnl3'], c='g', marker=".", label='diff_pnl3')
plt.legend(loc='lower right')
plt.title('Soccer Diff PnL for Pool on T4')
plt.xlabel("Date")
plt.ylabel("Diff PnL")
# plt.axhline(0, color='black', alpha=0.6)
plt.grid(True)
plt.show()






# df_bt = pd.read_csv('./backtest_soccer.csv')
# df_rs = pd.read_excel('risk_setting soccer 19.06.2018.xlsx')
# df_bt.sub_risk = df_bt.sub_risk / 100
# df_bt.current_risk_total = df_bt.current_risk_total / 100
# df_bt = df_bt.assign(real_risk=1-(df_bt.bf_pnl / df_bt.mb_pnl))
#
# df_bt = df_bt[df_bt.current_risk_total == df_bt.real_risk]
#
# df_new = pd.merge(df_bt, df_rs[['account_id', 'recommand risk']], on='account_id', how='left')
#
# df_new['recommand risk'] = df_new.current_risk_total - df_new.sub_risk + df_new['recommand risk']
#
# df_new = df_new.assign(diff_pnl_recommand = (df_new.mb_pnl * -df_new['recommand risk']))
#
# print(df_new.head(100))
#
# df_new.to_csv('./backtest_soccer_newrisk.csv',index=False)
