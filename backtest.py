import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# df_bt = pd.read_csv('./Soccer_No_T.csv')
df_bt = pd.read_pickle('./Soccer_backtest_update.pkl')

#
# df_bt = df_bt[df_bt.pool != 'CyanPool1']
# df_bt = df_bt[df_bt.pool != 'FancyPool']
# df_bt = df_bt[df_bt.pool != 'DAtradertest']
# df_bt = df_bt[df_bt.pool != 'CitronPool1']
# df_bt.to_pickle('./Soccer_all.pkl')

df_bt = df_bt.assign(date=pd.to_datetime(pd.to_datetime(df_bt['settled_date']).dt.date))
# df_bt = df_bt.assign(diff_pnl_suggest=df_bt.mb_pnl * -(df_bt.Risk_suggestion))
# print(df_bt.info())
# df_bt.diff_pnl_suggest = pd.to_numeric(df_bt.diff_pnl_suggest, errors='coerce')

df_bt = df_bt.groupby(['date', 'sport_name']).agg({'bf_pnl': np.sum,'mb_pnl': np.sum, 'diff_pnl': np.sum, 'diff_pnl_suggest': np.sum}).reset_index()

# df_bt = df_bt.groupby(['date', 'sport_name']).agg({'bf_pnl': np.sum,'mb_pnl': np.sum, 'diff_pnl': np.sum}).reset_index()
print(df_bt.info())

df_T2 = df_bt[df_bt.No_T == 'T2']
df_T3 = df_bt[df_bt.No_T == 'T3']
df_T4 = df_bt[df_bt.No_T == 'T4']
print(df_T4)
AquaPool1 = df_T3[df_T3.pool == 'AquaPool1']
Bigpool1 = df_T3[df_T3.pool == 'Bigpool1']
BronzePool1 = df_T3[df_T3.pool == 'BronzePool1']
GreenPool1 = df_T3[df_T3.pool == 'GreenPool1']
GreenPool2 = df_T3[df_T3.pool == 'GreenPool2']
Bigpool1 = df_T2[df_T2.pool == 'Bigpool1']
BluePool1 = df_T2[df_T2.pool == 'BluePool1']
BronzePool1 = df_T2[df_T2.pool == 'BronzePool1']
GreenPool1 = df_T2[df_T2.pool == 'GreenPool1']

AquaPool1 = df_T4[df_T4.pool == 'AquaPool1']
AquaPool3 = df_T4[df_T4.pool == 'AquaPool3']
Bigpool1 = df_T4[df_T4.pool == 'Bigpool1']
BluePool1 = df_T4[df_T4.pool == 'BluePool1']
BronzePool1 = df_T4[df_T4.pool == 'BronzePool1']
GreenPool1 = df_T4[df_T4.pool == 'GreenPool1']
GreenPool2 = df_T4[df_T4.pool == 'GreenPool2']
NavyPool1 = df_T4[df_T4.pool == 'NavyPool1']
UNMATCH = df_T4[df_T4.pool == 'UNMATCH']


fig = plt.figure(figsize=(20,10))
ax1 = fig.add_subplot(111)
ax1.plot(df_bt['date'], df_bt['diff_pnl'], c='b', marker=".", label='diff_pnl')
ax1.plot(df_bt['date'], df_bt['diff_pnl_suggest'], c='purple', marker=".", label='diff_pnl_suggest')

ax1.plot(Bigpool1['date'], Bigpool1['diff_pnl'], c='b', marker=".", label='Bigpool1')
ax1.plot(BronzePool1['date'], BronzePool1['diff_pnl'], c='r', marker=".", label='BronzePool1')
ax1.plot(GreenPool1['date'], GreenPool1['diff_pnl'], c='g', marker=".", label='GreenPool1')
ax1.plot(GreenPool2['date'], GreenPool2['diff_pnl'], c='purple', marker=".", label='GreenPool2')
ax1.plot(AquaPool1['date'], AquaPool1['diff_pnl'], c='brown', marker=".", label='AquaPool1')


ax1.plot(AquaPool1['date'], AquaPool1['diff_pnl'], c='purple', marker=".", label='AquaPool1')
ax1.plot(AquaPool3['date'], AquaPool3['diff_pnl'], c='b', marker=".", label='AquaPool3')
ax1.plot(Bigpool1['date'], Bigpool1['diff_pnl'], c='r', marker=".", label='Bigpool1')
ax1.plot(BluePool1['date'], BluePool1['diff_pnl'], c='g', marker=".", label='BluePool1')
ax1.plot(BronzePool1['date'], BronzePool1['diff_pnl'], c='brown', marker=".", label='BronzePool1')

ax1.plot(GreenPool1['date'], GreenPool1['diff_pnl'], c='orange', marker=".", label='GreenPool1')
ax1.plot(GreenPool2['date'], GreenPool2['diff_pnl'], c='lightcoral', marker=".", label='GreenPool2')
ax1.plot(NavyPool1['date'], NavyPool1['diff_pnl'], c='steelblue', marker=".", label='NavyPool1')
ax1.plot(UNMATCH['date'], UNMATCH['diff_pnl'], c='k', marker=".", label='UNMATCH')
plt.legend(loc='upper left')
plt.title('BackTest Soccer Total on T4')
plt.xlabel("Date")
plt.ylabel("Diff PnL (Euro)")
# plt.axhline(0, color='black', alpha=0.4)
plt.grid(True)
plt.show()






# df_bt = pd.read_csv('./backtest_horseracing.csv')
# df_np = pd.read_csv('./newpool.csv')
#
# df_new = pd.merge(df_bt, df_np[['subaccname', 'suggestionpool']], on='subaccname', how='left')
#
# df_new.ix[df_new.current_pool == 'FancyPool', 'suggestionpool'] = 'FancyPool'
#
# df_new.ix[df_new.suggestionpool.isnull(),'suggestionpool'] = df_new.current_pool
#
# AuqaPool1 = [0,	0, 0]
# AuqaPool3 = [10, 20, 30]
# Navypool1 = [0,	-5, -10]
# BluePool1 = [0,	-5, -10]
# GreenPool1 = [5, 15, 25]
# Unmatched = [0, 0, 0]
#
# # df_new = df_new.assign(risk1=df_new['suggestionpool'])
# df_new = df_new.assign(risk1 = 0)
# df_new.ix[df_new.suggestionpool == 'AuqaPool1', 'risk1'] = AuqaPool1[0]
# df_new.ix[df_new.suggestionpool == 'AuqaPool3', 'risk1'] = AuqaPool3[0]
# df_new.ix[df_new.suggestionpool == 'Navypool1', 'risk1'] = Navypool1[0]
# df_new.ix[df_new.suggestionpool == 'BluePool1', 'risk1'] = BluePool1[0]
# df_new.ix[df_new.suggestionpool == 'GreenPool1', 'risk1'] = GreenPool1[0]
# df_new.ix[df_new.suggestionpool == 'Unmatched', 'risk1'] = Unmatched[0]
#
# df_new.risk1 = df_new.current_risk_total - df_new.sub_risk + df_new.risk1
# df_new.ix[df_new.suggestionpool == 'FancyPool', 'risk1'] = df_new.current_risk_total
#
# df_new = df_new.assign(diff_pnl1 = (df_new.mb_pnl * -(df_new.risk1) / 100))
#
# df_new = df_new.assign(risk2 = 0)
# df_new.ix[df_new.suggestionpool == 'AuqaPool1', 'risk2'] = AuqaPool1[1]
# df_new.ix[df_new.suggestionpool == 'AuqaPool3', 'risk2'] = AuqaPool3[1]
# df_new.ix[df_new.suggestionpool == 'Navypool1', 'risk2'] = Navypool1[1]
# df_new.ix[df_new.suggestionpool == 'BluePool1', 'risk2'] = BluePool1[1]
# df_new.ix[df_new.suggestionpool == 'GreenPool1', 'risk2'] = GreenPool1[1]
# df_new.ix[df_new.suggestionpool == 'Unmatched', 'risk2'] = Unmatched[1]
#
# df_new.risk2 = df_new.current_risk_total - df_new.sub_risk + df_new.risk2
# df_new.ix[df_new.suggestionpool == 'FancyPool', 'risk2'] = df_new.current_risk_total
#
# df_new = df_new.assign(diff_pnl2 = (df_new.mb_pnl * -(df_new.risk2) / 100))
#
# df_new = df_new.assign(risk3 = 0)
# df_new.ix[df_new.suggestionpool == 'AuqaPool1', 'risk3'] = AuqaPool1[2]
# df_new.ix[df_new.suggestionpool == 'AuqaPool3', 'risk3'] = AuqaPool3[2]
# df_new.ix[df_new.suggestionpool == 'Navypool1', 'risk3'] = Navypool1[2]
# df_new.ix[df_new.suggestionpool == 'BluePool1', 'risk3'] = BluePool1[2]
# df_new.ix[df_new.suggestionpool == 'GreenPool1', 'risk3'] = GreenPool1[2]
# df_new.ix[df_new.suggestionpool == 'Unmatched', 'risk3'] = Unmatched[2]
#
# df_new.risk3 = df_new.current_risk_total - df_new.sub_risk + df_new.risk3
# df_new.ix[df_new.suggestionpool == 'FancyPool', 'risk3'] = df_new.current_risk_total
#
# df_new = df_new.assign(diff_pnl3 = (df_new.mb_pnl * -(df_new.risk3) / 100))
#
# print(df_new)
# df_new.to_csv('./backtest_newrisk.csv',index=False)