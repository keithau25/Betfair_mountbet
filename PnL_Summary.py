import pandas as pd
import os

FX = {'subcurrency': ['CHIPS', 'MOP', 'None', 'NoTrade', 'EUR', 'CTR', 'PNTS', 'BAZZI', 'INR', 'TKN', 'KD','USD'],
         'FX': [1.75, 1.85, 1, 0, 1, 1.8, 1.45, 165, 105, 6.1, 1.65, 0.8]}
df_FX = pd.DataFrame.from_dict(FX)

path = os.path.dirname(os.path.abspath(__file__))
for path, subdirs, files in os.walk(path):
    for file in files:
        if (file.startswith("rep-Event Comparison No Arch-")):
            bflist = file
        elif (file.startswith("rep-MB Betting History-")):
            mblist = file
        elif (file.startswith("rep-Operator Pool-")):
            poollist = file

path = os.path.dirname(os.path.abspath(__file__))
bflist = path + '/' + bflist
mblist = path + '/' + mblist
poollist = path + '/' + poollist

print("In Progress...")

# Merge Excel
df_MB = pd.read_excel(mblist)
df_MB = pd.DataFrame({
        'supermastername': df_MB['supermastername'],
        'mastername': df_MB['mastername'],
        'subaccname': df_MB['subaccname'],
        'account_id': df_MB['account_id'],
        'bet_id': df_MB['bet_id'].astype(str),
        'sport_name': df_MB['sport_name'],
        'settled_date': df_MB['settled_date'].dt.strftime('%Y-%m-%d'),
        'market_name': df_MB['market_name'],
        'profit/loss': df_MB['profit/loss'],
        'subcurrency': df_MB['subcurrency']})

df_BF = pd.read_excel(bflist)
df_BF = pd.DataFrame({
        'bet_id': df_BF['bet_id'].astype(str),
        'bf_pnl': df_BF['profit']})

df_Pool = pd.read_excel(poollist)
df_Pool = pd.DataFrame({
        'subaccname': df_Pool['Username'],
        'pool': df_Pool['Operator']})

df_merge = pd.merge(df_MB, df_Pool, how='left', left_on='subaccname', right_on='subaccname')
df_merge['pool'] = df_merge['pool'].fillna("Unattached")
df_merge = pd.merge(df_merge, df_BF, left_on='bet_id', right_on='bet_id')
df_merge = df_merge[df_merge['profit/loss'] != 0]
df_merge = pd.merge(df_merge, df_FX, how='left', left_on='subcurrency', right_on='subcurrency')
df_merge = df_merge[df_merge['FX'] != 0]

df_merge['pnl_in_euro'] = df_merge['profit/loss'] / df_merge['FX']
df_merge['mb_pnl'] = df_merge['bf_pnl'] - df_merge['pnl_in_euro']

df_merge = df_merge.drop('FX', 1)
df_merge = df_merge.drop('profit/loss', 1)

for i in range(len(df_merge['market_name'])):
    if(df_merge['market_name'][i].endswith("Overs Line") and df_merge['sport_name'][i] == "Cricket"):
        df_merge.at[i, 'sport_name'] = "Cricket Line"

# # Generate Summary Report
# accname = df_merge['subaccname'].unique()
# sportname = df_merge['sport_name'].unique()
# acclist = []
# sportlist = []
# nbbet = []
# nbwinbet = []
# bf_pnl_sum = []
# pnl_in_euro_sum = []
# mb_pnl_sum = []
# for i in range(len(sportname)):
#     for j in range(len(accname)):
#         df_temp1 = df_merge[df_merge['subaccname'] == accname[j]]
#         df_temp1 = df_temp1[df_temp1['sport_name'] == sportname[i]]
#         acclist.append(accname[j])
#         sportlist.append(sportname[i])
#         bf_pnl_sum.append(df_temp1['bf_pnl'].sum())
#         pnl_in_euro_sum.append(df_temp1['pnl_in_euro'].sum())
#         mb_pnl_sum.append(df_temp1['mb_pnl'].sum())
#         nbbet.append(len(df_temp1.index))
#         df_temp2 = df_temp1[df_temp1['mb_pnl'] > 0]
#         nbwinbet.append(len(df_temp2.index))
#
# df_summary = pd.DataFrame({
#         'subaccname': acclist,
#         'sport_name': sportlist,
#         'nbbet': nbbet,
#         'nbwinbet': nbwinbet,
#         'bf_pnl': bf_pnl_sum,
#         'pnl_in_euro': pnl_in_euro_sum,
#         'mb_pnl': mb_pnl_sum})

# Round to 2 decimal
df_merge['bf_pnl'] = df_merge['bf_pnl'].round(2)
df_merge['pnl_in_euro'] = df_merge['pnl_in_euro'].round(2)
df_merge['mb_pnl'] = df_merge['mb_pnl'].round(2)
# df_summary['bf_pnl'] = df_summary['bf_pnl'].round(2)
# df_summary['pnl_in_euro'] = df_summary['pnl_in_euro'].round(2)
# df_summary['mb_pnl'] = df_summary['mb_pnl'].round(2)

# Write excel
writer = pd.ExcelWriter(path + '/summary.xlsx', engine='xlsxwriter')
df_merge.to_excel(writer, sheet_name='Sheet1')
# df_summary.to_excel(writer, sheet_name='Summary')
writer.save()

print("Completed!")
