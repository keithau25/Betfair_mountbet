import os
import sys
import numpy as np
import pandas as pd

try:
    path = os.path.dirname(os.path.abspath(__file__))
    for path, subdirs, files in os.walk(path):
        for file in files:
            if (file.startswith("rep-Risk setting report - daily-")):
                riskfile = file
            elif (file.startswith("rep-Operator Pool-")):
                poolfile = file
            elif (file.startswith("rep-Daily summary (last 24 hour)-")):
                dailysummaryfile = file
            else:
                if('American' in file and 'Football' in file):
                    reportfile = file
                    SPORTNAME = 'American Football'
                elif('Baseball' in file):
                    reportfile = file
                    SPORTNAME = 'Baseball'
                elif ('Basketball' in file):
                    reportfile = file
                    SPORTNAME = 'Basketball'
                elif ('Boxing' in file):
                    reportfile = file
                    SPORTNAME = 'Boxing'
                elif ('Cricket' in file):
                    reportfile = file
                    SPORTNAME = 'Cricket'
                elif ('Greyhound' in file and 'Racing' in file):
                    reportfile = file
                    SPORTNAME = 'Greyhound Racing'
                elif ('Handball' in file):
                    reportfile = file
                    SPORTNAME = 'Handball'
                elif ('Horse' in file and 'Racing' in file):
                    reportfile = file
                    SPORTNAME = 'Horse Racing'
                elif ('Ice' in file and 'Hockey' in file):
                    reportfile = file
                    SPORTNAME = 'Ice Hockey'
                elif ('Motor' in file and 'Sport' in file):
                    reportfile = file
                    SPORTNAME = 'Motor Sport'
                elif ('Snooker' in file):
                    reportfile = file
                    SPORTNAME = 'Snooker'
                elif ('Soccer' in file):
                    reportfile = file
                    SPORTNAME = 'Soccer'
                elif ('Tennis' in file):
                    reportfile = file
                    SPORTNAME = 'Tennis'
                elif ('Total' in file):
                    reportfile = file
                    SPORTNAME = 'Total'

    path = os.path.dirname(os.path.abspath(__file__))
    riskfile = path + '/' + riskfile
    poolfile = path + '/' + poolfile
    dailysummaryfile = path + '/' + dailysummaryfile
    reportfile = path + '/' + reportfile
    # print(riskfile)
    # print(poolfile)
    # print(dailysummaryfile)
    # print(reportfile)

    # df_T4 = pd.read_csv('./T4_Cricket.csv')
    df_T4 = pd.read_excel(reportfile)
    df_Risk = pd.read_excel(riskfile)
    df_Pool = pd.read_excel(poolfile)
    df_DaySum = pd.read_excel(dailysummaryfile)
except NameError:
    print("File Name not found. Please check if the file name format is correct.")
    sys.exit(0)
except FileNotFoundError:
    print("No such file or directory. Please check if the file name format is correct.")
    sys.exit(0)


# df_T4 = pd.read_csv('./T4_Cricket.csv')
# df_T4 = pd.read_excel('./T4Report.xlsx')
# df_T4 = pd.read_excel(reportfile)

# df_Risk = pd.read_excel(riskfile)
if(SPORTNAME != 'Total'):
    df_Risk = df_Risk[df_Risk['Sport Name'] == SPORTNAME]
df_Risk = pd.DataFrame({
        'subaccname': df_Risk['Sub Username'],
        'current_risk': df_Risk['Sport Risk']})
df_Risk = df_Risk[df_Risk['subaccname'].notnull()]

# df_Pool = pd.read_excel(poolfile)
df_Pool = pd.DataFrame({
        'account_id': df_Pool['Id'],
        'current_pool': df_Pool['Operator']})

# df_DaySum = pd.read_excel(dailysummaryfile)
if(SPORTNAME != 'Total'):
    df_DaySum = df_DaySum[df_DaySum['sport_name'] == SPORTNAME]
df_DaySum = pd.DataFrame({
        'supermastername': df_DaySum['supermastername'],
        'subaccname': df_DaySum['subaccname'],
        'account_id': df_DaySum['account_id'],
        'nb_bets': df_DaySum['bet_id'],
        'turnover': df_DaySum['size_matched'],
        # 'current_pool': df_DaySum['pool'],
        'profit': df_DaySum['mb_pnl']})
df_DaySum = df_DaySum.groupby(['supermastername', 'subaccname', 'account_id']).agg({'nb_bets': np.size,'turnover': np.sum, 'profit': np.sum}).reset_index()
df_DaySum = df_DaySum.assign(nb_bets_current=df_DaySum['nb_bets'])


df_new = pd.concat([df_T4, df_DaySum], sort=False)
df_new = df_new.groupby(['supermastername', 'subaccname', 'account_id']).agg({'nb_bets': np.sum,'turnover': np.sum, 'profit': np.sum, 'nb_bets_current': np.sum}).reset_index()

# Update risk
df_new = pd.merge(df_new, df_T4[['account_id', 'current_risk']], on='account_id', how='left')
df_new = pd.merge(df_new, df_Risk, on='subaccname', how='left', suffixes=['','_new'])
df_new['current_risk'] = np.where((df_new['current_risk'] != df_new['current_risk_new']) & (~df_new['current_risk_new'].isnull()), df_new['current_risk_new'], df_new['current_risk'] )
df_new = df_new.drop('current_risk_new', 1)

# Update pool
df_new = pd.merge(df_new, df_Pool, on='account_id', how='left')

df_new = df_new.fillna({'current_risk':'N/A','current_pool':'UNMATCH'})



# df_out = pd.merge(df_T4, df_Risk, on='subaccname', how='left')
# df_out = pd.merge(df_out, df_Pool, on='account_id', how='left')
#
# df_out = df_out.fillna({'current_risk':'N/A','current_pool':'UNMATCH'})

# Write excel
# writer = pd.ExcelWriter(path + '/T4Report_Cricket.xlsx', engine='xlsxwriter')
writer = pd.ExcelWriter(reportfile, engine='xlsxwriter')
df_new.to_excel(writer, sheet_name='Sheet1', index=False)
writer.save()