import asyncio
import asyncpg
import pandas as pd
import numpy as np
from sshtunnel import SSHTunnelForwarder

async def connectSQL(database, query, port):
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database=database, host='localhost', port=port)
    stmt = await conn.prepare(query)
    columns = [a.name for a in stmt.get_attributes()]
    data = await stmt.fetch()
    await conn.close()
    return pd.DataFrame(data, columns=columns)

def queryViaSSH(database, query):
    tunnel = SSHTunnelForwarder(
        ('35.177.108.175', 22), ssh_username="ubuntu",
        ssh_pkey="./multimatrix_key.pem", remote_bind_address=('localhost', 5432))
    try:
        tunnel.start()
        port = tunnel.local_bind_port
        loop = asyncio.get_event_loop()
        dataframe = loop.run_until_complete(connectSQL(database, query, port))
        # tunnel.stop()
        # return dataframe
    except Exception as e:
        print(e)
    finally:
        tunnel.stop()
    return dataframe

date = '2018-6-18'
database = 'dailyreport'
query = '''select * from bet_placed 
    where settled_date::date >= date '2018-6-14'
    and sport_name = 'Soccer' '''

df_daily = queryViaSSH(database, query)
# df_daily.to_pickle('./df_daily.pkl')
# df_daily = pd.read_pickle('./Soccer_all.pkl')
# df_daily = df_daily.drop(columns=['No_T'])

df_T2 = pd.read_excel('./SoccerT2&3_Event_V0.3 26.06.2018.xlsx', sheet_name='T2')
df_T2 = df_T2.assign(No_T='T2')

df_T3 = pd.read_excel('./SoccerT2&3_Event_V0.3 26.06.2018.xlsx', sheet_name='T3')
df_T3 = df_T3.assign(No_T='T3')

df_T2 = pd.concat([df_T2, df_T3], sort=False)

# print(df_T2.info())
# print(df_daily.info())

df_daily.stake = pd.to_numeric(df_daily.stake, errors='coerce')
df_daily.size_matched = pd.to_numeric(df_daily.size_matched, errors='coerce')
df_daily.bf_pnl = pd.to_numeric(df_daily.bf_pnl, errors='coerce')
df_daily.mb_pnl = pd.to_numeric(df_daily.mb_pnl, errors='coerce')
df_daily.diff_pnl = pd.to_numeric(df_daily.diff_pnl, errors='coerce')
df_daily.settled_date = pd.to_datetime(df_daily['settled_date']).dt.tz_convert('Europe/London')
df_daily.placed_date = pd.to_datetime(df_daily['placed_date']).dt.tz_convert('Europe/London')
df_daily = df_daily.assign(date=pd.to_datetime(pd.to_datetime(df_daily['settled_date']).dt.date))


df_daily = pd.merge(df_daily, df_T2, how='left', left_on=['sport_name','date','event_name','competition_name','market_name','pool'],
                    right_on=['sport_name','date','event_name','competition_name','market_name','pool'])

df_daily.No_T = df_daily.No_T.fillna('T4')
df_daily.ix[(df_daily.subaccname == 'T2Kunal'), 'No_T'] = 'H'
# print(df_daily.head(10))
# df_daily.to_csv('./test.csv',index=False, date_format='%Y-%m-%d %H:%M:%S')

df_summ = df_daily.groupby(['sport_name', 'pool','No_T', 'date']).agg({'bf_pnl': np.sum,'mb_pnl': np.sum, 'diff_pnl': np.sum}).reset_index()
df_summ.bf_pnl = pd.to_numeric(df_summ.bf_pnl, errors='coerce')
df_summ.mb_pnl = pd.to_numeric(df_summ.mb_pnl, errors='coerce')
df_summ.diff_pnl = pd.to_numeric(df_summ.diff_pnl, errors='coerce')
df_summ.bf_pnl = df_summ.bf_pnl.round(2)
df_summ.mb_pnl = df_summ.mb_pnl.round(2)
df_summ.diff_pnl = df_summ.diff_pnl.round(2)
df_summ = df_summ.sort_values(by=['date', 'No_T', 'pool'])
df_summ = df_summ[['sport_name', 'pool', 'bf_pnl', 'mb_pnl', 'diff_pnl', 'No_T', 'date']]
# df_summ = df_summ[df_summ.No_T != 'Other']
#
# # df_daily.to_csv('./daily_report_20180618_No_T.csv',index=False)
#
# df_daily = df_daily.drop(columns=['date'])
#
# print(df_daily.info())
#
# df_daily.to_csv('./Soccer_20180614_21_No_T.csv',index=False, date_format='%Y-%m-%d %H:%M:%S')

df_daily = df_daily.drop(columns=['date'])
# Write excel
writer = pd.ExcelWriter('./Soccer_No_T.xlsx', engine='xlsxwriter', options={'remove_timezone': True, 'strings_to_numbers': True})
df_daily.to_excel(writer, sheet_name='Soccer_No_T', index=False)
df_summ.to_excel(writer, sheet_name='Summary-soccer', index=False)
writer.save()

df_daily.to_pickle('./Soccer_all.pkl')


