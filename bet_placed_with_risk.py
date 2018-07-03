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
query = '''select bp.*, rs.sub_risk / 100 as subacc_risk, rs.total_risk / 100 as total_risk, rs.date from
    (select * from bet_placed 
    where settled_date::date >= date '2018-6-14' 
    and supermastername not in (select * from supermaster_norisk_control)
    and sport_name = 'Soccer'
    ) bp left join(
    select subusername as subaccname, sub_sportrisk as sub_risk, (sub_sportrisk + master_sportrisk + supmaster_sportrisk + admin_sportrisk) as total_risk, date
    from( select sub.subusername,
        coalesce(trim(trailing '%' from sub.sub_sportrisk)::float, 0) as sub_sportrisk,
        coalesce(trim(trailing '%' from master.master_sportrisk)::float, 0) as master_sportrisk,
        coalesce(trim(trailing '%' from supmaster.supmaster_sportrisk)::float, 0) as supmaster_sportrisk,
        coalesce(trim(trailing '%' from admin.admin_sportrisk)::float, 0) as admin_sportrisk,
        sub.date
        from
        (select adminusername, supermasterusername, masterusername, subusername, sportrisk as sub_sportrisk, date::date from risk_setting where riskaccounttype = 'SUB' and sportname = 'Soccer') sub
        left join (select masterusername, sportrisk as master_sportrisk, date::date  from risk_setting where riskaccounttype = 'MASTER' and sportname = 'Soccer') master on sub.masterusername = master.masterusername and sub.date = master.date
        left join (select supermasterusername, sportrisk as supmaster_sportrisk, date::date  from risk_setting where riskaccounttype = 'SUPER_MASTER' and sportname = 'Soccer') supmaster on sub.supermasterusername = supmaster.supermasterusername and sub.date = supmaster.date
        left join (select adminusername, sportrisk as admin_sportrisk ,date::date from risk_setting  where riskaccounttype = 'ADMIN' and sportname = 'Soccer') admin on sub.adminusername = admin.adminusername and sub.date = admin.date) total
    where date >= '2018-6-14'::date) rs on bp.subaccname = rs.subaccname and bp.settled_date::date = rs.date '''

# df_daily = queryViaSSH(database, query)
#
# df_daily.to_pickle('./Soccer_backtest.pkl')

df_daily = pd.read_pickle('./Soccer_backtest.pkl')

df_daily.subacc_risk = df_daily.subacc_risk.fillna('0')
df_daily.total_risk = df_daily.total_risk.fillna('0')
df_daily = df_daily.drop(columns=['date'])
# print(df_daily.info())

df_T2 = pd.read_excel('./SoccerT2&3_Event_V0.3 26.06.2018.xlsx', sheet_name='T2')
df_T2 = df_T2.assign(No_T='T2')
df_T3 = pd.read_excel('./SoccerT2&3_Event_V0.3 26.06.2018.xlsx', sheet_name='T3')
df_T3 = df_T3.assign(No_T='T3')
df_T2 = pd.concat([df_T2, df_T3], sort=False)

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

df_daily = df_daily[df_daily.No_T == 'T4']

df_newsugg = pd.read_excel('./Soccer_backtest V0.1.xlsx')
df_daily = pd.merge(df_daily, df_newsugg, how='left', left_on=['account_id'],
                    right_on=['account_id'])
df_daily.Risk_suggestion = df_daily.Risk_suggestion.fillna('0')

df_daily.subacc_risk = pd.to_numeric(df_daily.subacc_risk, errors='coerce')
df_daily.total_risk = pd.to_numeric(df_daily.total_risk, errors='coerce')
df_daily.Risk_suggestion = pd.to_numeric(df_daily.Risk_suggestion, errors='coerce')
df_daily = df_daily.assign(risk_suggestion_total = df_daily.total_risk - df_daily.subacc_risk + df_daily.Risk_suggestion)
df_daily = df_daily.assign(diff_pnl_suggest=df_daily.mb_pnl * -(df_daily.risk_suggestion_total))
df_daily = df_daily.drop(columns=['date'])
df_daily = df_daily.drop(columns=['subaccount'])
print(df_daily.info())
df_daily.to_pickle('./Soccer_backtest_update.pkl')
df_daily.to_csv('./Soccer_backtest_T4.csv',index=False, date_format='%Y-%m-%d %H:%M:%S')