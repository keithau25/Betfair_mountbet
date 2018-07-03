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
query = '''select bp.*, rs.sub_risk / 100 as subacc_risk, rs.total_risk / 100 as total_risk from
    (select * from bet_placed 
    where settled_date::date = date '{Date}' 
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
    where date = '{Date}'::date) rs on bp.subaccname = rs.subaccname'''.format(Date=date)

df_daily = queryViaSSH(database, query)

df_daily.stake = pd.to_numeric(df_daily.stake, errors='coerce')
df_daily.size_matched = pd.to_numeric(df_daily.size_matched, errors='coerce')
df_daily.bf_pnl = pd.to_numeric(df_daily.bf_pnl, errors='coerce')
df_daily.mb_pnl = pd.to_numeric(df_daily.mb_pnl, errors='coerce')
df_daily.diff_pnl = pd.to_numeric(df_daily.diff_pnl, errors='coerce')
df_daily.settled_date = pd.to_datetime(df_daily['settled_date']).dt.tz_convert('Europe/London')
df_daily.placed_date = pd.to_datetime(df_daily['placed_date']).dt.tz_convert('Europe/London')
df_daily = df_daily.assign(date=pd.to_datetime(df_daily['settled_date']).dt.tz_convert('Europe/London').dt.date)


# df_daily = pd.read_csv('./daily_report_20180618.csv')
# df_rs = pd.read_csv('./risk_setting_summ_20180618.csv')
# df_rs.subacc_risk = df_rs.subacc_risk / 100
# df_rs.total_risk = df_rs.total_risk / 100
# df_daily = df_daily[df_daily.sport_name == 'Soccer']
# df_daily = pd.merge(df_daily, df_rs, left_on = 'subaccname', right_on = 'subaccname', how='left')
# df_daily = df_daily.assign(date=pd.to_datetime(df_daily['settled_date']).dt.tz_localize('UTC').dt.tz_convert('Europe/London').dt.date)

df_daily = df_daily.assign(real_risk=1-(df_daily.bf_pnl / df_daily.mb_pnl))
df_daily.real_risk = pd.to_numeric(df_daily.real_risk, errors='coerce')
df_daily.real_risk = df_daily.real_risk.round(2)
df_daily = df_daily.assign(No_T=np.where(df_daily.real_risk == df_daily.total_risk, 'T4', 'T2'))

# df_daily.ix[(df_daily.total_risk.isnull() & (df_daily.real_risk == 0)), 'No_T'] = 'Other'
df_daily.ix[(df_daily.total_risk.isnull() & (df_daily.real_risk == 0)), 'No_T'] = 'T4'


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

# df_daily.to_csv('./daily_report_20180618_No_T.csv',index=False)

df_daily = df_daily.drop(columns=['date'])
# Write excel
writer = pd.ExcelWriter('./daily_report_20180618_No_T.xlsx', engine='xlsxwriter', options={'remove_timezone': True, 'strings_to_numbers': True})
df_daily.to_excel(writer, sheet_name='daily_report_20180618_No_T', index=False)
df_summ.to_excel(writer, sheet_name='summary-soccer', index=False)
writer.save()

