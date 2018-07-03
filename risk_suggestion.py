import asyncio
import asyncpg
import pandas as pd
import numpy as np
import sys
from sshtunnel import SSHTunnelForwarder

input_text = input("Please enter sport name:")
sport = ''
if(('American' in input_text or 'american' in input_text) and ('Football' in input_text or 'football' in input_text)):
    sport = 'American Football'
elif('Baseball' in input_text or 'baseball' in input_text):
    sport = 'Baseball'
elif ('Basketball' in input_text or 'basketball' in input_text):
    sport = 'Basketball'
elif ('Boxing' in input_text or 'boxing' in input_text):
    sport = 'Boxing'
elif ('Cricket' in input_text or 'cricket' in input_text):
    sport = 'Cricket'
elif (('Greyhound' in input_text or 'greyhound' in input_text) and ('Racing' in input_text or 'racing' in input_text)):
    sport = 'Greyhound Racing'
elif ('Handball' in input_text or 'handball' in input_text):
    sport = 'Handball'
elif (('Horse' in input_text or 'horse' in input_text) and ('Racing' in input_text or 'racing' in input_text)):
    sport = 'Horse Racing'
elif (('Ice' in input_text or 'ice' in input_text) and ('Hockey' in input_text or 'hockey' in input_text)):
    sport = 'Ice Hockey'
elif (('Motor' in input_text or 'motor' in input_text) and ('Sport' in input_text or 'sport' in input_text)):
    sport = 'Motor Sport'
elif ('Snooker' in input_text or 'snooker' in input_text):
    sport = 'Snooker'
elif ('Soccer' in input_text or 'soccer' in input_text):
    sport = 'Soccer'
elif ('Tennis' in input_text or 'tennis' in input_text):
    sport = 'Tennis'
else:
    print("Sport not match!")
    sys.exit(0)

print('Processing for sport: {sport}...'.format(sport=sport))

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

database = 'dailyreport'
query = '''SELECT '{sport}' AS sport_name, summ.*, COALESCE(rs.sportrisk, '') AS current_risk, COALESCE(op.operator, 'Unmatched') AS current_pool, COALESCE(op.suspended, 'Unmatched') as suspended FROM (
    SELECT
        bp.adminname AS admin,
        bp.supermastername AS supermaster,
        bp.subaccname AS subaccount,
        bp.account_id,
        sum(CASE WHEN bp.sport_name = '{sport}' THEN bp.size_matched END) AS turnover_last1year,
        sum(CASE WHEN bp.sport_name = '{sport}' THEN bp.mb_pnl END) AS profit_last1year,
        count(CASE WHEN bp.sport_name = '{sport}' THEN bp.bet_id END) AS nb_placed_bets_last1year,
        sum(CASE WHEN bp.sport_name = '{sport}' AND bp.settled_date >= now() - INTERVAL '28 days' THEN bp.size_matched END) AS turnover_last4weeks,
        sum(CASE WHEN bp.sport_name = '{sport}' AND bp.settled_date >= now() - INTERVAL '28 days' THEN bp.mb_pnl END) AS profit_last4weeks,
        count(CASE WHEN bp.sport_name = '{sport}' AND bp.settled_date >= now() - INTERVAL '28 days' THEN bp.bet_id END) AS nb_bets_last4weeks,
        sum(bp.size_matched) AS als_turnover_last1year,
        sum(bp.mb_pnl) AS als_profit_last1year
    FROM	bet_placed bp
    WHERE	bp.mb_pnl IS NOT NULL
        AND	bp.settled_date >= now() - INTERVAL '1 year'
    GROUP BY	bp.adminname, bp.supermastername, bp.subaccname, bp.account_id
    HAVING	count(CASE WHEN bp.sport_name = '{sport}' THEN bp.bet_id END) != 0
    ) summ LEFT JOIN (SELECT * FROM risk_setting WHERE sportname = '{sport}' AND date = (SELECT MAX(date) FROM risk_setting)) rs ON summ.subaccount = rs.subusername
    LEFT JOIN (SELECT * FROM operator_pool WHERE date = (SELECT MAX(date) FROM operator_pool)) op ON summ.account_id = op.id '''.format(sport=sport)

df = queryViaSSH(database, query)

# df = pd.read_csv('./Cricket_last1year.csv')
df.turnover_last1year = pd.to_numeric(df.turnover_last1year, errors='coerce')
df.profit_last1year = pd.to_numeric(df.profit_last1year, errors='coerce')
df.turnover_last4weeks = pd.to_numeric(df.turnover_last4weeks, errors='coerce')
df.profit_last4weeks = pd.to_numeric(df.profit_last4weeks, errors='coerce')
df.als_turnover_last1year = pd.to_numeric(df.als_turnover_last1year, errors='coerce')
df.als_profit_last1year = pd.to_numeric(df.als_profit_last1year, errors='coerce')

# print(df.info())

df = df[df.current_pool != 'CyanPool1']
df = df[df.current_pool != 'FancyPool']
df = df[df.current_pool != 'DAtradertest']
df = df[df.current_pool != 'CitronPool1']
df = df[df.current_pool != 'GrayPool1']
df = df[df.current_pool != 'RedPool1']

df.current_risk = df.current_risk.astype(str).apply(lambda x: x.strip('%'))
df.current_risk = pd.to_numeric(df.current_risk, errors='coerce')
df.current_risk = df.current_risk.fillna('0')
df.current_risk = pd.to_numeric(df.current_risk, errors='coerce')
df.current_risk = df.current_risk / 100

df = df.assign(ROI=np.where(df.turnover_last1year == 0, 0, (df.profit_last1year / df.turnover_last1year)))
df = df.assign(new_suggest_risk=0.0)
df.loc[(df.nb_bets_last4weeks <= 0), 'new_suggest_risk'] = 0
df.loc[(df.nb_bets_last4weeks > 0)&(df.nb_placed_bets_last1year >= 499)&(df.ROI < 0), 'new_suggest_risk'] = 0.2

df.loc[(df.nb_bets_last4weeks > 0)&(df.nb_placed_bets_last1year >= 100)&(df.nb_placed_bets_last1year < 499)&(df.ROI <= -0.02), 'new_suggest_risk'] = 0.2

df.loc[(df.nb_bets_last4weeks > 0)&(df.nb_placed_bets_last1year >= 100)&(df.nb_placed_bets_last1year < 499)&
       (df.ROI > -0.02)&(df.ROI <= -0.01)&
       (df.profit_last1year == df.als_profit_last1year), 'new_suggest_risk'] = 0.2


# df.to_csv('./new_risk_suggest.csv',index=False)


df = df[df.nb_bets_last4weeks > 0]
df = df[df.current_risk != df.new_suggest_risk]
df_riskchange = pd.DataFrame({
        'subaccount': df['subaccount'],
        'account_id': df['account_id'],
        'new_suggest_risk': df['new_suggest_risk'],
        'sport_name': df['sport_name'],
        'suspended': df['suspended']})

df_riskchange.new_suggest_risk = (df_riskchange.new_suggest_risk * 100).astype(int).astype(str) + '%'

df_riskchange.to_csv('./list_of_subacc_riskchange.csv',index=False)

print("Done.")